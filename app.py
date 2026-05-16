"""HUBSTREAM PRO — Flask movie / series / anime download website.

Public catalogue front-end that talks to the same hubstream API the
Telegram bot uses. Serves a clean, mobile-friendly UI for browsing,
searching and downloading content.
"""
from __future__ import annotations

import functools
import json
import mimetypes
import os
import random
import time
import sqlite3
from typing import Any, Dict, List, Optional
from pymongo import MongoClient

import requests
from flask import (
    Flask, Response, abort, jsonify, redirect,
    render_template, request, session,
    stream_with_context, url_for, make_response
)
from bs4 import BeautifulSoup




from itsdangerous import BadSignature, URLSafeSerializer
import re
import urllib.parse

import asyncio

# ─────────────────────────── TG STREAMER ───────────────────────────
TG_API_ID = int(os.getenv("TELEGRAM_API_ID") or os.getenv("TG_API_ID") or "39093330")
TG_API_HASH = os.getenv("TELEGRAM_API_HASH") or os.getenv("TG_API_HASH") or "3ea2d9975816ef12baf40575973de92a"
TG_SESSION_STRING = (
    os.getenv("TELEGRAM_SESSION_STRING")
    or os.getenv("SESSION_STRING")
    or ""
).strip()
# Fallback to hardcoded token if env is missing (for local testing)
BOT_TOKEN = (
    os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or "8775047846:AAFWxdXgWJZzqQyZuJBsJh7KYRL_YChyQ-E"
).strip()
TG_AUTH_MODE = "user_session" if TG_SESSION_STRING else "bot_token"

tg_app = None

def _ensure_event_loop() -> asyncio.AbstractEventLoop:
    """Return a usable loop for sync Flask/Gunicorn threads."""
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def run_async(coro):
    """Helper to run async code in sync Flask routes."""
    loop = _ensure_event_loop()
    return loop.run_until_complete(coro)

# Global state for lazy start
_tg_started = False

def get_tg_app():
    """Starts the Pyrogram client lazily if not already started."""
    global tg_app, _tg_started

    # Ensure an event loop exists in this thread for Pyrogram
    _ensure_event_loop()

    if tg_app is None:
        from pyrogram import Client

        client_kwargs = {
            "api_id": TG_API_ID,
            "api_hash": TG_API_HASH,
            "in_memory": True,
        }
        if TG_SESSION_STRING:
            client_kwargs["session_string"] = TG_SESSION_STRING
            tg_app = Client("hubstream_streamer_user", **client_kwargs)
        else:
            client_kwargs["bot_token"] = BOT_TOKEN
            tg_app = Client("hubstream_streamer_bot", **client_kwargs)

    if not _tg_started:
        try:
            run_async(tg_app.start())
            _tg_started = True
            print(f"Pyrogram Client started lazily using {TG_AUTH_MODE}.")
        except Exception as e:
            print(f"Lazy Pyrogram Start Failed: {e}")
            raise e
    return tg_app


def _env_base(name: str, default: str) -> str:
    return os.getenv(name, default).strip().rstrip("/")


API_BASE = _env_base("API_BASE", "https://hubstream.sujanbotz.workers.dev/api")
DL_BASE = _env_base(
    "DL_BASE",
    f"{API_BASE[:-4]}/dl" if API_BASE.endswith("/api") else "https://hubstream.sujanbotz.workers.dev/dl",
)
TMDB_IMG = "https://image.tmdb.org/t/p/w500"
HTTP_TIMEOUT = 12
SITE_NAME = "OFC MOVIES"
TAGLINE = "Premium movies, series & anime — direct downloads"
TG_CHANNEL = "ofcmovie"
TG_CHANNEL_URL = f"https://t.me/{TG_CHANNEL}"

app = Flask(__name__, static_folder="static", template_folder="templates")







def _proxy_http_stream(url: str, filename: str, as_attachment: bool = False) -> Response:
    """Proxy an external HTTP stream to the client."""
    try:
        r = requests.get(url, stream=True, timeout=15)
        def generate():
            for chunk in r.iter_content(chunk_size=128 * 1024): # 128KB chunks
                if chunk:
                    yield chunk
        
        headers = {
            "Content-Type": r.headers.get("Content-Type", "video/mp4"),
            "Content-Length": r.headers.get("Content-Length"),
            "Accept-Ranges": "bytes",
        }
        if as_attachment:
            headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        else:
            headers["Content-Disposition"] = f'inline; filename="{filename}"'
            
        return Response(stream_with_context(generate()), headers=headers, status=r.status_code)
    except Exception as e:
        app.logger.error(f"Proxy stream failed: {e}")
        abort(500)





@app.template_filter('from_json')
def from_json_filter(s):
    if not s: return []
    if isinstance(s, (list, dict)): return s
    try:
        return json.loads(s)
    except:
        return []

# ─────────────── signed download token (hides upstream URL) ───────────────
_SECRET = os.getenv("SESSION_SECRET") or "ofcmovies@secret#key!2024$dl"
_dl_signer = URLSafeSerializer(_SECRET, salt="hubstream-dl-v1")
_custom_dl_signer = URLSafeSerializer(_SECRET, salt="hubstream-custom-dl-v1")


def make_custom_dl_token(url: str, title: str, quality: str) -> str:
    return _custom_dl_signer.dumps({"u": url, "t": title, "q": quality})


def decode_custom_dl_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        d = _custom_dl_signer.loads(token)
        return d if isinstance(d, dict) and d.get("u") else None
    except BadSignature:
        return None


def _parse_telegram_message_url(url: str) -> Optional[tuple[Any, int]]:
    if "t.me/" not in url:
        return None

    parts = [part for part in url.split("t.me/")[-1].split("/") if part]
    if len(parts) < 2:
        return None

    if parts[0] == "c" and len(parts) >= 3 and parts[1].isdigit() and parts[2].isdigit():
        return int(f"-100{parts[1]}"), int(parts[2])

    if parts[1].isdigit():
        return parts[0], int(parts[1])

    return None


def _telegram_stream_route(url: str) -> Optional[str]:
    parsed = _parse_telegram_message_url(url)
    if not parsed:
        return None

    chat_ref, message_id = parsed
    if isinstance(chat_ref, int):
        internal_id = str(chat_ref).removeprefix("-100")
        return url_for("telegram_private_stream", chat_id=internal_id, message_id=message_id)
    return url_for("telegram_stream", channel=chat_ref, message_id=message_id)


def _custom_download_target(token: str, url: str) -> str:
    # Keep Telegram-backed files on our proxy route so users download from the website,
    # not from a raw t.me link, and so we can handle Telegram auth server-side.
    if _parse_telegram_message_url(url):
        return url_for("custom_download_stream", token=token)
    return url


def _guess_media_type(file_name: str) -> str:
    ext = os.path.splitext((file_name or "").lower())[1]
    explicit = {
        ".mkv": "video/mp4", # Force video/mp4 for better browser compatibility
        ".mp4": "video/mp4",
        ".m4v": "video/mp4",
        ".mov": "video/quicktime",
        ".webm": "video/webm",
        ".avi": "video/x-msvideo",
        ".m3u8": "application/vnd.apple.mpegurl",
        ".ts": "video/mp2t",
    }
    if ext in explicit:
        return explicit[ext]
    guessed, _ = mimetypes.guess_type(file_name)
    return guessed or "application/octet-stream"


def _stream_telegram_message(chat_ref: Any, message_id: int, as_attachment: bool = True) -> Response:
    client = get_tg_app()
    if isinstance(chat_ref, str) and not chat_ref.startswith("@"):
        chat_ref = f"@{chat_ref}"

    try:
        chat = run_async(client.get_chat(chat_ref))
        resolved_chat_ref: Any = getattr(chat, "id", chat_ref)
    except Exception:
        resolved_chat_ref = chat_ref

    message = run_async(client.get_messages(resolved_chat_ref, message_id))

    if not message or not (message.document or message.video or message.audio):
        abort(404, description="No file found in this Telegram post.")

    media = message.document or message.video or message.audio
    file_name = media.file_name or "download"
    file_size = media.file_size
    mime = media.mime_type or _guess_media_type(file_name)

    # RANGE SUPPORT
    range_header = request.headers.get("Range")
    start_byte = 0
    end_byte = file_size - 1 if file_size else 0
    status_code = 200

    if range_header and file_size:
        try:
            # Parse Range: bytes=0-100
            byte_range = range_header.replace("bytes=", "").split("-")
            if byte_range[0]:
                start_byte = int(byte_range[0])
            if len(byte_range) > 1 and byte_range[1]:
                end_byte = int(byte_range[1])
            status_code = 206
        except Exception:
            pass

    def stream_generator(start, end):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Attempt efficient block-seeking (MTProto blocks are usually 1MB)
            BLOCK_SIZE = 1024 * 1024
            start_block = start // BLOCK_SIZE
            skip = start % BLOCK_SIZE
            
            try:
                # Try passing offset to stream_media (only works in some Pyrogram versions)
                iterator = client.stream_media(message, offset=start_block).__aiter__()
            except (TypeError, Exception):
                # Fallback: Stream from beginning and skip manually
                iterator = client.stream_media(message).__aiter__()
                skip = start

            bytes_sent = 0
            total_to_send = (end - start) + 1
            
            while bytes_sent < total_to_send:
                try:
                    chunk = loop.run_until_complete(iterator.__anext__())
                    if not chunk:
                        break
                    
                    # Handle skipping (works for both block-offset and start-from-zero)
                    if skip > 0:
                        if len(chunk) <= skip:
                            skip -= len(chunk)
                            continue
                        else:
                            chunk = chunk[skip:]
                            skip = 0
                    
                    send_len = min(len(chunk), total_to_send - bytes_sent)
                    if send_len <= 0:
                        break
                        
                    yield chunk[:send_len]
                    bytes_sent += send_len
                except StopAsyncIteration:
                    break
                except Exception:
                    break
        finally:
            loop.close()

    resp = Response(
        stream_with_context(stream_generator(start_byte, end_byte)),
        status=status_code,
        mimetype=mime
    )
    disposition = "attachment" if as_attachment else "inline"
    resp.headers["Content-Disposition"] = f'{disposition}; filename="{file_name}"'
    resp.headers["Accept-Ranges"] = "bytes"
    if file_size:
        resp.headers["Content-Length"] = str((end_byte - start_byte) + 1)
        resp.headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"
    resp.headers["Cache-Control"] = "private, no-store"
    return resp


def _proxy_http_stream(url: str, download_name: str, as_attachment: bool = True) -> Response:
    fwd_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    rng = request.headers.get("Range")
    if rng:
        fwd_headers["Range"] = rng

    try:
        upstream = requests.get(
            url,
            headers=fwd_headers,
            stream=True,
            timeout=20,
            allow_redirects=True,
        )
    except Exception as e:
        app.logger.warning("Upstream fetch failed for %s: %s", url, e)
        return render_template(
            "error.html",
            code=503,
            message="Stream server is unreachable. Please try again later.",
        ), 503

    if upstream.status_code >= 400:
        upstream.close()
        return render_template(
            "error.html",
            code=503,
            message="This file is temporarily unavailable on the stream server.",
        ), 503

    def generate():
        try:
            for chunk in upstream.iter_content(chunk_size=64 * 1024):
                if chunk:
                    yield chunk
        finally:
            upstream.close()

    resp = Response(stream_with_context(generate()), status=upstream.status_code)
    for h in ("content-type", "content-length", "accept-ranges", "content-range"):
        v = upstream.headers.get(h)
        if v:
            resp.headers[h.title()] = v
    content_type = resp.headers.get("Content-Type", "")
    if not content_type or content_type.startswith("application/octet-stream"):
        resp.headers["Content-Type"] = _guess_media_type(download_name)
    disposition = "attachment" if as_attachment else "inline"
    resp.headers["Content-Disposition"] = f'{disposition}; filename="{download_name}"'
    resp.headers["Cache-Control"] = "private, no-store"
    return resp

# ─────────────────────── ADMIN CONFIG ──────────────────────────
ADMIN_PASSWORD  = os.getenv("ADMIN_PASSWORD", "BISWA@9090")
ADMIN_TG_ID     = os.getenv("ADMIN_TG_ID", "")           # user id of the admin telegram
OMDB_API_KEY    = os.getenv("OMDB_API_KEY", "")          # get free key at omdbapi.com
BOT_STATS_PATH  = os.getenv(
    "BOT_STATS_DB",
    os.path.join(os.path.dirname(__file__), "..", "bot", "stats.db"),
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://xamicc222_db_user:LkOliSVjkBDGyYFT@cluster0.trwwl0v.mongodb.net/?appName=Cluster0")
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo_client["hubstream"]
custom_movies_col = db["custom_movies"]
web_stats_col = db["web_stats"]
settings_col = db["settings"]


def _get_setting(key: str, default: str = "") -> str:
    try:
        doc = settings_col.find_one({"_id": key})
        return doc["value"] if doc else default
    except Exception:
        return default


def _set_setting(key: str, value: str) -> None:
    try:
        settings_col.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)
    except Exception:
        pass


def _normalize_positive_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        if value.is_integer() and value > 0:
            return int(value)
        return None
    text = str(value).strip()
    if not text.isdigit():
        return None
    num = int(text)
    return num if num > 0 else None


def _configured_admin_password() -> str:
    return _get_setting("admin_password") or ADMIN_PASSWORD


def _configured_admin_tg_id() -> str:
    return _get_setting("admin_tg_id") or ADMIN_TG_ID


def _configured_omdb_api_key() -> str:
    return _get_setting("omdb_api_key") or OMDB_API_KEY


def _track_web(event_type: str, path: str = "") -> None:
    try:
        web_stats_col.insert_one({
            "event_type": event_type,
            "path": path[:200],
            "ts": int(time.time())
        })
    except Exception:
        pass


def _bot_stats() -> Dict[str, Any]:
    if not os.path.exists(BOT_STATS_PATH):
        return {}
    try:
        conn = sqlite3.connect(f"file:{BOT_STATS_PATH}?mode=ro", uri=True)
        now = int(time.time())
        def _s(q, *a):
            r = conn.execute(q, a).fetchone()
            return int(r[0] or 0) if r else 0
        result = {
            "total_users":     _s("SELECT COUNT(*) FROM users"),
            "dau":             _s("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts>=?", now-86400),
            "wau":             _s("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts>=?", now-604800),
            "mau":             _s("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts>=?", now-2592000),
            "searches_today":  _s("SELECT COUNT(*) FROM events WHERE kind='search' AND ts>=?", now-86400),
            "searches_month":  _s("SELECT COUNT(*) FROM events WHERE kind='search' AND ts>=?", now-2592000),
            "downloads_today": _s("SELECT COUNT(*) FROM events WHERE kind='download' AND ts>=?", now-86400),
            "downloads_month": _s("SELECT COUNT(*) FROM events WHERE kind='download' AND ts>=?", now-2592000),
        }
        conn.close()
        return result
    except Exception as e:
        app.logger.warning("bot stats read failed: %s", e)
        return {}


def _web_stats() -> Dict[str, Any]:
    now = int(time.time())
    try:
        def _s(event_type, since):
            return web_stats_col.count_documents({"event_type": event_type, "ts": {"$gte": since}})
        
        return {
            "views_today":     _s("view", now-86400),
            "views_week":      _s("view", now-604800),
            "views_month":     _s("view", now-2592000),
            "searches_today":  _s("search", now-86400),
            "searches_week":   _s("search", now-604800),
            "dl_today":        _s("dl_click", now-86400),
            "dl_month":        _s("dl_click", now-2592000),
        }
    except Exception:
        return {}


def _admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin_ok"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated


def make_dl_token(file_id: str, file_name: str, **extra: Any) -> str:
    """Create a tamper-proof token that encodes the upstream file pointer."""
    payload: Dict[str, Any] = {"i": str(file_id), "n": str(file_name)}
    for k in ("q", "s", "t"):  # quality, size, title (display only)
        if k in extra and extra[k]:
            payload[k] = str(extra[k])
    return _dl_signer.dumps(payload)


def decode_dl_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        data = _dl_signer.loads(token)
    except BadSignature:
        return None
    if not isinstance(data, dict) or not data.get("i") or not data.get("n"):
        return None
    return data


# ─────────────────────────── FTP CONFIG ────────────────────────────────
FTP_BASE = os.getenv("FTP_BASE", "http://ftp.ctgfun.com").rstrip("/")
FTP_SECTIONS = [
    ("English", "movie"),
    ("TV_Series", "series"),
    ("Indian", "movie"),
    ("Others", "movie"),
]
# Separate in-process cache for FTP listings (longer TTL — 10 min)
_ftp_cache: Dict[str, tuple[float, Any]] = {}


def _ftp_cached_get(url: str, ttl: int = 600) -> Optional[str]:
    """Fetch a URL and return raw HTML text, with caching."""
    now = time.time()
    hit = _ftp_cache.get(url)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        text = r.text
    except Exception as e:
        app.logger.warning("FTP fetch failed for %s: %s", url, e)
        text = None
    _ftp_cache[url] = (now, text)
    if len(_ftp_cache) > 128:
        oldest = sorted(_ftp_cache.items(), key=lambda kv: kv[1][0])[:len(_ftp_cache) - 64]
        for k, _ in oldest:
            _ftp_cache.pop(k, None)
    return text


def _parse_ftp_dirname(dirname: str) -> Dict[str, Any]:
    """Extract clean title, year and quality from an FTP directory name.

    Example: 'Sinners.2025.1080p.WEBRip.x264 [DDN]' → title='Sinners', year='2025', quality='1080p'
    """
    # Remove trailing brackets like [DDN], {DDN], etc.
    name = re.sub(r"[\[\{][^\]\}]*[\]\}]", "", dirname).strip()
    # Detect resolution
    quality = ""
    q_match = re.search(r"\b(4K|2160p|1080p|720p|480p|360p|HDTS|HDCAM|HDRip|WEBRip|BluRay|BRRip|DVDRip|WEB-DL)\b", name, re.IGNORECASE)
    if q_match:
        quality = q_match.group(1).upper()
    # Extract year
    year = ""
    y_match = re.search(r"\b(19\d{2}|20\d{2})\b", name)
    if y_match:
        year = y_match.group(1)
    # Build clean title: take everything before the year (or resolution if no year)
    cut_pos = y_match.start() if y_match else (q_match.start() if q_match else len(name))
    raw_title = name[:cut_pos]
    # Replace dots/underscores used as word separators, but keep real dots
    title = re.sub(r"[._-]+", " ", raw_title).strip()
    # Remove trailing/leading noise
    title = re.sub(r"\s+", " ", title).strip(" -.")
    return {"title": title, "year": year, "quality": quality}


def _scrape_ftp_section(section: str) -> List[Dict[str, Any]]:
    """Scrape one FTP directory section and return list of entry dicts."""
    url = f"{FTP_BASE}/{section}/"
    html = _ftp_cached_get(url, ttl=600)
    if not html:
        return []
    from bs4 import BeautifulSoup as _BS
    soup = _BS(html, "html.parser")
    entries = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Only keep sub-directories (folder links ending with /)
        if not href.endswith("/") or href in ("/", "../", f"/{section}/"):
            continue
        # Decode URL-encoded characters
        decoded = urllib.parse.unquote(href.rstrip("/").split("/")[-1])
        if not decoded:
            continue
        parsed = _parse_ftp_dirname(decoded)
        if not parsed["title"]:
            continue
        # Full URL to the folder page (for listing files inside)
        folder_url = href if href.startswith("http") else f"{FTP_BASE}/{section}/{urllib.parse.quote(decoded)}/"
        entries.append({
            "raw": decoded,
            "folder_url": folder_url,
            "section": section,
            **parsed,
        })
    return entries


def _get_all_ftp_entries() -> List[Dict[str, Any]]:
    """Return combined FTP entries from all sections (cached)."""
    cache_key = "__ftp_all__"
    now = time.time()
    hit = _ftp_cache.get(cache_key)
    if hit and now - hit[0] < 600:
        return hit[1]
    combined: List[Dict[str, Any]] = []
    for section, kind in FTP_SECTIONS:
        for entry in _scrape_ftp_section(section):
            entry["kind"] = kind if section != "TV_Series" else "series"
            combined.append(entry)
    _ftp_cache[cache_key] = (now, combined)
    return combined


def _search_ftp_entries(q: str, limit: int = 8) -> List[Dict[str, Any]]:
    """Search FTP entries by title keyword — fast in-memory after first load."""
    if not q or len(q) < 2:
        return []
    q_lower = q.lower()
    results = []
    for entry in _get_all_ftp_entries():
        if q_lower in entry["title"].lower():
            results.append(entry)
            if len(results) >= limit:
                break
    return results


def _ftp_entry_to_card(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Convert an FTP directory entry into a card-compatible dict for search results."""
    encoded_folder = urllib.parse.quote(entry["raw"], safe="")
    section = entry.get("section", "English")
    return {
        "title": entry["title"],
        "year": entry.get("year") or "",
        "rating": None,
        "kind": entry.get("kind") or "movie",
        "poster": "",
        "href": url_for("ftp_detail", section=section, folder=entry["raw"]),
        "source": "ftp",
    }


def _list_ftp_folder_files(folder_url: str) -> List[Dict[str, Any]]:
    """List video files inside an FTP folder page."""
    html = _ftp_cached_get(folder_url, ttl=300)
    if not html:
        return []
    from bs4 import BeautifulSoup as _BS
    soup = _BS(html, "html.parser")
    VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".ts"}
    files = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href in ("../", "/") or href.endswith("/"):
            continue
        ext = os.path.splitext(href.split("?")[0].lower())[1]
        if ext not in VIDEO_EXTS:
            continue
        decoded_name = urllib.parse.unquote(href.split("/")[-1])
        full_url = href if href.startswith("http") else folder_url.rstrip("/") + "/" + urllib.parse.quote(decoded_name)
        files.append({
            "name": decoded_name,
            "url": full_url,
            "ext": ext,
        })
    return files


# Signed token for FTP stream/download (hides real URL)
_ftp_signer = URLSafeSerializer(_SECRET, salt="ftp-dl-v1")


def _make_ftp_token(url: str, name: str) -> str:
    return _ftp_signer.dumps({"u": url, "n": name})


def _decode_ftp_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        d = _ftp_signer.loads(token)
        return d if isinstance(d, dict) and d.get("u") else None
    except BadSignature:
        return None


# ────────────────────────── tiny in-process cache ──────────────────────
_cache: Dict[str, tuple[float, Any]] = {}


def _cached_get(url: str, ttl: int = 120) -> Any:
    now = time.time()
    hit = _cache.get(url)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT, headers={
            "User-Agent": f"{SITE_NAME}/1.0 (+web)",
            "Accept": "application/json",
        })
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        app.logger.warning("API fetch failed for %s: %s", url, e)
        data = None
    _cache[url] = (now, data)
    # bound cache size
    if len(_cache) > 512:
        oldest = sorted(_cache.items(), key=lambda kv: kv[1][0])[: len(_cache) - 256]
        for k, _ in oldest:
            _cache.pop(k, None)
    return data


# ────────────────────────── data helpers ───────────────────────────
def _poster(item: Dict[str, Any]) -> Optional[str]:
    p = item.get("poster") or item.get("poster_path") or item.get("backdrop_path")
    if not p:
        return None
    if p.startswith("http"):
        return p
    if p.startswith("/"):
        return f"{TMDB_IMG}{p}"
    return f"{TMDB_IMG}/{p}"


def _media_type(item: Dict[str, Any]) -> str:
    mt = (item.get("media_type") or "").lower()
    if mt == "movie":
        return "movie"
    if mt == "tv":
        return "anime" if item.get("is_anime") else "series"
    return "movie"


def _normalize_catalog_item(item: Dict[str, Any], default_kind: str) -> Dict[str, Any]:
    """Normalize an item from /movies, /tvshows or /anime into the card shape."""
    mt = (item.get("media_type") or "").lower()
    if mt == "movie":
        kind = "movie"
    elif mt == "tv":
        kind = "anime" if item.get("is_anime") else "series"
    else:
        kind = default_kind
    return {
        "tmdb_id": item.get("tmdb_id"),
        "title": item.get("title") or item.get("name"),
        "year": item.get("release_year"),
        "rating": item.get("rating"),
        "poster": _poster(item),
        "kind": kind,
        "quality": item.get("quality") or "",
        "updated_on": item.get("updated_on") or "",
    }



def _fetch_latest(endpoint: str, list_keys: List[str], default_kind: str, limit: int = 12) -> List[Dict[str, Any]]:
    """Fetch a catalog endpoint and return the most recently updated items."""
    data = _cached_get(f"{API_BASE}/{endpoint}", ttl=0) or {}
    raw: List[Dict[str, Any]] = []
    for k in list_keys:
        v = data.get(k)
        if isinstance(v, list):
            raw = v
            break
    items = [_normalize_catalog_item(it, default_kind) for it in raw if it.get("tmdb_id")]
    # Trust the API's natural order (usually newest first)
    return items[:limit]


def _normalize_search(data: Dict[str, Any] | None) -> Dict[str, Any]:
    if not data:
        return {"results": [], "page": 1, "total_pages": 1}
    out: List[Dict[str, Any]] = []
    for item in data.get("results", []) or []:
        title = (item.get("title") or item.get("name") or "").lower()
        if "ftp" in title:
            continue
        out.append({
            "tmdb_id": item.get("tmdb_id"),
            "title": item.get("title") or item.get("name"),
            "year": item.get("release_year"),
            "rating": item.get("rating"),
            "poster": _poster(item),
            "kind": _media_type(item),
        })
    return {
        "results": out,
        "page": int(data.get("page") or 1),
        "total_pages": int(data.get("total_pages") or data.get("totalPages") or 1),
    }


def _build_custom_movie_item(movie: Dict[str, Any]) -> Dict[str, Any]:
    raw_dls = movie.get("downloads")
    if isinstance(raw_dls, str):
        try:
            raw_dls = json.loads(raw_dls)
        except Exception:
            raw_dls = []
    downloads = []
    for d in (raw_dls or []):
        url = (d.get("url") or "").strip()
        if not url:
            continue
        quality = (d.get("quality") or "").strip() or "HD"
        downloads.append({
            "quality": quality,
            "size": (d.get("size") or "").strip(),
            "url": f"/cdl/{make_custom_dl_token(url, movie.get('title') or 'Download', quality)}",
            "file_name": "",
        })
    genres = movie.get("genres") or []
    if isinstance(genres, str):
        try:
            genres = json.loads(genres)
        except Exception:
            genres = []
            
    custom_id = movie.get("custom_id")
    return {
        "id": custom_id,
        "custom_id": custom_id,
        "tmdb_id": movie.get("tmdb_id"),
        "title": movie.get("title") or "Untitled",
        "year": movie.get("year") or "",
        "rating": movie.get("rating"),
        "overview": movie.get("overview") or "",
        "genres": genres,
        "runtime": None,
        "poster": movie.get("poster_url") or "",
        "backdrop": movie.get("backdrop_url") or "",
        "downloads": downloads,
        "kind": movie.get("kind") or "movie", # Use kind from DB if available
        "is_custom": True,
        "is_featured": bool(movie.get("is_featured")),
        "href": url_for("custom_movie", mid=custom_id),
        "added_at": movie.get("added_at") or 0,
    }


def _search_custom_movies(q: str) -> List[Dict[str, Any]]:
    try:
        query = {"title": {"$regex": q, "$options": "i"}}
        if q.isdigit():
            query = {"$or": [query, {"custom_id": int(q)}]}
            
        cursor = custom_movies_col.find(query).sort([("is_featured", -1), ("added_at", -1)])
        return [_build_custom_movie_item(doc) for doc in cursor]
    except Exception:
        return []


def _live_search_results(q: str, limit: int = 8) -> List[Dict[str, Any]]:
    q = q.strip()
    if not q:
        return []

    custom_results = _search_custom_movies(q)
    api_data = _normalize_search(_cached_get(
        f"{API_BASE}/search/?query={requests.utils.quote(q)}&page=1", ttl=60
    ))

    results: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def add_item(item: Dict[str, Any]) -> None:
        href = item.get("href") or (
            url_for("title", tmdb_id=item["tmdb_id"]) if item.get("tmdb_id") else ""
        )
        if not href:
            return

        key = href
        if key in seen:
            return
        seen.add(key)
        results.append({
            "title": item.get("title") or "Untitled",
            "year": item.get("year") or "",
            "rating": item.get("rating"),
            "kind": item.get("kind") or "movie",
            "poster": item.get("poster") or "",
            "href": href,
            "source": item.get("source") or "api",
        })

    for item in custom_results:
        add_item(item)
        if len(results) >= limit:
            return results

    for item in api_data.get("results", []):
        add_item(item)
        if len(results) >= limit:
            break

    return results


def _build_downloads(items: List[Dict[str, Any]] | None, title: str = "") -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in items or []:
        tid = t.get("id")
        name = t.get("name")
        if not (tid and name):
            continue
        quality = t.get("quality") or "File"
        size = t.get("size") or ""
        token = make_dl_token(tid, name, q=quality, s=size, t=title)
        out.append({
            "quality": quality,
            "size": size,
            "url": f"/d/{token}",
            "file_name": name,
        })
    # sort by quality desc-ish (1080 > 720 > 480 > others)
    def rank(d: Dict[str, Any]) -> int:
        q = (d.get("quality") or "").lower()
        for n in (2160, 1440, 1080, 720, 480, 360):
            if str(n) in q:
                return -n
        return 0
    out.sort(key=rank)
    return out


def _build_movie(tmdb_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    title = data.get("title") or data.get("name") or "Untitled"
    return {
        "tmdb_id": tmdb_id,
        "title": title,
        "year": data.get("release_year"),
        "rating": data.get("rating"),
        "overview": data.get("overview") or "",
        "genres": data.get("genres") or [],
        "runtime": data.get("runtime"),
        "poster": _poster(data),
        "backdrop": data.get("backdrop_path") and f"https://image.tmdb.org/t/p/original{data['backdrop_path']}",
        "downloads": _build_downloads(data.get("telegram"), title=title),
    }


def _build_series(tmdb_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    show_title = data.get("title") or data.get("name") or "Untitled"
    seasons: List[Dict[str, Any]] = []
    for s in data.get("seasons", []) or []:
        episodes: List[Dict[str, Any]] = []
        season_no = s.get("season_number")
        for ep in s.get("episodes", []) or []:
            ep_no = ep.get("episode_number")
            ep_title = ep.get("title") or ""
            label = f"{show_title} · S{int(season_no or 0):02d}E{int(ep_no or 0):02d}"
            if ep_title:
                label = f"{label} — {ep_title}"
            episodes.append({
                "episode_number": ep_no,
                "title": ep_title,
                "overview": ep.get("overview") or "",
                "still": ep.get("still_path") and f"{TMDB_IMG}{ep['still_path']}",
                "downloads": _build_downloads(ep.get("telegram"), title=label),
            })
        seasons.append({
            "season_number": s.get("season_number"),
            "name": s.get("name") or f"Season {s.get('season_number')}",
            "episodes": episodes,
        })
    return {
        "tmdb_id": tmdb_id,
        "title": data.get("title") or data.get("name") or "Untitled",
        "year": data.get("first_air_year") or data.get("release_year"),
        "rating": data.get("rating"),
        "overview": data.get("overview") or "",
        "genres": data.get("genres") or [],
        "is_anime": bool(data.get("is_anime")),
        "poster": _poster(data),
        "backdrop": data.get("backdrop_path") and f"https://image.tmdb.org/t/p/original{data['backdrop_path']}",
        "seasons": seasons,
    }


def _looks_like_series(data: Dict[str, Any]) -> bool:
    return bool(data.get("seasons")) or (data.get("media_type") == "tv")


# ──────────────────────────── routes ───────────────────────────────
@app.before_request
def _auto_track():
    p = request.path
    if p.startswith("/static") or p.startswith("/admin") or p == "/healthz":
        return
    if p == "/":
        _track_web("view", p)
    elif p == "/search":
        _track_web("search", p)
    elif p.startswith("/d/"):
        _track_web("dl_click", p)


@app.context_processor
def inject_globals():
    return {
        "site_name": SITE_NAME,
        "tagline": TAGLINE,
        "current_year": time.strftime("%Y"),
        "tg_channel": f"@{TG_CHANNEL}",
        "tg_channel_url": TG_CHANNEL_URL,
    }


@app.route("/")
def home():
    user_page = max(1, int(request.args.get("page") or 1))
    category = (request.args.get("category") or "all").lower()
    
    # Validate category
    if category not in ["all", "movies", "series", "anime"]:
        category = "all"
    
    # User Page 1 -> API Page 1 & 2
    # User Page 2 -> API Page 3 & 4
    api_page_start = ((user_page - 1) * 2) + 1
    
    items_combined = []
    total_pages_raw = 10
    api_data = {}  # Store latest API response for total_pages
    
    try:
        # Determine which endpoints to fetch from based on category
        if category == "all":
            endpoints = [("movies", "movies", "movies"), ("tvshows", "series", "tv_shows"), ("ani", "anime", "ani")]
            # Fetch one page from each endpoint
            for endpoint, kind, data_key in endpoints:
                api_p = api_page_start
                data = _cached_get(f"{API_BASE}/{endpoint}?sort_by=updated_on:desc&page={api_p}&page_size=20", ttl=0) or {}
                raw_results = data.get(data_key) or data.get(endpoint) or data.get("results") or []
                p_items = [_normalize_catalog_item(it, kind) for it in raw_results if it.get("tmdb_id")]
                items_combined.extend(p_items)
                if data:
                    api_data = data  # Store for total_pages
            # For "all" category, estimate total pages based on first endpoint
            api_total = int(api_data.get("total_pages") or api_data.get("totalPages") or 10)
            total_pages_raw = api_total
        elif category == "movies":
            for i in range(2):
                api_p = api_page_start + i
                data = _cached_get(f"{API_BASE}/movies?sort_by=updated_on:desc&page={api_p}&page_size=20", ttl=0) or {}
                raw_results = data.get("movies") or data.get("results") or []
                p_items = [_normalize_catalog_item(it, "movie") for it in raw_results if it.get("tmdb_id")]
                items_combined.extend(p_items)
                api_data = data
            api_total = int(api_data.get("total_pages") or api_data.get("totalPages") or 20)
            total_pages_raw = (api_total // 2) + (1 if api_total % 2 else 0)
        elif category == "series":
            for i in range(2):
                api_p = api_page_start + i
                data = _cached_get(f"{API_BASE}/tvshows?sort_by=updated_on:desc&page={api_p}&page_size=20", ttl=0) or {}
                raw_results = data.get("tv_shows") or data.get("tvshows") or data.get("series") or data.get("results") or []
                p_items = [_normalize_catalog_item(it, "series") for it in raw_results if it.get("tmdb_id")]
                items_combined.extend(p_items)
                api_data = data
            api_total = int(api_data.get("total_pages") or api_data.get("totalPages") or 20)
            total_pages_raw = (api_total // 2) + (1 if api_total % 2 else 0)
        elif category == "anime":
            for i in range(2):
                api_p = api_page_start + i
                data = _cached_get(f"{API_BASE}/ani?sort_by=updated_on:desc&page={api_p}&page_size=20", ttl=0) or {}
                raw_results = data.get("ani") or data.get("anime") or data.get("results") or data.get("data") or []
                p_items = [_normalize_catalog_item(it, "anime") for it in raw_results if it.get("tmdb_id")]
                items_combined.extend(p_items)
                api_data = data
            api_total = int(api_data.get("total_pages") or api_data.get("totalPages") or 20)
            total_pages_raw = (api_total // 2) + (1 if api_total % 2 else 0)

    except Exception as e:
        app.logger.error(f"Home fetch failed: {e}")
    
    # Shuffle items for variety on every refresh
    random.shuffle(items_combined)

    return render_template(
        "home.html", 
        items=items_combined, 
        page=user_page, 
        total_pages=total_pages_raw,
        category=category
    )






@app.route("/search")
def search():
    q = (request.args.get("q") or "").strip()
    page = max(1, int(request.args.get("page") or 1))
    if not q:
        return redirect(url_for("home"))
    data = _normalize_search(_cached_get(
        f"{API_BASE}/search/?query={requests.utils.quote(q)}&page={page}", ttl=120
    ))
    return render_template("search.html", q=q, **data)


@app.route("/api/live-search")
def live_search():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"results": []})
    return jsonify({"results": _live_search_results(q, limit=8)})



@app.route("/title/<int:tmdb_id>")
def title(tmdb_id: int):
    data = _cached_get(f"{API_BASE}/id/{tmdb_id}", ttl=300) or {}
    if not data:
        abort(404)
    if _looks_like_series(data):
        return render_template("series.html", item=_build_series(tmdb_id, data))
    return render_template("movie.html", item=_build_movie(tmdb_id, data))


@app.route("/d/<token>")
def download_page(token: str):
    """Public download page — shows file info and a Download button.
    The actual upstream URL is never sent to the client; clicking Download
    hits /file/<token> which streams the bytes via this server.
    """
    info = decode_dl_token(token)
    if not info:
        abort(404)
    return render_template(
        "download.html",
        token=token,
        title=info.get("t") or info.get("n") or "Download",
        file_name=info.get("n"),
        quality=info.get("q") or "File",
        size=info.get("s") or "",
        direct_url=f"{DL_BASE}/{info['i']}/{info['n']}",
    )


@app.route("/play/<token>")
def play_page(token: str):
    info = decode_dl_token(token)
    if not info:
        abort(404)
    return render_template(
        "player.html",
        title=info.get("t") or info.get("n") or "Watch",
        quality=info.get("q") or "File",
        stream_url=url_for("play_stream", token=token),
        download_url=url_for("download_stream", token=token),
        mime_type=_guess_media_type(info.get("n") or ""),
        file_name=info.get("n") or "",
    )


@app.route("/player")
def generic_player():
    u = request.args.get("u", "").strip()
    t = request.args.get("t", "Watch").strip() or "Watch"
    q = request.args.get("q", "").strip()
    f = request.args.get("f", "").strip()
    if not u:
        abort(404)
    return render_template(
        "player.html",
        title=t,
        quality=q,
        stream_url=u,
        download_url=u,
        mime_type=_guess_media_type(f or u.split("/")[-1].split("?")[0]),
        file_name=f,
    )


@app.route("/stream/<token>")
def play_stream(token: str):
    info = decode_dl_token(token)
    if not info:
        abort(404)
    upstream_url = f"{DL_BASE}/{info['i']}/{info['n']}"
    result = _proxy_http_stream(upstream_url, info["n"], as_attachment=False)
    if isinstance(result, tuple):
        return result
    return result


@app.route("/file/<token>")
def download_stream(token: str):
    """Stream the file from the upstream CDN, or show a friendly error if unavailable."""
    info = decode_dl_token(token)
    if not info:
        abort(404)
    upstream_url = f"{DL_BASE}/{info['i']}/{info['n']}"
    result = _proxy_http_stream(upstream_url, info["n"], as_attachment=True)
    if isinstance(result, tuple):
        return result
    return result





@app.route("/healthz")
def healthz():
    return {"ok": True, "service": SITE_NAME}, 200


@app.route("/sitemap.xml")
def sitemap():
    """Generate a dynamic sitemap.xml listing main pages and custom movies."""
    try:
        pages = []
        now_str = time.strftime("%Y-%m-%d")
        
        # 1. Main Landing Pages
        main_endpoints = ["home"]
        for ep in main_endpoints:
            try:
                pages.append({
                    "loc": url_for(ep, _external=True),
                    "lastmod": now_str,
                    "priority": "1.0"
                })
            except: pass

        # 2. Custom Movies from MongoDB (High Priority for indexing)
        try:
            cursor = custom_movies_col.find({}, {"custom_id": 1, "added_at": 1}).sort("added_at", -1)
            for movie in cursor:
                loc = url_for("custom_movie", mid=movie["custom_id"], _external=True)
                ts = movie.get("added_at")
                lastmod = time.strftime("%Y-%m-%d", time.gmtime(ts)) if ts else now_str
                pages.append({
                    "loc": loc,
                    "lastmod": lastmod,
                    "priority": "0.8"
                })
        except Exception as e:
            app.logger.error(f"Sitemap Mongo fetch failed: {e}")

        sitemap_xml = render_template("sitemap.xml", pages=pages)
        response = make_response(sitemap_xml)
        response.headers["Content-Type"] = "application/xml"
        return response
    except Exception as e:
        app.logger.error(f"Sitemap generation error: {e}")
        return Response("Error generating sitemap", status=500)


@app.route("/robots.txt")
def robots():
    """Instruct search engines on what to crawl and where the sitemap is."""
    lines = [
        "User-agent: *",
        "Disallow: /admin/",
        "Disallow: /admin",
        "Disallow: /api/",
        "Allow: /",
        f"Sitemap: {url_for('sitemap', _external=True)}"
    ]
    return Response("\n".join(lines), mimetype="text/plain")


# ─────────────────────── ADMIN ROUTES ─────────────────────────
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = None
    if request.method == "POST":
        pwd = request.form.get("password")
        tg_id = request.form.get("tg_id")
        
        expected_pwd = _configured_admin_password()
        expected_tg = _configured_admin_tg_id()
        
        if pwd == expected_pwd and (not expected_tg or tg_id == expected_tg):
            session.permanent = True  # cache for admin
            session["admin_ok"] = True
            return redirect(url_for("admin_dashboard"))
        error = "Incorrect Telegram ID or Password."
    return render_template("admin_login.html", error=error)


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_ok", None)
    return redirect(url_for("admin_login"))


@app.route("/admin", strict_slashes=False)
@_admin_required
def admin_dashboard():
    movie_count = custom_movies_col.count_documents({})
    featured_count = custom_movies_col.count_documents({"is_featured": 1})
    recent_docs = custom_movies_col.find({}, {"custom_id": 1, "title": 1, "year": 1, "poster_url": 1, "is_featured": 1}).sort("added_at", -1).limit(6)
    recent = []
    for d in recent_docs:
        d["id"] = d.get("custom_id")
        recent.append(d)
    return render_template(
        "admin_dashboard.html",
        bot=_bot_stats(), web=_web_stats(),
        movie_count=movie_count, featured_count=featured_count, recent=recent,
    )


@app.route("/admin/movies")
@_admin_required
def admin_movies():
    q = request.args.get("q", "").strip()
    if q:
        query = {"title": {"$regex": q, "$options": "i"}}
        if q.isdigit():
            query = {"$or": [query, {"custom_id": int(q)}]}
        docs = custom_movies_col.find(query).sort([("is_featured", -1), ("added_at", -1)])
    else:
        docs = custom_movies_col.find({}).sort([("is_featured", -1), ("added_at", -1)])
    
    rows = []
    for d in docs:
        d["id"] = d.get("custom_id")
        rows.append(d)
        
    return render_template("admin_movies.html", movies=rows, q=q)


@app.route("/admin/movies/add", methods=["GET", "POST"])
@_admin_required
def admin_add_movie():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        title    = (data.get("title") or "").strip()
        if not title:
            return jsonify({"error": "Title is required"}), 400
        year     = str(data.get("year") or "")
        rating_raw = data.get("rating")
        rating_text = "" if rating_raw is None else str(rating_raw).strip()
        try:
            rating = float(rating_text) if rating_text else None
        except (TypeError, ValueError):
            return jsonify({"error": "Rating must be a number"}), 400
        overview = data.get("overview") or ""
        genres   = json.dumps(data.get("genres") or [])
        poster   = data.get("poster_url") or ""
        backdrop = data.get("backdrop_url") or ""
        source_tmdb_id = _normalize_positive_int(data.get("tmdb_id"))
        is_feat  = 1 if data.get("is_featured") else 0
        kind     = data.get("kind") or "movie"
        
        # Download links: [{quality, size, url}]
        raw_dls  = data.get("downloads") or []
        clean_downloads = []
        for d in raw_dls:
            url = (d.get("url") or "").strip()
            if not url:
                continue
            clean_downloads.append({
                "quality": (d.get("quality") or "").strip(),
                "size": (d.get("size") or "").strip(),
                "url": url,
            })
        downloads = json.dumps(clean_downloads)
        
        mid = int(time.time())
        try:
            custom_movies_col.insert_one({
                "custom_id": mid,
                "tmdb_id": source_tmdb_id,
                "title": title,
                "year": year,
                "rating": rating,
                "overview": overview,
                "poster_url": poster,
                "backdrop_url": backdrop,
                "genres": genres,
                "is_featured": is_feat,
                "downloads": downloads,
                "kind": kind,
                "added_at": int(time.time())
            })
            return jsonify({"ok": True, "id": mid})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
            
    return render_template("admin_add_movie.html", omdb_configured=bool(_configured_omdb_api_key()))


@app.route("/admin/movies/edit/<int:mid>", methods=["GET", "POST"])
@_admin_required
def admin_edit_movie(mid: int):
    doc = custom_movies_col.find_one({"custom_id": mid})
    if not doc:
        abort(404)
        
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        title    = (data.get("title") or "").strip()
        if not title:
            return jsonify({"error": "Title is required"}), 400
        year     = str(data.get("year") or "")
        rating_raw = data.get("rating")
        rating_text = "" if rating_raw is None else str(rating_raw).strip()
        try:
            rating = float(rating_text) if rating_text else None
        except (TypeError, ValueError):
            return jsonify({"error": "Rating must be a number"}), 400
        overview = data.get("overview") or ""
        genres   = json.dumps(data.get("genres") or [])
        poster   = data.get("poster_url") or ""
        backdrop = data.get("backdrop_url") or ""
        is_feat  = 1 if data.get("is_featured") else 0
        kind     = data.get("kind") or "movie"
        
        raw_dls  = data.get("downloads") or []
        clean_downloads = []
        for d in raw_dls:
            url = (d.get("url") or "").strip()
            if not url:
                continue
            clean_downloads.append({
                "quality": (d.get("quality") or "").strip(),
                "size": (d.get("size") or "").strip(),
                "url": url,
            })
        downloads = json.dumps(clean_downloads)
        try:
            custom_movies_col.update_one(
                {"custom_id": mid},
                {"$set": {
                    "title": title,
                    "year": year,
                    "rating": rating,
                    "overview": overview,
                    "poster_url": poster,
                    "backdrop_url": backdrop,
                    "genres": genres,
                    "is_featured": is_feat,
                    "downloads": downloads,
                    "kind": kind
                }}
            )
            return jsonify({"ok": True, "title": title})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return render_template(
        "admin_edit_movie.html",
        movie=doc,
        omdb_configured=bool(_configured_omdb_api_key()),
    )


@app.route("/admin/movies/delete/<int:mid>", methods=["POST"])
@_admin_required
def admin_delete_movie(mid: int):
    custom_movies_col.delete_one({"custom_id": mid})
    return redirect(url_for("admin_movies"))


@app.route("/admin/movies/feature/<int:mid>", methods=["POST"])
@_admin_required
def admin_feature_movie(mid: int):
    doc = custom_movies_col.find_one({"custom_id": mid})
    if doc:
        new_val = 0 if doc.get("is_featured") else 1
        custom_movies_col.update_one({"custom_id": mid}, {"$set": {"is_featured": new_val}})
    return redirect(url_for("admin_movies"))


@app.route("/admin/api/search")
@_admin_required
def admin_api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    data = _cached_get(
        f"{API_BASE}/search/?query={requests.utils.quote(q)}&page=1", ttl=60
    ) or {}
    out = []
    for item in (data.get("results") or [])[:12]:
        out.append({
            "tmdb_id": item.get("tmdb_id"),
            "title":   item.get("title") or item.get("name"),
            "year":    item.get("release_year"),
        })
    return jsonify(out)


@app.route("/admin/settings", methods=["GET", "POST"])
@_admin_required
def admin_settings():
    saved = False
    if request.method == "POST":
        for key in ("tmdb_api_key", "omdb_api_key", "admin_password"):
            v = request.form.get(key, "").strip()
            if v:
                _set_setting(key, v)
        saved = True
    return render_template(
        "admin_settings.html",
        tmdb_key=_get_setting("tmdb_api_key"),
        omdb_key=_get_setting("omdb_api_key"),
        saved=saved,
    )


@app.route("/admin/api/tmdb-search")
@_admin_required
def admin_api_tmdb_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    key = _get_setting("tmdb_api_key") or os.getenv("TMDB_API_KEY", "")
    if not key:
        return jsonify({"error": "No TMDB API key — add it in Settings"}), 400
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/search/multi"
            f"?query={requests.utils.quote(q)}&api_key={key}&include_adult=false",
            timeout=8,
        )
        data = r.json()
        out = []
        for item in (data.get("results") or [])[:10]:
            mt = item.get("media_type", "")
            if mt not in ("movie", "tv"):
                continue
            poster = item.get("poster_path")
            out.append({
                "tmdb_id":  item.get("id"),
                "title":    item.get("title") or item.get("name"),
                "year":     (item.get("release_date") or item.get("first_air_date") or "")[:4],
                "rating":   round(float(item.get("vote_average") or 0), 1),
                "poster":   f"https://image.tmdb.org/t/p/w500{poster}" if poster else None,
                "overview": item.get("overview", ""),
                "type":     mt,
            })
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/admin/api/tmdb-detail")
@_admin_required
def admin_api_tmdb_detail():
    tmdb_id  = request.args.get("id", "").strip()
    mt       = request.args.get("type", "movie").strip()
    key      = _get_setting("tmdb_api_key") or os.getenv("TMDB_API_KEY", "")
    if not key or not tmdb_id:
        return jsonify({"error": "No key or ID"}), 400
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/{mt}/{tmdb_id}?api_key={key}&language=en-US",
            timeout=8,
        )
        d = r.json()
        poster   = d.get("poster_path")
        backdrop = d.get("backdrop_path")
        return jsonify({
            "title":       d.get("title") or d.get("name", ""),
            "year":        (d.get("release_date") or d.get("first_air_date") or "")[:4],
            "rating":      round(float(d.get("vote_average") or 0), 1),
            "overview":    d.get("overview", ""),
            "genres":      [g["name"] for g in (d.get("genres") or [])],
            "poster_url":  f"https://image.tmdb.org/t/p/w500{poster}"   if poster   else "",
            "backdrop_url":f"https://image.tmdb.org/t/p/original{backdrop}" if backdrop else "",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/c/<int:mid>")
def custom_movie(mid: int):
    doc = custom_movies_col.find_one({"custom_id": mid})
    if not doc:
        abort(404)
    item = _build_custom_movie_item(doc)
    return render_template("movie.html", item=item)


@app.route("/cdl/<token>")
def custom_download_page(token: str):
    info = decode_custom_dl_token(token)
    if not info:
        abort(404)
    return render_template(
        "custom_download.html",
        token=token,
        title=info.get("t") or "Download",
        quality=info.get("q") or "HD",
        direct_url=_custom_download_target(token, info["u"]),
    )


@app.route("/cplay/<token>")
def custom_play_page(token: str):
    info = decode_custom_dl_token(token)
    if not info:
        abort(404)
    file_name = info["u"].split("/")[-1].split("?")[0] if info.get("u") else ""
    return render_template(
        "player.html",
        title=info.get("t") or "Watch",
        quality=info.get("q") or "HD",
        stream_url=url_for("custom_play_stream", token=token),
        download_url=url_for("custom_download_stream", token=token),
        mime_type=_guess_media_type(file_name),
        file_name=file_name,
    )


@app.route("/cstream/<token>")
def custom_play_stream(token: str):
    info = decode_custom_dl_token(token)
    if not info:
        abort(404)
    url = info["u"].strip()
    parsed = _parse_telegram_message_url(url)
    if parsed:
        return _stream_telegram_message(*parsed, as_attachment=False)
    fname = url.split("/")[-1].split("?")[0] or "stream"
    return _proxy_http_stream(url, fname, as_attachment=False)


@app.route("/cdlfile/<token>")
def custom_download_stream(token: str):
    info = decode_custom_dl_token(token)
    if not info:
        abort(404)
    url = info["u"].strip()
    parsed = _parse_telegram_message_url(url)
    if parsed:
        return _stream_telegram_message(*parsed, as_attachment=True)
    fname = url.split("/")[-1].split("?")[0] or "download"
    return _proxy_http_stream(url, fname, as_attachment=True)


@app.route("/tstream/c/<chat_id>/<int:message_id>")
def telegram_private_stream(chat_id: str, message_id: int):
    abort(404)


# ─────────────────────── FTP ROUTES ────────────────────────────────────

@app.route("/ftp/<section>/<path:folder>")
def ftp_detail(section: str, folder: str):
    """Detail page for an FTP folder — lists video files with Watch/Download."""
    allowed_sections = {s for s, _ in FTP_SECTIONS}
    if section not in allowed_sections:
        abort(404)
    folder_url = f"{FTP_BASE}/{section}/{urllib.parse.quote(folder)}/"
    files = _list_ftp_folder_files(folder_url)
    parsed = _parse_ftp_dirname(folder)
    # Build download list with signed tokens
    downloads = []
    for f in files:
        token = _make_ftp_token(f["url"], f["name"])
        parsed_q = _parse_ftp_dirname(f["name"])
        quality = parsed_q.get("quality") or parsed.get("quality") or "FTP"
        downloads.append({
            "quality": quality,
            "size": "",
            "url": url_for("ftp_download_page", token=token),
            "file_name": f["name"],
            "stream_url": url_for("ftp_stream", token=token),
            "download_url": url_for("ftp_download_stream", token=token),
        })
    # Build a minimal item dict compatible with movie.html
    item = {
        "title": parsed["title"] or folder,
        "year": parsed.get("year") or "",
        "rating": None,
        "overview": f"Source: FTP — {section}",
        "genres": [],
        "runtime": None,
        "poster": "",
        "backdrop": "",
        "downloads": downloads,
        "kind": "series" if section == "TV_Series" else "movie",
        "is_custom": False,
        "source": "ftp",
    }
    return render_template("movie.html", item=item)


@app.route("/ftp-dl-page/<token>")
def ftp_download_page(token: str):
    """FTP download landing page."""
    info = _decode_ftp_token(token)
    if not info:
        abort(404)
    fname = info.get("n") or info["u"].split("/")[-1]
    return render_template(
        "custom_download.html",
        token=token,
        title=fname,
        quality="FTP",
        direct_url=url_for("ftp_download_stream", token=token),
    )


@app.route("/ftp-stream/<token>")
def ftp_stream(token: str):
    """Proxy-stream an FTP file inline (for Watch)."""
    info = _decode_ftp_token(token)
    if not info:
        abort(404)
    fname = info.get("n") or info["u"].split("/")[-1]
    return _proxy_http_stream(info["u"], fname, as_attachment=False)


@app.route("/ftp-dl/<token>")
def ftp_download_stream(token: str):
    """Proxy-stream an FTP file as attachment (for Download)."""
    info = _decode_ftp_token(token)
    if not info:
        abort(404)
    fname = info.get("n") or info["u"].split("/")[-1]
    return _proxy_http_stream(info["u"], fname, as_attachment=True)


@app.route("/api/ftp-search")
def api_ftp_search():
    """JSON endpoint: search FTP listings by keyword."""
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"results": []})
    entries = _search_ftp_entries(q, limit=20)
    results = [_ftp_entry_to_card(e) for e in entries]
    return jsonify({"results": results, "total": len(results)})



@app.errorhandler(404)
def _404(_):
    return render_template("error.html", code=404, message="Page not found"), 404


@app.errorhandler(500)
def _500(_):
    return render_template("error.html", code=500, message="Something went wrong"), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
