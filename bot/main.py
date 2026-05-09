"""
HUBSTREAM PRO — Premium Telegram Bot
High-concurrency async build (handles 1000+ simultaneous requests).

Run:
    pip install -r requirements.txt
    python bot.py
"""

import asyncio
import difflib
import hashlib
import html
import json
import logging
import os
import secrets
import time
from pymongo import MongoClient
from collections import OrderedDict, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web
from telegram import BotCommand, BotCommandScopeAllPrivateChats, ChatMemberUpdated, InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, RetryAfter, TimedOut
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

# ─────────────────────────── CONFIG ────────────────────────────
def _env_base(name: str, default: str) -> str:
    return os.getenv(name, default).strip().rstrip("/")


API_BASE = _env_base("API_BASE", "https://hubstream.sujanbotz.workers.dev/api")
DL_BASE = _env_base(
    "DL_BASE",
    f"{API_BASE[:-4]}/dl" if API_BASE.endswith("/api") else "https://hubstream.sujanbotz.workers.dev/dl",
)
BOT_TOKEN = os.getenv("BOT_TOKEN", "8775047846:AAFWxdXgWJZzqQyZuJBsJh7KYRL_YChyQ-E")

# Public base URL of the HUBSTREAM PRO website. Download links sent to users
# point at the site's /d/<token> page so the upstream URL is never exposed.
def _default_web_base() -> str:
    explicit = os.getenv("WEB_BASE", "").strip().rstrip("/")
    if explicit:
        return explicit
    return "https://ofccc.onrender.com"


WEB_BASE = _default_web_base()

# Signed-token serializer — must use the SAME secret as web/app.py.
from itsdangerous import URLSafeSerializer  # noqa: E402

_DL_SECRET = os.getenv("SESSION_SECRET") or "ofcmovies@secret#key!2024$dl"
_dl_signer = URLSafeSerializer(_DL_SECRET, salt="hubstream-dl-v1")
_custom_dl_signer = URLSafeSerializer(_DL_SECRET, salt="hubstream-custom-dl-v1")


def make_download_url(file_id: Optional[str], file_name: Optional[str],
                      quality: Optional[str] = "", size: Optional[str] = "",
                      title: Optional[str] = "") -> Optional[str]:
    """Return a download URL for the given file.

    When WEB_BASE is a real public host the user can reach, returns a signed
    Flask download-page URL (nicer UX, hides upstream URL).
    When running locally (localhost) returns the direct CDN URL so the
    Telegram inline button still works from the user's browser.
    """
    if not (file_id and file_name):
        return None
    if not _is_local_url(WEB_BASE):
        payload: Dict[str, Any] = {"i": str(file_id), "n": str(file_name)}
        if quality:
            payload["q"] = str(quality)
        if size:
            payload["s"] = str(size)
        if title:
            payload["t"] = str(title)
        token = _dl_signer.dumps(payload)
        return f"{WEB_BASE}/d/{token}"
    return f"{DL_BASE}/{file_id}/{file_name}"


def make_custom_download_url(source_url: Optional[str], title: Optional[str] = "",
                             quality: Optional[str] = "", size: Optional[str] = "") -> Optional[str]:
    """Return a download URL for a custom admin-added movie file."""
    if not source_url:
        return None
    if not _is_local_url(WEB_BASE):
        payload: Dict[str, Any] = {"u": str(source_url)}
        if title:
            payload["t"] = str(title)
        if quality:
            payload["q"] = str(quality)
        if size:
            payload["s"] = str(size)
        token = _custom_dl_signer.dumps(payload)
        return f"{WEB_BASE}/cdl/{token}"
    return str(source_url)

# Owner gate for adding the bot to groups. Only this user may install the bot
# in a group/supergroup; the bot leaves any group added by anyone else.
try:
    OWNER_ID = int(os.getenv("5851349028", "0"))
except ValueError:
    OWNER_ID = 0
OWNER_USERNAME = os.getenv("@freek31", "").lstrip("@").lower()

# Bot identity — applied to the Telegram bot profile on startup.
BOT_DISPLAY_NAME = os.getenv("BOT_DISPLAY_NAME", "OFC Movie Bot")
BOT_SHORT_DESCRIPTION = os.getenv(
    "BOT_SHORT_DESCRIPTION",
    "Premium movie, series & anime delivery — fast, cached, unlimited.",
)
BOT_DESCRIPTION = os.getenv(
    "BOT_DESCRIPTION",
    "🎬 OFC Movie Bot\n"
    "Your premium gateway to movies, series & anime.\n\n"
    "• Search any title — instant results with posters\n"
    "• Choose your quality — 480p · 720p · 1080p · 4K\n"
    "• Tap to download — direct links, no ads\n"
    "• Series & anime with full season/episode menus\n\n"
    "Type a movie or series name to begin.",
)

WEB_PORT = int(os.getenv("BOT_PORT", "5001"))
HTTP_TIMEOUT = 12
HTTP_MAX_CONNECTIONS = 500
HTTP_MAX_PER_HOST = 200
CACHE_TTL_SEARCH = 120
CACHE_TTL_DETAIL = 300
RESULTS_PER_CATEGORY = 5
TG_POOL_SIZE = 256
TG_CONCURRENT_UPDATES = 256
TMDB_IMG_BASE = "https://image.tmdb.org/t/p/w500"
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://xamicc222_db_user:LkOliSVjkBDGyYFT@cluster0.trwwl0v.mongodb.net/?appName=Cluster0")
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo_client["hubstream"]
custom_movies_col = db["custom_movies"]

RECENT_QUERIES_MAX = 2000
SEARCH_CTX_TTL = 1800  # 30 min

# ─── Force-join gate ───
FORCE_JOIN_CHANNEL = os.getenv("FORCE_JOIN_CHANNEL", "@ofcmovie")
FORCE_JOIN_URL = os.getenv(
    "FORCE_JOIN_URL",
    f"https://t.me/{FORCE_JOIN_CHANNEL.lstrip('@')}",
)
MEMBERSHIP_TTL = 300  # 5 min cache

# ─── Auto-cleanup ───
try:
    AUTO_CLEANUP_MINUTES = int(os.getenv("AUTO_CLEANUP_MINUTES", "15"))
except ValueError:
    AUTO_CLEANUP_MINUTES = 15

# ─── Webhook mode (optional) ───
# If WEBHOOK_URL is set (e.g. https://yourapp.replit.app), the bot switches
# from long-polling to webhook mode and serves updates from the same aiohttp
# server. Leave empty to keep polling.
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
# Path is derived from the token so it's unguessable.
WEBHOOK_PATH = "/tg/" + hashlib.blake2b(BOT_TOKEN.encode(), digest_size=12).hexdigest()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("hubstream")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)


# ─────────────────────────── HELPERS ───────────────────────────
def esc(text: Any) -> str:
    if text is None:
        return ""
    return html.escape(str(text))


def fmt_rating(r: Any) -> str:
    try:
        return f"{float(r):.1f}"
    except (TypeError, ValueError):
        return "N/A"


def fmt_size(s: Any) -> str:
    return esc(s) if s else "—"


def extract_poster(item: Dict[str, Any]) -> Optional[str]:
    for key in ("poster_url", "poster", "poster_path", "image", "backdrop_path"):
        v = item.get(key)
        if not v:
            continue
        if isinstance(v, str):
            if v.startswith("http"):
                return v
            return f"{TMDB_IMG_BASE}{v if v.startswith('/') else '/' + v}"
    return None


def quality_emoji(q: str) -> str:
    if not q:
        return "🎞"
    q = str(q).lower()
    if "2160" in q or "4k" in q:
        return "🟣"
    if "1080" in q:
        return "🔵"
    if "720" in q:
        return "🟢"
    if "480" in q:
        return "🟡"
    return "🎞"


# ───────────────────── HTTP CLIENT (singleton) ─────────────────
class HTTPClient:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: Dict[str, Tuple[float, Any]] = {}
        self._cache_lock = asyncio.Lock()
        self._inflight: Dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        from aiohttp.resolver import ThreadedResolver

        resolver = ThreadedResolver()

        connector = aiohttp.TCPConnector(
            limit=HTTP_MAX_CONNECTIONS,
            limit_per_host=HTTP_MAX_PER_HOST,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            resolver=resolver,
        )
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT, connect=5)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": "HubstreamPro/2.0"},
        )

    async def stop(self) -> None:
        if self._session:
            await self._session.close()

    async def get_json(self, url: str, ttl: int = 0, retries: int = 2) -> Any:
        if ttl:
            async with self._cache_lock:
                cached = self._cache.get(url)
                if cached and cached[0] > time.time():
                    return cached[1]
                fut = self._inflight.get(url)
                if fut is not None:
                    return await fut
                fut = asyncio.get_event_loop().create_future()
                self._inflight[url] = fut

        try:
            data = await self._fetch_with_retry(url, retries)
            if ttl:
                async with self._cache_lock:
                    self._cache[url] = (time.time() + ttl, data)
                    self._cleanup_cache_locked()
                fut.set_result(data)
            return data
        except Exception as e:
            if ttl:
                fut.set_exception(e)
            raise
        finally:
            if ttl:
                async with self._cache_lock:
                    self._inflight.pop(url, None)

    async def _fetch_with_retry(self, url: str, retries: int) -> Any:
        last_exc: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                assert self._session is not None
                async with self._session.get(url) as resp:
                    resp.raise_for_status()
                    return await resp.json(content_type=None)
            except aiohttp.client_exceptions.ClientConnectorDNSError as e:
                last_exc = e
                if attempt < retries:
                    log.warning(f"DNS error (attempt {attempt + 1}/{retries + 1}): {e}")
                    await asyncio.sleep(1.0 * (2 ** attempt))
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exc = e
                if attempt < retries:
                    await asyncio.sleep(0.4 * (attempt + 1))
        assert last_exc is not None
        raise last_exc

    def _cleanup_cache_locked(self) -> None:
        if len(self._cache) <= 2048:
            return
        now = time.time()
        expired = [k for k, (exp, _) in self._cache.items() if exp <= now]
        for k in expired:
            self._cache.pop(k, None)


http_client = HTTPClient()


# ─────────────────────── STATS / MAU ───────────────────────────
import aiosqlite

STATS_DB_PATH = os.getenv("STATS_DB_PATH", os.path.join(os.path.dirname(__file__), "stats.db"))


class Stats:
    """Persistent SQLite-backed usage tracker.
    Tables:
        users(user_id PK, username, first_name, first_seen, last_seen)
        events(id PK, user_id, kind, ts)
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._db: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def init(self) -> None:
        self._db = await aiosqlite.connect(self.path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute(
            """CREATE TABLE IF NOT EXISTS users(
                user_id    INTEGER PRIMARY KEY,
                username   TEXT,
                first_name TEXT,
                first_seen INTEGER NOT NULL,
                last_seen  INTEGER NOT NULL
            )"""
        )
        await self._db.execute(
            """CREATE TABLE IF NOT EXISTS events(
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                kind    TEXT NOT NULL,
                ts      INTEGER NOT NULL
            )"""
        )
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_events_user_ts ON events(user_id, ts)")
        await self._db.commit()
        log.info("Stats DB ready at %s", self.path)

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def track(self, user, kind: str) -> None:
        if self._db is None or user is None:
            return
        now = int(time.time())
        try:
            async with self._lock:
                await self._db.execute(
                    """INSERT INTO users(user_id, username, first_name, first_seen, last_seen)
                       VALUES(?, ?, ?, ?, ?)
                       ON CONFLICT(user_id) DO UPDATE SET
                         username = excluded.username,
                         first_name = excluded.first_name,
                         last_seen = excluded.last_seen""",
                    (user.id, user.username or "", user.first_name or "", now, now),
                )
                await self._db.execute(
                    "INSERT INTO events(user_id, kind, ts) VALUES(?, ?, ?)",
                    (user.id, kind, now),
                )
                await self._db.commit()
        except Exception as e:
            log.warning("stats track failed: %s", e)

    async def summary(self) -> Dict[str, Any]:
        if self._db is None:
            return {}
        now = int(time.time())
        day = now - 86_400
        week = now - 7 * 86_400
        month = now - 30 * 86_400

        async def scalar(q: str, *args) -> int:
            cur = await self._db.execute(q, args)
            row = await cur.fetchone()
            await cur.close()
            return int(row[0] or 0) if row else 0

        total_users = await scalar("SELECT COUNT(*) FROM users")
        dau = await scalar("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= ?", day)
        wau = await scalar("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= ?", week)
        mau = await scalar("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= ?", month)
        new_today = await scalar("SELECT COUNT(*) FROM users WHERE first_seen >= ?", day)
        new_week = await scalar("SELECT COUNT(*) FROM users WHERE first_seen >= ?", week)
        new_month = await scalar("SELECT COUNT(*) FROM users WHERE first_seen >= ?", month)
        searches_today = await scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'search' AND ts >= ?", day
        )
        searches_week = await scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'search' AND ts >= ?", week
        )
        searches_month = await scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'search' AND ts >= ?", month
        )
        downloads_today = await scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'download' AND ts >= ?", day
        )
        downloads_month = await scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'download' AND ts >= ?", month
        )
        return {
            "total_users": total_users,
            "dau": dau,
            "wau": wau,
            "mau": mau,
            "new_today": new_today,
            "new_week": new_week,
            "new_month": new_month,
            "searches_today": searches_today,
            "searches_week": searches_week,
            "searches_month": searches_month,
            "downloads_today": downloads_today,
            "downloads_month": downloads_month,
        }


stats = Stats(STATS_DB_PATH)


# ─────────────────────── BOT STATE ─────────────────────────────
# Per-user list of season-message ids to delete on reload
user_season_messages: Dict[int, List[int]] = defaultdict(list)
user_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# Search-context store for pagination: short_id -> (expiry, query)
search_ctx: "OrderedDict[str, Tuple[float, str]]" = OrderedDict()
search_ctx_lock = asyncio.Lock()

# ─── Group → DM redirect ───
# When a user picks a quality inside a group/supergroup, we don't post the
# download link in the group. Instead we mint a one-time token, point them
# at the bot DM via t.me/<bot>?start=<token>, and deliver the file privately
# when /start <token> arrives.
BOT_USERNAME: Optional[str] = None
pending_delivery: "OrderedDict[str, Tuple[float, Dict[str, Any]]]" = OrderedDict()
pending_delivery_lock = asyncio.Lock()
DELIVERY_TOKEN_TTL = 3600  # 1 hour
DELIVERY_TOKEN_MAX = 5000


async def store_delivery_token(payload: Dict[str, Any]) -> str:
    token = "d" + secrets.token_urlsafe(9)
    async with pending_delivery_lock:
        pending_delivery[token] = (time.time() + DELIVERY_TOKEN_TTL, payload)
        # GC a few expired entries on each insert
        now = time.time()
        for k in list(pending_delivery.keys())[:50]:
            if pending_delivery[k][0] < now:
                pending_delivery.pop(k, None)
        while len(pending_delivery) > DELIVERY_TOKEN_MAX:
            pending_delivery.popitem(last=False)
    return token


async def consume_delivery_token(token: str) -> Optional[Dict[str, Any]]:
    async with pending_delivery_lock:
        entry = pending_delivery.pop(token, None)
    if not entry:
        return None
    exp, payload = entry
    if exp < time.time():
        return None
    return payload


async def restore_delivery_token(token: str, payload: Dict[str, Any]) -> None:
    async with pending_delivery_lock:
        pending_delivery[token] = (time.time() + DELIVERY_TOKEN_TTL, payload)

# Global pool of successful queries for "Did you mean…?" suggestions.
recent_queries: "OrderedDict[str, None]" = OrderedDict()
recent_queries_lock = asyncio.Lock()


def query_id(query: str) -> str:
    return hashlib.blake2b(query.lower().encode(), digest_size=6).hexdigest()


async def remember_query(query: str) -> None:
    key = query.strip().lower()
    if not key:
        return
    async with recent_queries_lock:
        recent_queries.pop(key, None)
        recent_queries[key] = None
        while len(recent_queries) > RECENT_QUERIES_MAX:
            recent_queries.popitem(last=False)


async def suggest_queries(query: str, limit: int = 3) -> List[str]:
    key = query.strip().lower()
    async with recent_queries_lock:
        candidates = list(recent_queries.keys())
    return difflib.get_close_matches(key, candidates, n=limit, cutoff=0.55)


async def store_search_ctx(query: str) -> str:
    sid = query_id(query)
    async with search_ctx_lock:
        search_ctx[sid] = (time.time() + SEARCH_CTX_TTL, query)
        # gc
        now = time.time()
        for k in list(search_ctx.keys())[:50]:
            if search_ctx[k][0] < now:
                search_ctx.pop(k, None)
    return sid


async def load_search_ctx(sid: str) -> Optional[str]:
    async with search_ctx_lock:
        entry = search_ctx.get(sid)
        if not entry:
            return None
        exp, q = entry
        if exp < time.time():
            search_ctx.pop(sid, None)
            return None
        return q


# ─── Force-join membership cache ───
membership_cache: Dict[int, Tuple[float, bool]] = {}
membership_lock = asyncio.Lock()


async def is_user_member(bot, user_id: int) -> bool:
    if not FORCE_JOIN_CHANNEL:
        return True
    now = time.time()
    async with membership_lock:
        cached = membership_cache.get(user_id)
        if cached and cached[0] > now:
            return cached[1]
    ok = False
    try:
        member = await bot.get_chat_member(chat_id=FORCE_JOIN_CHANNEL, user_id=user_id)
        ok = member.status not in ("left", "kicked")
    except Exception as e:
        # If the bot isn't an admin of the channel or the channel is wrong,
        # we can't verify — fail open so the bot stays usable.
        log.warning("membership check failed for %s: %s", user_id, e)
        ok = True
    async with membership_lock:
        membership_cache[user_id] = (now + MEMBERSHIP_TTL, ok)
    return ok


async def invalidate_membership(user_id: int) -> None:
    async with membership_lock:
        membership_cache.pop(user_id, None)


def join_gate_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢  Join Channel", url=FORCE_JOIN_URL)],
        [InlineKeyboardButton("✅  I've Joined — Continue", callback_data="jc")],
    ])


JOIN_GATE_TEXT = (
    "🔒 <b>One quick step</b>\n"
    "━━━━━━━━━━━━━━━━━━━━\n"
    f"Join our channel to use OFC Movie Bot:\n"
    f"👉 <b>{esc(FORCE_JOIN_CHANNEL)}</b>\n\n"
    "1. Tap <b>Join Channel</b> below\n"
    "2. Then tap <b>I've Joined</b>"
)


async def gate_or_continue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return True if the user is allowed through; otherwise show the gate and return False."""
    user = update.effective_user
    if not user:
        return True
    if await is_user_member(context.bot, user.id):
        return True
    kb = join_gate_keyboard()
    cq = update.callback_query
    if cq:
        try:
            await cq.answer("Join the channel first.", show_alert=False)
        except Exception:
            pass
        try:
            await context.bot.send_message(
                chat_id=cq.message.chat_id,
                text=JOIN_GATE_TEXT,
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception:
            pass
    elif update.message:
        await update.message.reply_text(
            JOIN_GATE_TEXT,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    return False


def is_owner(user) -> bool:
    if not user:
        return False
    if OWNER_ID and user.id == OWNER_ID:
        return True
    if OWNER_USERNAME and (user.username or "").lower() == OWNER_USERNAME:
        return True
    return False


async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Owner-only group install: leave any group/supergroup the bot was added to
    by anyone other than the configured owner."""
    upd: Optional[ChatMemberUpdated] = update.my_chat_member
    if not upd:
        return
    chat = upd.chat
    if chat.type not in ("group", "supergroup"):
        return

    old_status = upd.old_chat_member.status if upd.old_chat_member else None
    new_status = upd.new_chat_member.status if upd.new_chat_member else None

    # Only act when the bot transitions into the chat
    became_member = old_status in (None, "left", "kicked") and new_status in (
        "member",
        "administrator",
        "restricted",
    )
    if not became_member:
        return

    adder = upd.from_user
    if is_owner(adder):
        log.info("Owner %s added bot to %s (%s) — staying.", adder.id, chat.id, chat.title)
        return

    # Not the owner — politely refuse and leave.
    if not OWNER_ID and not OWNER_USERNAME:
        log.warning(
            "Bot added to group %s by %s, but no OWNER_ID/OWNER_USERNAME set. Leaving anyway.",
            chat.id,
            adder.id if adder else "unknown",
        )

    try:
        await context.bot.send_message(
            chat_id=chat.id,
            text=(
                "🚫 <b>Private bot</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "This bot can only be added to groups by its owner.\n"
                "Please use it in DM instead."
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    try:
        await context.bot.leave_chat(chat_id=chat.id)
        log.info("Left unauthorized group %s (added by %s)", chat.id, adder.id if adder else "?")
    except Exception as e:
        log.warning("failed to leave chat %s: %s", chat.id, e)


async def show_typing(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    except Exception:
        pass


# ─────────────────────── DATA LAYER ────────────────────────────
async def search_all(query: str, page: int = 1) -> Dict[str, Any]:
    url = (
        f"{API_BASE}/search/?query={aiohttp.helpers.quote(query, safe='')}"
        f"&page={page}"
    )
    api_failed = False
    try:
        data = await http_client.get_json(url, ttl=CACHE_TTL_SEARCH)
    except Exception as e:
        log.warning("catalog search failed for %r: %s", query, e)
        data = {}
        api_failed = True
    out: Dict[str, Any] = {
        "movies": [],
        "series": [],
        "anime": [],
        "page": page,
        "total_pages": (data or {}).get("total_pages") or (data or {}).get("totalPages") or 1,
    }
    for item in (data or {}).get("results", []) or []:
        media = item.get("media_type")
        entry = {
            "tmdb_id": item.get("tmdb_id"),
            "title": item.get("title"),
            "year": item.get("release_year"),
            "rating": item.get("rating"),
            "poster": extract_poster(item),
        }
        if media == "movie":
            out["movies"].append(entry)
        elif media == "tv":
            (out["anime"] if item.get("is_anime") else out["series"]).append(entry)

    if api_failed:
        try:
            for item in await search_web_site(query):
                href = str(item.get("href") or "")
                kind = item.get("kind") or "movie"
                entry = {
                    "title": item.get("title"),
                    "year": item.get("year"),
                    "rating": item.get("rating"),
                    "poster": item.get("poster") or "",
                    "web_url": _absolute_web_url(href),
                    "is_custom": href.startswith("/custom/"),
                }
                if kind == "movie":
                    out["movies"].append(entry)
                elif kind == "anime":
                    out["anime"].append(entry)
                else:
                    out["series"].append(entry)
        except Exception as e:
            log.warning("website fallback search failed for %r: %s", query, e)

    if page == 1:
        try:
            custom_entries = await search_custom_movies(query)
            known_urls = {m.get("web_url") for m in out["movies"] if m.get("web_url")}
            deduped_custom = [
                item for item in custom_entries
                if not item.get("web_url") or item.get("web_url") not in known_urls
            ]
            out["movies"] = deduped_custom + out["movies"]
        except Exception as e:
            log.warning("custom movie search failed for %r: %s", query, e)
    return out


async def fetch_details(tmdb_id: int) -> Dict[str, Any]:
    url = f"{API_BASE}/id/{tmdb_id}"
    return await http_client.get_json(url, ttl=CACHE_TTL_DETAIL) or {}


def _build_custom_search_entry(movie: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "custom_id": movie.get("custom_id"),
        "title": movie.get("title"),
        "year": movie.get("year"),
        "rating": movie.get("rating"),
        "poster": movie.get("poster_url") or "",
        "is_custom": True,
    }


def _absolute_web_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if not href.startswith("/"):
        href = f"/{href}"
    return f"{WEB_BASE}{href}"


async def search_web_site(query: str) -> List[Dict[str, Any]]:
    data = await http_client.get_json(
        f"{WEB_BASE}/api/live-search?q={aiohttp.helpers.quote(query, safe='')}",
        ttl=30,
        retries=1,
    ) or {}
    return data.get("results") or []


async def search_custom_movies(query: str) -> List[Dict[str, Any]]:
    try:
        q = {"title": {"$regex": query, "$options": "i"}}
        if query.isdigit():
            q = {"$or": [q, {"custom_id": int(query)}]}
        
        cursor = custom_movies_col.find(q).sort([("is_featured", -1), ("added_at", -1)])
        # we still run this in an executor to avoid blocking the loop heavily, though motor is better
        # but pymongo is synchronous, so we use to_thread
        docs = await asyncio.to_thread(lambda: list(cursor))
        if docs:
            return [_build_custom_search_entry(doc) for doc in docs]
    except Exception:
        pass

    results = await search_web_site(query)
    out: List[Dict[str, Any]] = []
    for item in results:
        href = str(item.get("href") or "")
        if not href.startswith("/custom/"):
            continue
        out.append({
            "title": item.get("title"),
            "year": item.get("year"),
            "rating": item.get("rating"),
            "poster": item.get("poster") or "",
            "is_custom": True,
            "web_url": _absolute_web_url(href),
        })
    return out


async def fetch_custom_movie(custom_id: int) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(
        lambda: custom_movies_col.find_one({"custom_id": custom_id})
    )


def build_custom_movie_payload(movie: Dict[str, Any]) -> Dict[str, Any]:
    downloads = []
    raw_dls = movie.get("downloads")
    if isinstance(raw_dls, str):
        try:
            raw_dls = json.loads(raw_dls)
        except Exception:
            raw_dls = []
    for d in (raw_dls or []):
        source_url = (d.get("url") or "").strip()
        if not source_url:
            continue
        quality = (d.get("quality") or "").strip() or "HD"
        size = (d.get("size") or "").strip()
        downloads.append({
            "quality": quality,
            "size": size,
            "file_id": None,
            "file_name": None,
            "url": make_custom_download_url(source_url, movie.get("title"), quality=quality, size=size),
        })
    return {
        "custom_id": movie.get("custom_id"),
        "title": movie.get("title"),
        "year": movie.get("year"),
        "rating": movie.get("rating"),
        "downloads": downloads,
        "is_custom": True,
    }


def build_movie_payload(tmdb_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    title = data.get("title") or "Untitled"
    downloads = []
    for tele in data.get("telegram", []) or []:
        tid = tele.get("id")
        name = tele.get("name")
        quality = tele.get("quality") or ""
        size = tele.get("size") or ""
        downloads.append(
            {
                "quality": quality,
                "size": size,
                "file_id": tid,
                "file_name": name,
                "url": make_download_url(tid, name, quality=quality, size=size, title=title),
            }
        )
    return {
        "tmdb_id": tmdb_id,
        "title": data.get("title"),
        "year": data.get("release_year"),
        "rating": data.get("rating"),
        "downloads": downloads,
    }


def build_series_payload(tmdb_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    show_title = data.get("title") or "Untitled"
    seasons: List[Dict[str, Any]] = []
    for season in data.get("seasons", []) or []:
        season_no = season.get("season_number") or 0
        episodes: List[Dict[str, Any]] = []
        for ep in season.get("episodes", []) or []:
            ep_no = ep.get("episode_number") or 0
            ep_title = ep.get("title") or ""
            label = f"{show_title} · S{int(season_no):02d}E{int(ep_no):02d}"
            if ep_title:
                label = f"{label} — {ep_title}"
            downloads = []
            for t in ep.get("telegram", []) or []:
                tid = t.get("id")
                name = t.get("name")
                quality = t.get("quality") or ""
                size = t.get("size") or ""
                downloads.append({
                    "quality": quality,
                    "size": size,
                    "file_id": tid,
                    "file_name": name,
                    "url": make_download_url(tid, name, quality=quality, size=size, title=label),
                })
            episodes.append(
                {
                    "episode_number": ep.get("episode_number"),
                    "title": ep.get("title"),
                    "downloads": downloads,
                }
            )
        seasons.append(
            {"season_number": season.get("season_number"), "episodes": episodes}
        )
    return {
        "tmdb_id": tmdb_id,
        "title": data.get("title"),
        "is_anime": bool(data.get("is_anime")),
        "seasons": seasons,
    }


# ─────────────────────── WEB ROUTES ────────────────────────────
async def route_index(_: web.Request) -> web.Response:
    return web.Response(
        text="<h1>OFC MOVIE BOT</h1><p>API up.</p>",
        content_type="text/html",
    )


async def route_search(req: web.Request) -> web.Response:
    q = req.query.get("q", "").strip()
    if not q:
        return web.json_response({"error": "No query provided"}, status=400)
    try:
        return web.json_response(await search_all(q))
    except Exception as e:
        log.exception("search failed")
        return web.json_response({"error": str(e)}, status=500)


async def route_movie(req: web.Request) -> web.Response:
    try:
        tmdb_id = int(req.match_info["tmdb_id"])
        data = await fetch_details(tmdb_id)
        return web.json_response(build_movie_payload(tmdb_id, data))
    except Exception as e:
        log.exception("movie failed")
        return web.json_response({"error": str(e)}, status=500)


async def route_series(req: web.Request) -> web.Response:
    try:
        tmdb_id = int(req.match_info["tmdb_id"])
        data = await fetch_details(tmdb_id)
        return web.json_response(build_series_payload(tmdb_id, data))
    except Exception as e:
        log.exception("series failed")
        return web.json_response({"error": str(e)}, status=500)


async def route_health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


PLAYER_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>__TITLE__</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root {
    --bg: var(--tg-theme-bg-color, #0b0b0f);
    --fg: var(--tg-theme-text-color, #f5f5f7);
    --hint: var(--tg-theme-hint-color, #8a8a90);
    --accent: var(--tg-theme-button-color, #2ea6ff);
    --accent-fg: var(--tg-theme-button-text-color, #ffffff);
  }
  * { box-sizing: border-box; }
  html, body {
    margin: 0; padding: 0; height: 100%;
    background: var(--bg); color: var(--fg);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    overflow: hidden;
  }
  .wrap {
    display: flex; flex-direction: column;
    height: 100vh; height: 100dvh;
  }
  header {
    padding: 12px 16px 8px;
    flex: 0 0 auto;
    border-bottom: 1px solid rgba(255,255,255,0.06);
  }
  .title {
    font-size: 15px; font-weight: 600; line-height: 1.3;
    overflow: hidden; text-overflow: ellipsis;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
  }
  .meta { font-size: 12px; color: var(--hint); margin-top: 2px; }
  .stage {
    flex: 1 1 auto; position: relative;
    display: flex; align-items: center; justify-content: center;
    background: #000;
  }
  video {
    width: 100%; height: 100%;
    object-fit: contain; background: #000;
  }
  .overlay {
    position: absolute; inset: 0;
    display: flex; align-items: center; justify-content: center;
    text-align: center; padding: 24px;
    color: var(--fg); font-size: 14px; line-height: 1.5;
    background: rgba(0,0,0,0.55); backdrop-filter: blur(8px);
    pointer-events: none;
  }
  .overlay.hidden { display: none; }
  .overlay .inner { max-width: 320px; }
  .overlay h2 { margin: 0 0 8px; font-size: 16px; font-weight: 600; }
  .overlay p { margin: 0 0 16px; color: var(--hint); }
  .overlay a {
    pointer-events: auto;
    display: inline-block; padding: 10px 18px; border-radius: 10px;
    background: var(--accent); color: var(--accent-fg);
    font-weight: 600; text-decoration: none; font-size: 14px;
  }
  .spinner {
    position: absolute; top: 50%; left: 50%;
    width: 38px; height: 38px; margin: -19px 0 0 -19px;
    border: 3px solid rgba(255,255,255,0.15);
    border-top-color: var(--accent);
    border-radius: 50%; animation: spin 0.9s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="title" id="title">__TITLE__</div>
      <div class="meta" id="meta">__META__</div>
    </header>
    <div class="stage">
      <div class="spinner" id="spin"></div>
      <video id="v" controls playsinline preload="metadata" crossorigin="anonymous"></video>
      <div class="overlay hidden" id="err">
        <div class="inner">
          <h2>Can't stream this file</h2>
          <p id="errmsg">Your browser can't play this format directly. Try downloading it instead.</p>
          <a id="dl" href="#" target="_blank" rel="noopener">Open file</a>
        </div>
      </div>
    </div>
  </div>
<script>
(function () {
  var tg = window.Telegram && window.Telegram.WebApp;
  if (tg) {
    try { tg.ready(); tg.expand(); } catch (e) {}
    try { tg.setHeaderColor && tg.setHeaderColor('secondary_bg_color'); } catch (e) {}
    try { tg.BackButton && tg.BackButton.show(); tg.BackButton.onClick(function(){ tg.close(); }); } catch (e) {}
  }
  var qs = new URLSearchParams(window.location.search);
  var url = qs.get('u') || '';
  var v = document.getElementById('v');
  var spin = document.getElementById('spin');
  var err = document.getElementById('err');
  var errmsg = document.getElementById('errmsg');
  var dl = document.getElementById('dl');
  if (!url) {
    spin.style.display = 'none';
    err.classList.remove('hidden');
    errmsg.textContent = 'No file URL provided.';
    dl.style.display = 'none';
    return;
  }
  dl.href = url;
  v.src = url;
  v.addEventListener('loadedmetadata', function () { spin.style.display = 'none'; });
  v.addEventListener('canplay', function () { spin.style.display = 'none'; v.play().catch(function(){}); });
  v.addEventListener('error', function () {
    spin.style.display = 'none';
    err.classList.remove('hidden');
  });
  // If metadata never loads in 12s, treat as unsupported.
  setTimeout(function () {
    if (!v.readyState) {
      spin.style.display = 'none';
      err.classList.remove('hidden');
    }
  }, 12000);
})();
</script>
</body>
</html>
"""


async def route_player(req: web.Request) -> web.Response:
    title = req.query.get("t", "Now Playing").strip() or "Now Playing"
    quality = req.query.get("q", "").strip()
    meta = quality if quality else ""
    body = (
        PLAYER_HTML
        .replace("__TITLE__", html.escape(title))
        .replace("__META__", html.escape(meta))
    )
    resp = web.Response(text=body, content_type="text/html")
    # Telegram Web Apps must be embeddable; ensure no restrictive headers.
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp


def build_web_app(application=None) -> web.Application:
    app = web.Application()
    app.router.add_get("/", route_index)
    app.router.add_get("/healthz", route_health)
    app.router.add_get("/search/universal", route_search)
    app.router.add_get("/movie/{tmdb_id}", route_movie)
    app.router.add_get("/series/{tmdb_id}", route_series)
    app.router.add_get("/player", route_player)

    # Optional Telegram webhook receiver — only mounted when WEBHOOK_URL is set
    # and an Application is provided.
    if application is not None and WEBHOOK_URL:
        async def route_webhook(request: web.Request) -> web.Response:
            # Verify Telegram secret token if configured
            if WEBHOOK_SECRET:
                token = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
                if token != WEBHOOK_SECRET:
                    return web.Response(status=401, text="unauthorized")
            try:
                data = await request.json()
            except Exception:
                return web.Response(status=400, text="bad json")
            try:
                update = Update.de_json(data, application.bot)
                await application.process_update(update)
            except Exception as e:
                log.exception("webhook processing failed: %s", e)
            return web.Response(text="ok")

        app.router.add_post(WEBHOOK_PATH, route_webhook)
        log.info("Webhook endpoint mounted at POST %s", WEBHOOK_PATH)

    return app


# ─────────────────────── PRESENTATION ──────────────────────────
DIVIDER = "━━━━━━━━━━━━━━━━━━━━"


def card_movie(m: Dict[str, Any]) -> str:
    label = "Custom Movie" if m.get("is_custom") else "Movie"
    return (
        f"🎬 <b>{esc(m.get('title'))}</b>\n"
        f"<i>{label} · {esc(m.get('year') or 'N/A')}</i>\n"
        f"⭐ <b>{fmt_rating(m.get('rating'))}</b>"
    )


def card_series(s: Dict[str, Any], anime: bool = False) -> str:
    icon = "🎌" if anime else "📺"
    label = "Anime" if anime else "Series"
    return (
        f"{icon} <b>{esc(s.get('title'))}</b>\n"
        f"<i>{label} · {esc(s.get('year') or 'N/A')}</i>\n"
        f"⭐ <b>{fmt_rating(s.get('rating'))}</b>"
    )


def downloads_keyboard(
    downloads: List[Dict[str, Any]],
    cb_prefix: str,
    back_cb: Optional[str] = None,
    back_label: str = "◀  Back to Episodes",
) -> InlineKeyboardMarkup:
    """Build a download picker. Each button is a callback (no exposed URL).
    Tapping a button triggers in-app file delivery via the bot."""
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for idx, d in enumerate(downloads):
        q = d.get("quality") or "File"
        size = d.get("size") or ""
        label = f"{quality_emoji(q)} {q}" + (f" · {size}" if size else "")
        row.append(
            InlineKeyboardButton(label, callback_data=f"{cb_prefix}|{idx}")
        )
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if not rows:
        rows = [[InlineKeyboardButton("No links available", callback_data="noop")]]
    if back_cb:
        rows.append([InlineKeyboardButton(back_label, callback_data=back_cb)])
    return InlineKeyboardMarkup(rows)


# ─────────────────────── BOT HANDLERS ──────────────────────────
WELCOME = (
    "🎬 <b>OFC MOVIE BOT</b>\n"
    f"{DIVIDER}\n"
    "Premium movie · series · anime delivery.\n\n"
    "<b>How it works</b>\n"
    "• Type any title — I'll find it.\n"
    "• Tap a card to view qualities.\n"
    "• Tap a quality button — your file starts instantly.\n\n"
    "<i>Old season menus auto-clean when you load a new season.</i>"
)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    # Deep-link delivery: /start <token> minted from a group quality button.
    # In this mode we do NOT show the welcome screen — we silently deliver
    # the download link with title for that one specific item.
    args = context.args or []
    if args:
        token = args[0].strip()
        payload = await consume_delivery_token(token)
        if payload is None:
            await update.message.reply_text(
                "⚠️ <b>This download link has expired or was already used.</b>\n"
                "Go back to the group and tap the quality button again.",
                parse_mode=ParseMode.HTML,
            )
            return
        # Force-join still applies. If the gate blocks, restore the token so
        # the user can retry once they've joined.
        if not await gate_or_continue(update, context):
            await restore_delivery_token(token, payload)
            return
        await stats.track(update.effective_user, "download")
        await deliver_pending_download(update, context, payload)
        return

    await stats.track(update.effective_user, "start")
    if not await gate_or_continue(update, context):
        return
    await update.message.reply_text(WELCOME, parse_mode=ParseMode.HTML)


def _is_local_url(url: str) -> bool:
    return url.startswith("http://localhost") or url.startswith("http://127.")


def _dl_card_line(url: str) -> str:
    return "✅ <b>Download ready — tap the button below</b>"


def _dl_keyboard(url: str) -> InlineKeyboardMarkup:
    """Always point the user at the web /d/<token> download page (ad countdown).
    url is already the signed /d/<token> URL produced by make_download_url."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("🌐 Open Download Page", url=url)]])


async def deliver_pending_download(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    payload: Dict[str, Any],
) -> None:
    """Render the download card for a token-based delivery in the user's DM."""
    chat_id = update.effective_chat.id
    kind = payload.get("kind")
    tmdb_id = int(payload.get("tmdb_id") or 0)
    custom_id = int(payload.get("custom_id") or 0)
    idx = int(payload.get("idx", -1))

    if kind == "mv":
        data = await fetch_details(tmdb_id)
        m = build_movie_payload(tmdb_id, data)
        downloads = m.get("downloads") or []
        if idx < 0 or idx >= len(downloads):
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ This download is no longer available. Search the title again.",
            )
            return
        d = downloads[idx]
        quality = d.get("quality") or "File"
        size = d.get("size") or ""
        url = d.get("url")
        title = m.get("title") or "Movie"
        year = m.get("year") or "N/A"
        if not url:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ This file is unavailable right now. Please try another quality.",
            )
            return
        text = (
            f"🎬 <b>{esc(title)}</b> <i>({esc(year)})</i>\n"
            f"{quality_emoji(quality)} <b>{esc(quality)}</b>"
            + (f" · {esc(size)}" if size else "")
            + "\n━━━━━━━━━━━━━━━━━━━━\n"
            + _dl_card_line(url)
        )
        keyboard = _dl_keyboard(url)
    elif kind == "cm":
        row = await fetch_custom_movie(custom_id)
        if not row:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ This custom movie is no longer available.",
            )
            return
        m = build_custom_movie_payload(row)
        downloads = m.get("downloads") or []
        if idx < 0 or idx >= len(downloads):
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ This download is no longer available. Search the title again.",
            )
            return
        d = downloads[idx]
        quality = d.get("quality") or "File"
        size = d.get("size") or ""
        url = d.get("url")
        title = m.get("title") or "Custom Movie"
        year = m.get("year") or "N/A"
        if not url:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ This file is unavailable right now. Please try another quality.",
            )
            return
        text = (
            f"🎬 <b>{esc(title)}</b> <i>({esc(year)})</i>\n"
            f"{quality_emoji(quality)} <b>{esc(quality)}</b>"
            + (f" · {esc(size)}" if size else "")
            + "\n━━━━━━━━━━━━━━━━━━━━\n"
            + _dl_card_line(url)
        )
        keyboard = _dl_keyboard(url)
    elif kind == "ep":
        season_no = int(payload.get("season") or 0)
        episode_no = int(payload.get("episode") or 0)
        data = await fetch_details(tmdb_id)
        s_payload = build_series_payload(tmdb_id, data)
        season = next(
            (s for s in s_payload["seasons"] if s.get("season_number") == season_no),
            None,
        )
        ep = None
        if season:
            ep = next(
                (e for e in season["episodes"] if e.get("episode_number") == episode_no),
                None,
            )
        downloads = (ep or {}).get("downloads") or []
        if idx < 0 or idx >= len(downloads):
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ This episode download is no longer available.",
            )
            return
        d = downloads[idx]
        quality = d.get("quality") or "File"
        size = d.get("size") or ""
        url = d.get("url")
        title = s_payload.get("title") or "Series"
        icon = "🎌" if s_payload.get("is_anime") else "📺"
        if not url:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ This file is unavailable right now. Please try another quality.",
            )
            return
        text = (
            f"{icon} <b>{esc(title)}</b>\n"
            f"S{season_no:02d} · E{episode_no:02d}"
            + (f" — {esc(ep.get('title'))}" if ep and ep.get("title") else "")
            + f"\n{quality_emoji(quality)} <b>{esc(quality)}</b>"
            + (f" · {esc(size)}" if size else "")
            + "\n━━━━━━━━━━━━━━━━━━━━\n"
            + _dl_card_line(url)
        )
        keyboard = _dl_keyboard(url)
    else:
        await context.bot.send_message(
            chat_id=chat_id, text="⚠️ Unknown download request."
        )
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await stats.track(update.effective_user, "help")
    if not await gate_or_continue(update, context):
        return
    await update.message.reply_text(WELCOME, parse_mode=ParseMode.HTML)


async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await stats.track(update.effective_user, "about")
    if not await gate_or_continue(update, context):
        return
    await update.message.reply_text(
        "🎬 <b>OFC MOVIE BOT</b>\n"
        f"{DIVIDER}\n"
        "Premium movie · series · anime delivery on Telegram.\n"
        "Built for speed — async, cached, scales to thousands of users.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user = update.effective_user
    if not is_owner(user):
        # Silently ignore for non-owners — keep the command undiscoverable.
        return
    s = await stats.summary()
    if not s:
        await update.message.reply_text("Stats DB not ready.")
        return
    text = (
        "📊 <b>OFC Movie Bot · Stats</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Total users:</b> {s['total_users']:,}\n\n"
        "<b>Active users</b>\n"
        f"  • DAU (24h):   <b>{s['dau']:,}</b>\n"
        f"  • WAU (7d):    <b>{s['wau']:,}</b>\n"
        f"  • MAU (30d):   <b>{s['mau']:,}</b>\n\n"
        "<b>New users</b>\n"
        f"  • Today:       <b>{s['new_today']:,}</b>\n"
        f"  • This week:   <b>{s['new_week']:,}</b>\n"
        f"  • This month:  <b>{s['new_month']:,}</b>\n\n"
        "<b>Searches</b>\n"
        f"  • Today:       <b>{s['searches_today']:,}</b>\n"
        f"  • This week:   <b>{s['searches_week']:,}</b>\n"
        f"  • This month:  <b>{s['searches_month']:,}</b>\n\n"
        "<b>Downloads</b>\n"
        f"  • Today:       <b>{s['downloads_today']:,}</b>\n"
        f"  • This month:  <b>{s['downloads_month']:,}</b>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await stats.track(update.effective_user, "search_cmd")
    if not await gate_or_continue(update, context):
        return
    args = " ".join(context.args or []).strip() if hasattr(context, "args") else ""
    if not args:
        await update.message.reply_text(
            "🔍 <b>Search</b>\nSend the title after the command.\n"
            "<i>Example:</i>  <code>/search Inception</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    update.message.text = args
    await on_text(update, context)


async def safe_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, msg_id: int) -> None:
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except (BadRequest, TimedOut):
        pass
    except Exception:
        pass


def schedule_delete(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    msg_id: int,
    minutes: Optional[int] = None,
) -> None:
    """Schedule a message for auto-deletion after `minutes`. No-op if disabled."""
    delay_min = AUTO_CLEANUP_MINUTES if minutes is None else minutes
    if delay_min <= 0:
        return

    async def _delayed():
        try:
            await asyncio.sleep(delay_min * 60)
            await safe_delete(context, chat_id, msg_id)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.debug("scheduled delete failed: %s", e)

    context.application.create_task(_delayed())


async def clear_season_messages(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with user_locks[user_id]:
        ids = user_season_messages.pop(user_id, [])
    if ids:
        await asyncio.gather(
            *(safe_delete(context, user_id, mid) for mid in ids),
            return_exceptions=True,
        )


def track_season_message(user_id: int, msg_id: int) -> None:
    user_season_messages[user_id].append(msg_id)


async def send_result_card(
    bot,
    chat_id: int,
    text: str,
    keyboard: InlineKeyboardMarkup,
    poster: Optional[str],
) -> None:
    if poster:
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=poster,
                caption=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            return
        except Exception as e:
            log.debug("poster send failed (%s); falling back to text", e)
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def deliver_results(
    bot,
    chat_id: int,
    query: str,
    results: Dict[str, Any],
) -> None:
    movies = results["movies"][:RESULTS_PER_CATEGORY]
    series = results["series"][:RESULTS_PER_CATEGORY]
    anime = results["anime"][:RESULTS_PER_CATEGORY]

    sends = []
    for m in movies:
        if m.get("web_url"):
            label = "🎬  Open Movie Page" if m.get("is_custom") else "🌐  Open on Website"
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton(label, url=m["web_url"])]]
            )
        else:
            callback = (
                f"cm|{m['custom_id']}"
                if m.get("is_custom")
                else f"mv|{m['tmdb_id']}"
            )
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("🎬  Get Download Links", callback_data=callback)]]
            )
        sends.append(send_result_card(bot, chat_id, card_movie(m), kb, m.get("poster")))
    for s in series:
        if s.get("web_url"):
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("🌐  Open on Website", url=s["web_url"])]]
            )
        else:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("📺  View Seasons", callback_data=f"sl|{s['tmdb_id']}|s")]]
            )
        sends.append(send_result_card(bot, chat_id, card_series(s), kb, s.get("poster")))
    for a in anime:
        if a.get("web_url"):
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("🌐  Open on Website", url=a["web_url"])]]
            )
        else:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("🎌  View Seasons", callback_data=f"sl|{a['tmdb_id']}|a")]]
            )
        sends.append(
            send_result_card(bot, chat_id, card_series(a, anime=True), kb, a.get("poster"))
        )

    await asyncio.gather(*sends, return_exceptions=True)

    page = int(results.get("page") or 1)
    total_pages = int(results.get("total_pages") or 1)
    if page < total_pages:
        sid = await store_search_ctx(query)
        next_btn = InlineKeyboardMarkup(
            [[InlineKeyboardButton(
                f"More results ▶  (page {page + 1}/{total_pages})",
                callback_data=f"pg|{sid}|{page + 1}",
            )]]
        )
        await bot.send_message(
            chat_id=chat_id,
            text=f"<i>Showing page {page} of {total_pages} for</i> <b>{esc(query)}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=next_btn,
        )


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.text:
        return
    query = msg.text.strip()
    if not query or query.startswith("/"):
        return

    if not await gate_or_continue(update, context):
        return

    # React to the user's message with a 🎬 emoji
    try:
        await context.bot.set_message_reaction(
            chat_id=msg.chat_id,
            message_id=msg.message_id,
            reaction=[{"type": "emoji", "emoji": "🎬"}],
        )
    except Exception:
        pass  # Reactions may not be supported in all chat types

    await stats.track(update.effective_user, "search")
    await show_typing(context, msg.chat_id)

    loading = await msg.reply_text(
        f"🔍 <b>Searching</b> · <code>{esc(query)}</code>",
        parse_mode=ParseMode.HTML,
    )

    try:
        results = await search_all(query)
    except Exception as e:
        log.warning("search error: %s", e)
        await loading.edit_text(
            "⚠️ Couldn't reach the catalog right now. Try again in a moment.",
            parse_mode=ParseMode.HTML,
        )
        return

    has_any = bool(results["movies"] or results["series"] or results["anime"])

    if not has_any:
        suggestions = await suggest_queries(query)
        if suggestions:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton(f"🔎 {s.title()}", callback_data=f"sg|{query_id(s)}")]
                 for s in suggestions]
            )
            # remember each suggestion ctx so the callback can resolve it
            for s in suggestions:
                await store_search_ctx(s)
            await loading.edit_text(
                f"🫥 No results for <b>{esc(query)}</b>.\n\n"
                f"<b>Did you mean…?</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        else:
            await loading.edit_text(
                f"🫥 No results for <b>{esc(query)}</b>.\nTry a different spelling.",
                parse_mode=ParseMode.HTML,
            )
        return

    await remember_query(query)
    await safe_delete(context, msg.chat_id, loading.message_id)
    await deliver_results(context.bot, msg.chat_id, query, results)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    if not cq or not cq.data:
        return

    parts = cq.data.split("|")
    action = parts[0]

    # "I've Joined" confirmation — re-check membership and clear cache.
    # Owns its own callback answer (no pre-answer above).
    if action == "jc":
        await invalidate_membership(cq.from_user.id)
        if await is_user_member(context.bot, cq.from_user.id):
            try:
                await cq.answer("Welcome aboard! 🎉", show_alert=False)
            except Exception:
                pass
            try:
                await cq.edit_message_text(WELCOME, parse_mode=ParseMode.HTML)
            except BadRequest:
                await context.bot.send_message(
                    chat_id=cq.message.chat_id,
                    text=WELCOME,
                    parse_mode=ParseMode.HTML,
                )
        else:
            try:
                await cq.answer(
                    "We still don't see you in the channel. Please join first.",
                    show_alert=True,
                )
            except Exception:
                pass
        return

    # Unified delivery: every quality tap — whether in a group/supergroup or
    # in the bot's DM — runs through the /start <token> automation. Telegram
    # opens the bot DM with the start parameter, cmd_start consumes the
    # token, and the title + download card is delivered privately.
    #
    # IMPORTANT: this MUST be the first (and only) `cq.answer` call for this
    # callback — Telegram allows a callback to be answered exactly once, so
    # any prior empty `cq.answer()` would silently break the URL redirect.
    if action in ("dlm", "dle", "dlc") and BOT_USERNAME:
        if not await gate_or_continue(update, context):
            return
        try:
            if action == "dlm":
                payload = {
                    "kind": "mv",
                    "tmdb_id": int(parts[1]),
                    "idx": int(parts[2]),
                    "requester_id": cq.from_user.id,
                }
            elif action == "dle":
                payload = {
                    "kind": "ep",
                    "tmdb_id": int(parts[1]),
                    "season": int(parts[2]),
                    "episode": int(parts[3]),
                    "idx": int(parts[4]),
                    "requester_id": cq.from_user.id,
                }
            else:
                payload = {
                    "kind": "cm",
                    "custom_id": int(parts[1]),
                    "idx": int(parts[2]),
                    "requester_id": cq.from_user.id,
                }
        except (ValueError, IndexError):
            try:
                await cq.answer()
            except Exception:
                pass
            return

        token = await store_delivery_token(payload)
        deep_url = f"https://t.me/{BOT_USERNAME}?start={token}"
        chat_type = cq.message.chat.type if cq.message and cq.message.chat else "private"
        log.info(
            "redirect→DM user=%s chat=%s action=%s token=%s",
            cq.from_user.id, chat_type, action, token,
        )

        # Cleanest UX: callback answer with url= opens the bot DM directly
        # with /start <token> — no extra message, no extra tap.
        opened = False
        try:
            await cq.answer(url=deep_url)
            opened = True
        except Exception as e:
            log.warning("answer(url=...) failed (%s) — falling back to button", e)

        if not opened:
            # Fallback path: post an inline button that opens the deep link.
            try:
                await cq.answer()
            except Exception:
                pass
            try:
                await context.bot.send_message(
                    chat_id=cq.message.chat_id,
                    text=(
                        "📥 <b>Get your download privately</b>\n"
                        "Tap below to open the bot in DM and receive your link."
                    ),
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=cq.message.message_id,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📥 Open in DM", url=deep_url)
                    ]]),
                )
            except Exception as e:
                log.warning("fallback DM-button send failed: %s", e)
        return

    # Everything else: ack the callback then dispatch normally.
    try:
        await cq.answer()
    except Exception:
        pass

    if not await gate_or_continue(update, context):
        return

    try:
        if action == "mv":
            await handle_movie_qualities(cq, context, int(parts[1]))
        elif action == "cm":
            await handle_custom_movie_qualities(cq, context, int(parts[1]))
        elif action == "sl":
            await handle_season_list(cq, context, int(parts[1]))
        elif action == "se":
            # se|tmdb_id|season_number
            await handle_season_episodes(cq, context, int(parts[1]), int(parts[2]))
        elif action == "ep":
            # ep|tmdb_id|season|episode
            await handle_episode_qualities(
                cq, context, int(parts[1]), int(parts[2]), int(parts[3])
            )
        elif action == "dlm":
            # dlm|tmdb_id|idx
            await handle_movie_delivery(cq, context, int(parts[1]), int(parts[2]))
        elif action == "dlc":
            # dlc|custom_id|idx
            await handle_custom_movie_delivery(cq, context, int(parts[1]), int(parts[2]))
        elif action == "dle":
            # dle|tmdb_id|season|episode|idx
            await handle_episode_delivery(
                cq, context, int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
            )
        elif action == "pg":
            # pg|sid|page
            await handle_pagination(cq, context, parts[1], int(parts[2]))
        elif action == "sg":
            # sg|sid  (Did you mean… suggestion tap)
            await handle_suggestion(cq, context, parts[1])
        elif action == "noop":
            return
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
    except Exception as e:
        log.exception("callback error: %s", e)
        try:
            await cq.message.reply_text("⚠️ Something went wrong. Please try again.")
        except Exception:
            pass


async def handle_movie_qualities(cq, context, tmdb_id: int) -> None:
    data = await fetch_details(tmdb_id)
    payload = build_movie_payload(tmdb_id, data)
    if not payload["downloads"]:
        await cq.message.reply_text("🚫 No download links found for this title.")
        return
    text = (
        f"🎬 <b>{esc(payload['title'])}</b> "
        f"<i>({esc(payload.get('year') or 'N/A')})</i>\n"
        f"⭐ <b>{fmt_rating(payload.get('rating'))}</b>\n"
        f"{DIVIDER}\n"
        "Choose a quality to start your download:"
    )
    sent = await cq.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=downloads_keyboard(
            payload["downloads"],
            cb_prefix=f"dlm|{tmdb_id}",
        ),
    )
    schedule_delete(context, sent.chat_id, sent.message_id)


async def handle_custom_movie_qualities(cq, context, custom_id: int) -> None:
    row = await fetch_custom_movie(custom_id)
    if not row:
        await cq.message.reply_text("🚫 This custom movie is no longer available.")
        return
    payload = build_custom_movie_payload(row)
    if not payload["downloads"]:
        await cq.message.reply_text("🚫 No download links found for this title.")
        return
    text = (
        f"🎬 <b>{esc(payload['title'])}</b> "
        f"<i>({esc(payload.get('year') or 'N/A')})</i>\n"
        f"⭐ <b>{fmt_rating(payload.get('rating'))}</b>\n"
        f"{DIVIDER}\n"
        "Choose a quality to start your download:"
    )
    sent = await cq.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=downloads_keyboard(
            payload["downloads"],
            cb_prefix=f"dlc|{custom_id}",
        ),
    )
    schedule_delete(context, sent.chat_id, sent.message_id)


def _grid(buttons: List[InlineKeyboardButton], per_row: int) -> List[List[InlineKeyboardButton]]:
    return [buttons[i : i + per_row] for i in range(0, len(buttons), per_row)]


async def handle_season_list(cq, context, tmdb_id: int) -> None:
    data = await fetch_details(tmdb_id)
    payload = build_series_payload(tmdb_id, data)
    seasons = [s for s in payload["seasons"] if s.get("season_number") is not None]
    if not seasons:
        await cq.message.reply_text("🚫 No seasons available.")
        return

    icon = "🎌" if payload.get("is_anime") else "📺"
    lines = [f"{icon} <b>{esc(payload['title'])}</b>", DIVIDER, "<b>Available Seasons</b>", ""]
    for s in seasons:
        sn = s["season_number"]
        ep_count = len(s.get("episodes") or [])
        lines.append(f"  <b>S{sn:02d}</b> — {ep_count} episode{'s' if ep_count != 1 else ''}")
    lines += ["", "<i>Tap a season below to view episodes.</i>"]

    buttons = [
        InlineKeyboardButton(f"S{s['season_number']:02d}", callback_data=f"se|{tmdb_id}|{s['season_number']}")
        for s in seasons
    ]
    kb = InlineKeyboardMarkup(_grid(buttons, 5))

    await cq.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=kb)


async def handle_season_episodes(cq, context, tmdb_id: int, season_no: int) -> None:
    user_id = cq.from_user.id
    await clear_season_messages(user_id, context)

    data = await fetch_details(tmdb_id)
    payload = build_series_payload(tmdb_id, data)
    target = next(
        (s for s in payload["seasons"] if s.get("season_number") == season_no), None
    )
    episodes = (target or {}).get("episodes") or []
    if not episodes:
        sent = await cq.message.reply_text("🚫 No episodes found for this season.")
        track_season_message(user_id, sent.message_id)
        return

    icon = "🎌" if payload.get("is_anime") else "📺"
    lines = [
        f"{icon} <b>{esc(payload['title'])}</b> · Season {season_no:02d}",
        DIVIDER,
        f"<b>{len(episodes)} Episodes</b>",
        "",
    ]
    for ep in episodes:
        en = ep.get("episode_number")
        title = ep.get("title") or "Untitled"
        try:
            en_str = f"E{int(en):02d}"
        except (TypeError, ValueError):
            en_str = f"E{esc(en)}"
        lines.append(f"  <b>{en_str}</b>  ·  {esc(title)}")
    lines += ["", "<i>Tap an episode below to choose a quality.</i>"]

    buttons = []
    for ep in episodes:
        en = ep.get("episode_number")
        if en is None:
            continue
        try:
            label = f"{int(en):02d}"
        except (TypeError, ValueError):
            label = str(en)
        buttons.append(
            InlineKeyboardButton(label, callback_data=f"ep|{tmdb_id}|{season_no}|{en}")
        )
    kb = InlineKeyboardMarkup(_grid(buttons, 5))

    sent = await cq.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=kb
    )
    track_season_message(user_id, sent.message_id)


async def handle_episode_qualities(
    cq, context, tmdb_id: int, season_no: int, episode_no: int
) -> None:
    data = await fetch_details(tmdb_id)
    payload = build_series_payload(tmdb_id, data)
    season = next(
        (s for s in payload["seasons"] if s.get("season_number") == season_no), None
    )
    ep = None
    if season:
        ep = next(
            (e for e in season["episodes"] if e.get("episode_number") == episode_no), None
        )
    if not ep or not ep.get("downloads"):
        try:
            await cq.answer("No links for this episode.", show_alert=True)
        except Exception:
            pass
        return

    icon = "🎌" if payload.get("is_anime") else "📺"
    text = (
        f"{icon} <b>{esc(payload['title'])}</b>\n"
        f"S{int(season_no):02d} · E{int(episode_no):02d} — {esc(ep.get('title') or '')}\n"
        f"{DIVIDER}\n<b>Select a quality to download:</b>"
    )
    kb = downloads_keyboard(
        ep["downloads"],
        cb_prefix=f"dle|{tmdb_id}|{season_no}|{episode_no}",
        back_cb=f"se|{tmdb_id}|{season_no}",
    )

    # Edit the episode-grid message in place so user is taken straight
    # to the quality picker — no jumping between messages.
    try:
        await cq.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        schedule_delete(context, cq.message.chat_id, cq.message.message_id)
    except BadRequest:
        sent = await cq.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        schedule_delete(context, sent.chat_id, sent.message_id)


async def deliver_file(
    cq,
    context: ContextTypes.DEFAULT_TYPE,
    download: Dict[str, Any],
    caption: str,
) -> None:
    """Deliver a download to the user.

    Sends the user to the web /d/<token> download page (with 15-sec ad
    countdown) via an inline button. The raw CDN URL is never exposed.
    """
    chat_id = cq.message.chat_id
    file_id   = download.get("file_id")
    file_name = download.get("file_name")
    quality   = download.get("quality") or ""
    size      = download.get("size") or ""

    await stats.track(cq.from_user, "download")

    try:
        await cq.answer("🌐 Opening download page…", show_alert=False)
    except Exception:
        pass

    # Build the signed web-page URL regardless of WEB_BASE setting.
    # If WEB_BASE is localhost the user can't reach it from their device,
    # so fall back to the direct CDN URL in that case.
    web_url = download.get("url") or make_download_url(file_id, file_name, quality=quality, size=size)
    if not web_url:
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ This file is unavailable right now. Please try another quality.",
        )
        return

    if "/cdl/" in web_url:
        stream_url = web_url.replace("/cdl/", "/cdlfile/")
    else:
        stream_url = web_url.replace("/d/", "/file/")

    # Extract title from caption loosely
    try:
        title = caption.split("<b>")[1].split("</b>")[0]
    except Exception:
        title = "Now Playing"

    player_url = f"{WEB_BASE}/player?u={aiohttp.helpers.quote(stream_url, safe='')}&t={aiohttp.helpers.quote(title, safe='')}"

    text = (
        f"{caption}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🌐 <b>Tap below to stream or download</b>\n"
        f"<i>A short wait is shown on the download page before it starts.</i>"
    )
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Stream (Web App)", web_app=WebAppInfo(url=player_url))],
            [InlineKeyboardButton("🌐 Open Download Page", url=web_url)]
        ])
    )


async def handle_movie_delivery(cq, context, tmdb_id: int, idx: int) -> None:
    data = await fetch_details(tmdb_id)
    payload = build_movie_payload(tmdb_id, data)
    downloads = payload.get("downloads") or []
    if idx < 0 or idx >= len(downloads):
        try:
            await cq.answer("That option expired. Reopen the title.", show_alert=True)
        except Exception:
            pass
        return
    d = downloads[idx]
    quality = d.get("quality") or "File"
    size = d.get("size") or ""
    caption = (
        f"🎬 <b>{esc(payload['title'])}</b> "
        f"<i>({esc(payload.get('year') or 'N/A')})</i>\n"
        f"{quality_emoji(quality)} <b>{esc(quality)}</b>"
        + (f" · {esc(size)}" if size else "")
    )
    await deliver_file(cq, context, d, caption)


async def handle_custom_movie_delivery(cq, context, custom_id: int, idx: int) -> None:
    row = await fetch_custom_movie(custom_id)
    if not row:
        try:
            await cq.answer("That title is no longer available.", show_alert=True)
        except Exception:
            pass
        return
    payload = build_custom_movie_payload(row)
    downloads = payload.get("downloads") or []
    if idx < 0 or idx >= len(downloads):
        try:
            await cq.answer("That option expired. Reopen the title.", show_alert=True)
        except Exception:
            pass
        return
    d = downloads[idx]
    quality = d.get("quality") or "File"
    size = d.get("size") or ""
    caption = (
        f"🎬 <b>{esc(payload['title'])}</b> "
        f"<i>({esc(payload.get('year') or 'N/A')})</i>\n"
        f"{quality_emoji(quality)} <b>{esc(quality)}</b>"
        + (f" · {esc(size)}" if size else "")
    )
    await deliver_file(cq, context, d, caption)


async def handle_episode_delivery(
    cq, context, tmdb_id: int, season_no: int, episode_no: int, idx: int
) -> None:
    data = await fetch_details(tmdb_id)
    payload = build_series_payload(tmdb_id, data)
    season = next(
        (s for s in payload["seasons"] if s.get("season_number") == season_no), None
    )
    ep = None
    if season:
        ep = next(
            (e for e in season["episodes"] if e.get("episode_number") == episode_no), None
        )
    downloads = (ep or {}).get("downloads") or []
    if idx < 0 or idx >= len(downloads):
        try:
            await cq.answer("That option expired. Reopen the episode.", show_alert=True)
        except Exception:
            pass
        return
    d = downloads[idx]
    quality = d.get("quality") or "File"
    size = d.get("size") or ""
    icon = "🎌" if payload.get("is_anime") else "📺"
    caption = (
        f"{icon} <b>{esc(payload['title'])}</b>\n"
        f"S{int(season_no):02d} · E{int(episode_no):02d}"
        + (f" — {esc(ep.get('title'))}" if ep and ep.get("title") else "")
        + f"\n{quality_emoji(quality)} <b>{esc(quality)}</b>"
        + (f" · {esc(size)}" if size else "")
    )
    await deliver_file(cq, context, d, caption)


async def handle_pagination(cq, context, sid: str, page: int) -> None:
    query = await load_search_ctx(sid)
    if not query:
        try:
            await cq.answer("Search expired. Send the title again.", show_alert=True)
        except Exception:
            pass
        return
    chat_id = cq.message.chat_id
    await show_typing(context, chat_id)
    # Replace the "More results" button with a loading state
    try:
        await cq.edit_message_text(
            f"🔍 <i>Loading page {page} for</i> <b>{esc(query)}</b>",
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass
    try:
        results = await search_all(query, page=page)
    except Exception as e:
        log.warning("pagination search error: %s", e)
        try:
            await cq.edit_message_text(
                "⚠️ Couldn't fetch the next page. Try again in a moment.",
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            pass
        return
    has_any = bool(results["movies"] or results["series"] or results["anime"])
    await safe_delete(context, chat_id, cq.message.message_id)
    if not has_any:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🫥 No more results for <b>{esc(query)}</b>.",
            parse_mode=ParseMode.HTML,
        )
        return
    await deliver_results(context.bot, chat_id, query, results)


async def handle_suggestion(cq, context, sid: str) -> None:
    query = await load_search_ctx(sid)
    if not query:
        try:
            await cq.answer("Suggestion expired. Send the title again.", show_alert=True)
        except Exception:
            pass
        return
    chat_id = cq.message.chat_id
    await show_typing(context, chat_id)
    try:
        await cq.edit_message_text(
            f"🔍 <b>Searching</b> · <code>{esc(query)}</code>",
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass
    try:
        results = await search_all(query)
    except Exception as e:
        log.warning("suggestion search error: %s", e)
        try:
            await cq.edit_message_text(
                "⚠️ Couldn't reach the catalog right now. Try again in a moment.",
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            pass
        return
    has_any = bool(results["movies"] or results["series"] or results["anime"])
    if not has_any:
        try:
            await cq.edit_message_text(
                f"🫥 No results for <b>{esc(query)}</b>.",
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            pass
        return
    await remember_query(query)
    await safe_delete(context, chat_id, cq.message.message_id)
    await deliver_results(context.bot, chat_id, query, results)


# ─────────────────────── BOOTSTRAP ─────────────────────────────
async def run() -> None:
    await http_client.start()
    await stats.init()

    request = HTTPXRequest(
        connection_pool_size=TG_POOL_SIZE,
        read_timeout=30,
        write_timeout=30,
        connect_timeout=15,
        pool_timeout=10,
    )
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .concurrent_updates(TG_CONCURRENT_UPDATES)
        .build()
    )

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("search", cmd_search))
    application.add_handler(CommandHandler("about", cmd_about))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    application.add_handler(
        ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER)
    )

    # Start aiohttp web server in same loop (mounts webhook endpoint if enabled)
    web_app = build_web_app(application=application)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEB_PORT)
    await site.start()
    log.info("Web server listening on :%s", WEB_PORT)
    log.info("Catalog upstream: API_BASE=%s DL_BASE=%s", API_BASE, DL_BASE)

    # Start bot
    await application.initialize()

    # Cache the bot's username so we can mint t.me/<bot>?start=<token> deep
    # links for the group-to-DM download redirect.
    global BOT_USERNAME
    try:
        me = await application.bot.get_me()
        BOT_USERNAME = me.username
        log.info("Bot identity: @%s (id=%s)", BOT_USERNAME, me.id)
    except Exception as e:
        log.warning("Failed to fetch bot username: %s", e)

    try:
        await application.bot.set_my_commands(
            [
                BotCommand("start", "🚀  Launch OFC Movie Bot"),
                BotCommand("search", "🔍  Search a movie / series / anime"),
                BotCommand("help", "💡  How to use the bot"),
                BotCommand("about", "🎬  About OFC Movie Bot"),
            ],
            scope=BotCommandScopeAllPrivateChats(),
        )
    except Exception as e:
        log.warning("set_my_commands failed: %s", e)

    # Apply bot profile identity (name + descriptions). Telegram silently 400s
    # if the value is unchanged, so we treat exceptions as non-fatal.
    for label, coro in (
        ("name", application.bot.set_my_name(BOT_DISPLAY_NAME)),
        ("short_description", application.bot.set_my_short_description(BOT_SHORT_DESCRIPTION)),
        ("description", application.bot.set_my_description(BOT_DESCRIPTION)),
    ):
        try:
            await coro
            log.info("Updated bot %s.", label)
        except Exception as e:
            log.info("set_my_%s skipped/failed: %s", label, e)

    if not OWNER_ID and not OWNER_USERNAME:
        log.warning(
            "OWNER_ID and OWNER_USERNAME are both unset — the bot will leave "
            "ANY group it is added to. Set OWNER_ID to your Telegram user ID."
        )
    else:
        log.info("Owner gate active (OWNER_ID=%s, OWNER_USERNAME=%s).", OWNER_ID or "—", OWNER_USERNAME or "—")

    await application.start()

    if WEBHOOK_URL:
        # Webhook mode — register URL with Telegram and skip polling.
        full_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        try:
            await application.bot.set_webhook(
                url=full_url,
                secret_token=WEBHOOK_SECRET or None,
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
            )
            log.info("Webhook mode active → %s", full_url)
        except Exception as e:
            log.error("Failed to set webhook (%s) — falling back to polling.", e)
            await application.updater.start_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES,
            )
    else:
        # Long-polling mode (default). Make sure no stale webhook exists.
        try:
            await application.bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await application.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )

    log.info("Bot is running. Press Ctrl+C to stop.")

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        log.info("Shutting down...")
        try:
            if application.updater and application.updater.running:
                await application.updater.stop()
        except Exception:
            pass
        if WEBHOOK_URL:
            try:
                await application.bot.delete_webhook()
            except Exception:
                pass
        await application.stop()
        await application.shutdown()
        await runner.cleanup()
        await http_client.stop()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
