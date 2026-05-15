import requests
import json
import re
import time
import os
from bs4 import BeautifulSoup

FTP_BASE = "https://ftp.ctgfun.com/"
FTP_CATEGORIES = [
    "English/",
    "Indian/Hindi%20Movies/",
    "Indian/South%20Indian%20Movies/",
    "Others/4K%20MOVIES/",
    "Others/Asian%20Movie/",
    "Others/European%20Movies/"
]

def get_rank(q):
    q = q.lower()
    ranks = {'2160p': 100, '4k': 100, '1080p': 80, 'bluray': 85, '720p': 70, 'webrip': 75, 'hdrip': 65, 'web-dl': 75, 'hd': 50, 'hdts': 10, 'hdtc': 5}
    for k, v in ranks.items():
        if k in q: return v
    return 0

def parse_name(name):
    name = requests.utils.unquote(name).replace('.mp4','').replace('.mkv','')
    y_m = re.search(r'\.(19|20)\d{2}\.', name) or re.search(r'[(](19|20)\d{2}[)]', name)
    year = y_m.group(0).strip('.()') if y_m else ""
    q_m = re.search(r'(2160p|1080p|720p|4k|HDRip|WEBRip|BluRay|HDTS|HDTC|Web-DL)', name, flags=re.I)
    quality = q_m.group(0) if q_m else "HD"
    title = name.split(year)[0].replace('.',' ').strip() if year else name.replace('.',' ').strip()
    if q_m: title = title.split(q_m.group(0))[0].strip()
    return title, year, quality

def update():
    print("Generating ftp_movies.json...")
    movie_map = {}
    for cat in FTP_CATEGORIES:
        try:
            r = requests.get(f"{FTP_BASE}{cat}", timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a'):
                h = a.get('href')
                if not h or h.startswith('?') or h == '../': continue
                title, year, quality = parse_name(h.strip('/'))
                if not title: continue
                key = f"{title.lower()}_{year}"
                final_url = f"{FTP_BASE}{cat}{h}"
                if h.endswith('/'):
                    try:
                        ir = requests.get(final_url, timeout=5)
                        isoup = BeautifulSoup(ir.text, 'html.parser')
                        for ia in isoup.find_all('a'):
                            ih = ia.get('href')
                            if ih and ih.lower().endswith(('.mp4', '.mkv')):
                                final_url = f"{final_url}{ih}"
                                break
                    except: continue
                elif not h.lower().endswith(('.mp4', '.mkv')): continue
                
                rank = get_rank(quality)
                if key not in movie_map or rank > movie_map[key]['rank']:
                    movie_map[key] = {
                        "title": title, "year": year, "quality": quality,
                        "url": final_url, "rank": rank, "custom_id": abs(hash(final_url))
                    }
        except Exception as e: print(f"Error {cat}: {e}")
    
    with open("ftp_movies.json", "w") as f:
        json.dump(list(movie_map.values()), f, indent=2)
    print(f"Done! Saved {len(movie_map)} movies.")

if __name__ == "__main__":
    update()
