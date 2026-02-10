# ══════════════════════════════════════════════════════════════
# BEST OF OPERA — APP1: CURADORIA (Production Backend v2)
# YouTube Data API v3 · Scoring Engine · Duplicate Filter
# ══════════════════════════════════════════════════════════════

import os, re, csv, json, unicodedata, asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

# ─── CONFIG ───
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
DATASET_PATH = Path(os.getenv("DATASET_PATH", "./data/dataset_v3_categorizado.csv"))
STATIC_PATH = Path(os.getenv("STATIC_PATH", "./static"))

# ─── GUIA VIVO V3 (calibrated from 267 posts) ───
GUIA = {
    "artists": {
        "Luciano Pavarotti":88,"Andrea Bocelli":85,"Amira Willighagen":82,
        "Maria Callas":80,"Jackie Evancho":78,"Laura Bretan":77,
        "Plácido Domingo":75,"Montserrat Caballé":73,"Jonas Kaufmann":72,
        "Anna Netrebko":71,"José Carreras":70,"Susan Boyle":69,
        "Paul Potts":68,"Renée Fleming":67,"Cecilia Bartoli":66,
        "Diana Damrau":65,"Bryn Terfel":63,"Juan Diego Flórez":62,
        "Rolando Villazón":60,"Angela Gheorghiu":59,"Pretty Yende":58,
        "Jakub Józef Orliński":75,"Katica Illényi":55,"Pentatonix":72,
        "Sarah Brightman":65,"Charlotte Church":60,"Emma Kok":70,
        "Malakai Bayoh":68,"Connie Talbot":62,"Angelina Jordan":74,
        "Soweto Gospel Choir":60,"King's College Choir":58,
        "Vienna Boys Choir":56,"Libera":55,"VOCES8":54,
    },
    "songs": {
        "Nessun Dorma":92,"Ave Maria":88,"O mio babbino caro":85,
        "Time to Say Goodbye":84,"The Prayer":82,"Hallelujah":80,
        "O Sole Mio":78,"La donna è mobile":77,"Casta Diva":76,
        "Pie Jesu":74,"Vissi d'arte":73,"Flower Duet":72,
        "Largo al factotum":71,"O Holy Night":70,"Amazing Grace":68,
        "Vesti la giubba":67,"Granada":65,"Sempre Libera":64,
        "Queen of the Night":75,"Habanera":72,"Baba Yetu":70,
        "Danny Boy":66,"Con te partirò":83,"Caruso":70,
        "I Dreamed a Dream":78,"Never Enough":72,"Bohemian Rhapsody":65,
        "Africa":68,"Miserere mei":62,"Canon in D":60,
    },
    "categories": {
        "Corais Folk Music":65,"Grandes Nomes":61,"Sons Surpreendentes":55,
        "Duetos":54,"Corais Sacro":46,"Jovens Talentos":46,
        "Programa de Audição":50,"Solos":52,"Solos & Duetos":53,
    },
}

# ─── CATEGORY SEARCH QUERIES (multiple per category for broad coverage) ───
CATEGORY_QUERIES = {
    "Grandes Nomes": [
        "Luciano Pavarotti best arias live",
        "Maria Callas opera arias full",
        "Plácido Domingo tenor concert",
        "Andrea Bocelli live opera concert",
        "Montserrat Caballé soprano",
        "Jonas Kaufmann opera aria",
        "Anna Netrebko soprano performance",
        "Renée Fleming opera",
        "Cecilia Bartoli mezzo soprano",
        "José Carreras tenor opera",
        "Angela Gheorghiu soprano",
        "Diana Damrau Queen of the Night",
    ],
    "Jovens Talentos": [
        "child sings opera got talent",
        "young opera singer amazing audition",
        "kid opera surprise performance",
        "Amira Willighagen opera",
        "Jackie Evancho opera",
        "Laura Bretan Nessun Dorma",
        "boy soprano opera performance",
        "young girl opera classical",
        "child prodigy classical singer",
        "teen opera singer talent show",
        "Emma Kok André Rieu",
        "Malakai Bayoh opera",
    ],
    "Corais Folk Music": [
        "best choir performance viral",
        "African choir amazing",
        "choir flash mob opera",
        "a cappella group performance",
        "Pentatonix Hallelujah",
        "world choir folk music",
        "Stellenbosch choir Baba Yetu",
        "gospel choir performance",
        "Perpetuum Jazzile Africa",
        "Ndlovu Youth Choir",
        "virtual choir Eric Whitacre",
        "choir Africa Toto cover",
    ],
    "Corais Sacro": [
        "sacred choir cathedral performance",
        "Kings College Choir Christmas",
        "boys choir sacred music",
        "Allegri Miserere choir",
        "Libera angel voices",
        "sacred choral music",
        "Gregorian chant choir",
        "Ave Maria choir",
        "Vienna Boys Choir",
        "church choir classical",
    ],
    "Solos & Duetos": [
        "best opera aria solo performance",
        "opera duet soprano tenor famous",
        "Nessun Dorma best live version",
        "soprano aria concert",
        "tenor famous aria live",
        "baritone opera aria",
        "countertenor baroque performance",
        "Bocelli Brightman Time Say Goodbye",
        "opera love duet",
        "Jakub Orliński baroque",
    ],
    "Programa de Audição": [
        "opera singer surprise audition",
        "Paul Potts Nessun Dorma audition",
        "Susan Boyle audition",
        "unexpected opera talent show",
        "classical crossover audition",
        "opera golden buzzer",
        "singing competition opera",
        "X Factor opera audition",
        "The Voice opera blind audition",
        "street singer opera discovered",
    ],
    "Sons Surpreendentes": [
        "theremin classical performance",
        "overtone singing polyphonic",
        "unusual instrument classical",
        "glass harp music",
        "musical saw performance",
        "beatbox classical music",
        "Katica Illényi theremin",
        "Anna Maria Hefele overtone",
        "hang drum handpan music",
        "waterphone instrument music",
    ],
}


# ─── POSTED REGISTRY ───
posted_registry = set()

def normalize_str(s: str) -> str:
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9\s]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def is_posted(artist: str, song: str) -> bool:
    na, ns = normalize_str(artist), normalize_str(song)
    if not na and not ns: return False
    stop = {"","the","and","de","di","la","le","el","a","o","in","of"}
    for ra, rs in posted_registry:
        if ra == na and rs == ns: return True
        aw = set(na.split()) - stop; rw = set(ra.split()) - stop
        am = (len(aw&rw) >= min(2,len(aw)) or ra in na or na in ra) if aw and rw else False
        sw = set(ns.split()) - stop; rsw = set(rs.split()) - stop
        sm = (len(sw&rsw) >= min(2,len(sw)) or rs in ns or ns in rs) if sw and rsw else False
        if am and sm: return True
    return False

def load_posted():
    global posted_registry; posted_registry = set()
    if DATASET_PATH.exists():
        with open(DATASET_PATH, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                a = row.get("Nome do Cantor","").strip()
                s = row.get("Nome da Música","").strip()
                if a: posted_registry.add((normalize_str(a), normalize_str(s)))
        print(f"✅ Posted registry: {len(posted_registry)} entries")


# ─── SCORING ───
def calc_score(v: dict) -> dict:
    f = 0
    if v.get("hd"): f += 10
    if v.get("views",0) > 50000: f += 10
    if v.get("year",0) >= datetime.now().year - 10: f += 10
    d = v.get("duration",0)
    if 120 <= d <= 600: f += 10
    f += 10

    g = 0; am = sm = cm = None
    artist_low = v.get("artist","").lower()
    song_low = v.get("song","").lower()

    for name, peso in GUIA["artists"].items():
        nl = name.lower()
        if nl in artist_low or artist_low in nl or (set(nl.split()) & set(artist_low.split())):
            if not am or peso > am[1]: am = (name, peso)
    if am: g += am[1] * 0.3

    for name, peso in GUIA["songs"].items():
        nl = name.lower()
        if nl in song_low or song_low in nl or (set(nl.split()) & set(song_low.split())):
            if not sm or peso > sm[1]: sm = (name, peso)
    if sm: g += sm[1] * 0.25

    cat = v.get("category","")
    cp = GUIA["categories"].get(cat)
    if cp: g += cp * 0.25; cm = (cat, cp)

    g = min(g, 50)
    return {"total": min(round(f+g),100), "fixed":f, "guia":round(g,1),
            "artist_match": am[0] if am else None, "artist_peso": am[1] if am else None,
            "song_match": sm[0] if sm else None, "song_peso": sm[1] if sm else None,
            "cat_peso": cm[1] if cm else None}


def parse_iso_dur(iso: str) -> int:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m: return 0
    h,mn,s = (int(x) if x else 0 for x in m.groups())
    return h*3600 + mn*60 + s

def extract_artist_song(title: str) -> tuple:
    clean = re.sub(r"\s*[\(\[](?:Official|Live|HD|4K|Lyrics|Audio|Video|Concert|Performance|Full).*?[\)\]]","",title,flags=re.I).strip()
    for p in [r"^(.+?)\s*[-–—]\s*[\"'](.+?)[\"']", r"^(.+?)\s*[-–—]\s*(.+?)$",
              r"^(.+?)\s*[:|]\s*(.+?)$", r"^(.+?)\s+(?:sings?|performs?)\s+(.+?)$"]:
        m = re.match(p, clean, re.I)
        if m: return m.group(1).strip(), m.group(2).strip()
    return clean, ""


# ─── YOUTUBE API v3 ───
async def yt_search(query: str, max_results: int = 25) -> list:
    if not YOUTUBE_API_KEY: return []
    async with httpx.AsyncClient(timeout=15) as client:
        r1 = await client.get("https://www.googleapis.com/youtube/v3/search", params={
            "part":"snippet","q":query,"type":"video","maxResults":min(max_results,50),
            "key":YOUTUBE_API_KEY,"videoCategoryId":"10","order":"relevance"})
        if r1.status_code != 200:
            print(f"⚠️ YT search error {r1.status_code}: {r1.text[:200]}")
            return []
        items = r1.json().get("items",[])
        if not items: return []

        vids = [it["id"]["videoId"] for it in items if "videoId" in it.get("id",{})]
        if not vids: return []

        r2 = await client.get("https://www.googleapis.com/youtube/v3/videos", params={
            "part":"contentDetails,statistics","id":",".join(vids),"key":YOUTUBE_API_KEY})
        dm = {}
        if r2.status_code == 200:
            for v in r2.json().get("items",[]): dm[v["id"]] = v

        results = []
        for it in items:
            vid = it["id"].get("videoId","")
            if not vid: continue
            sn = it.get("snippet",{})
            title = sn.get("title",""); pub = sn.get("publishedAt","")[:10]
            yr = int(pub[:4]) if pub else 0
            thumb = sn.get("thumbnails",{}).get("high",{}).get("url","")
            det = dm.get(vid,{})
            dur = parse_iso_dur(det.get("contentDetails",{}).get("duration",""))
            defn = det.get("contentDetails",{}).get("definition","sd")
            views = int(det.get("statistics",{}).get("viewCount",0))
            artist, song = extract_artist_song(title)
            results.append({"video_id":vid, "url":f"https://www.youtube.com/watch?v={vid}",
                "title":title, "artist":artist, "song":song or title,
                "channel":sn.get("channelTitle",""), "year":yr, "published":pub,
                "duration":dur, "views":views, "hd":defn in ("hd","4k"), "thumbnail":thumb, "category":""})
        return results


# ─── APP ───
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_posted()
    print(f"{'✅' if YOUTUBE_API_KEY else '⚠️'} YouTube API {'configured' if YOUTUBE_API_KEY else 'NOT SET'}")
    yield

app = FastAPI(title="Best of Opera — APP1 Curadoria", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _process(videos, query, hide_posted, category=None):
    scored = []
    for v in videos:
        if category: v["category"] = category
        sc = calc_score(v)
        p = is_posted(v.get("artist",""), v.get("song",""))
        scored.append({**v, "score":sc, "posted":p})
    scored.sort(key=lambda x: x["score"]["total"], reverse=True)
    pc = sum(1 for v in scored if v["posted"])
    vis = [v for v in scored if not v["posted"]] if hide_posted else scored
    return {"query":query, "category":category, "total_found":len(scored),
            "posted_hidden":pc if hide_posted else 0, "videos":vis}


@app.get("/api/health")
async def health():
    return {"status":"ok","youtube_api":bool(YOUTUBE_API_KEY),
            "posted_count":len(posted_registry),"guia_artists":len(GUIA["artists"])}

@app.get("/api/search")
async def search(q:str=Query(...), max_results:int=Query(25,ge=1,le=50), hide_posted:bool=Query(True)):
    """Free text search on YouTube, scored by Guia Vivo"""
    raw = await yt_search(q, max_results)
    return _process(raw, q, hide_posted)

@app.get("/api/category/{category}")
async def search_category(category:str, hide_posted:bool=Query(True)):
    """Category search — runs 10-12 parallel YouTube queries for broad coverage"""
    queries = CATEGORY_QUERIES.get(category)
    if not queries: raise HTTPException(404, f"Categoria não encontrada: {category}")
    tasks = [yt_search(q, 15) for q in queries]
    batches = await asyncio.gather(*tasks, return_exceptions=True)
    seen = set(); merged = []
    for batch in batches:
        if isinstance(batch, Exception): continue
        for v in batch:
            if v["video_id"] not in seen:
                seen.add(v["video_id"]); merged.append(v)
    return _process(merged, category, hide_posted, category)

@app.get("/api/ranking")
async def ranking(hide_posted:bool=Query(True)):
    """Ranking: broad search across all categories"""
    all_q = [(cat, qs[0]) for cat, qs in CATEGORY_QUERIES.items()]
    tasks = [yt_search(q, 10) for _, q in all_q]
    batches = await asyncio.gather(*tasks, return_exceptions=True)
    seen = set(); merged = []
    for i, batch in enumerate(batches):
        if isinstance(batch, Exception): continue
        cat = all_q[i][0]
        for v in batch:
            if v["video_id"] not in seen:
                seen.add(v["video_id"]); v["category"]=cat; merged.append(v)
    return _process(merged, "ranking", hide_posted)

@app.get("/api/categories")
async def list_categories():
    return {"categories": list(CATEGORY_QUERIES.keys())}

@app.get("/api/posted")
async def get_posted():
    return {"count":len(posted_registry)}

@app.get("/api/posted/check")
async def check_posted(artist:str="", song:str=""):
    return {"posted":is_posted(artist,song)}

# Serve frontend
if STATIC_PATH.exists():
    @app.get("/")
    async def index(): return FileResponse(STATIC_PATH/"index.html")
    app.mount("/", StaticFiles(directory=str(STATIC_PATH)), name="static")
