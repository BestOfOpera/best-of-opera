# ══════════════════════════════════════════════════════════════
# BEST OF OPERA — MOTOR V7
# Seed Rotation · V7 Scoring · Anti-Spam · Quota Control
# ══════════════════════════════════════════════════════════════

import os, re, csv, json, unicodedata, asyncio, tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, Response

import database as db

# ─── CONFIG ───
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
DATASET_PATH = Path(os.getenv("DATASET_PATH", "./dataset_v3_categorizado.csv"))
STATIC_PATH = Path(os.getenv("STATIC_PATH", "./static"))
PLAYLIST_ID = "PLGjiuPqoIDSnphyXIetV6iwm4-3K-fvKk"
APP_PASSWORD = os.getenv("APP_PASSWORD", "opera2026")

# ─── ANTI-SPAM (appended to all YouTube searches) ───
ANTI_SPAM = "-karaoke -piano -tutorial -lesson -reaction -review -lyrics -chords"

# ─── DOWNLOAD CONFIG ───
download_semaphore = asyncio.Semaphore(2)

def sanitize_filename(s: str) -> str:
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    s = s.strip('. ')
    return s[:200] if s else 'video'

# ─── CATEGORIES V7 (6 categories, each with 6 seeds for rotation) ───
CATEGORIES_V7 = {
    "icones": {
        "name": "Icones",
        "emoji": "👑",
        "desc": "Lendas eternas da opera",
        "seeds": [
            "Luciano Pavarotti best live aria opera performance",
            "Maria Callas iconic soprano opera aria live",
            "Placido Domingo tenor concert opera live",
            "Montserrat Caballe soprano legendary opera performance",
            "Jose Carreras three tenors concert live opera",
            "Enrico Caruso historical opera tenor recording",
        ]
    },
    "estrelas": {
        "name": "Estrelas",
        "emoji": "⭐",
        "desc": "Estrelas modernas da opera",
        "seeds": [
            "Andrea Bocelli live concert opera performance",
            "Anna Netrebko soprano opera performance live",
            "Jonas Kaufmann tenor opera aria live concert",
            "Pretty Yende soprano opera live performance",
            "Juan Diego Florez tenor opera live performance",
            "Jakub Jozef Orlinski countertenor baroque opera live",
        ]
    },
    "hits": {
        "name": "Hits",
        "emoji": "🎵",
        "desc": "Arias e musicas mais populares",
        "seeds": [
            "Nessun Dorma best live performance opera tenor",
            "Ave Maria opera live soprano performance beautiful",
            "Time to Say Goodbye Con te partiro live opera",
            "O Sole Mio best live tenor performance opera",
            "The Prayer duet opera live performance beautiful",
            "Hallelujah best live performance classical choir",
        ]
    },
    "surpreendente": {
        "name": "Surpreendente",
        "emoji": "🎭",
        "desc": "Performances virais e inesperadas",
        "seeds": [
            "flash mob opera surprise public performance amazing",
            "unexpected opera singer street performance viral",
            "theremin classical music amazing performance instrument",
            "overtone singing polyphonic incredible vocal technique",
            "opera singer surprise restaurant wedding performance",
            "unusual instrument classical performance viral amazing",
        ]
    },
    "talent": {
        "name": "Talent",
        "emoji": "🌟",
        "desc": "Revelacoes em shows de talentos",
        "seeds": [
            "opera singer audition got talent amazing judges shocked",
            "golden buzzer opera performance talent show incredible",
            "child sings opera audition judges crying talent show",
            "Susan Boyle I Dreamed a Dream first audition",
            "Paul Potts Nessun Dorma Britain got talent audition",
            "unexpected opera voice talent show blind audition amazing",
        ]
    },
    "corais": {
        "name": "Corais",
        "emoji": "🎶",
        "desc": "Corais e grupos vocais",
        "seeds": [
            "amazing choir opera performance live concert best",
            "Pentatonix Hallelujah live concert performance",
            "African choir incredible performance amazing vocal",
            "boys choir sacred music cathedral performance beautiful",
            "a cappella group classical opera performance live",
            "choir flash mob opera surprise performance public",
        ]
    },
}

# ─── SCORING V7 DATA ───
ELITE_HITS = [
    "Nessun Dorma", "Ave Maria", "O mio babbino caro", "Time to Say Goodbye",
    "The Prayer", "Hallelujah", "O Sole Mio", "La donna e mobile",
    "Con te partiro", "Casta Diva", "Queen of the Night", "Flower Duet",
    "I Dreamed a Dream", "Never Enough", "Vissi d'arte", "Pie Jesu",
    "O Holy Night", "Amazing Grace", "Sempre Libera", "Habanera",
    "Granada", "Largo al factotum", "Vesti la giubba", "Baba Yetu",
    "Danny Boy", "Caruso", "Bohemian Rhapsody",
]

POWER_NAMES = [
    "Luciano Pavarotti", "Andrea Bocelli", "Maria Callas",
    "Placido Domingo", "Montserrat Caballe", "Jonas Kaufmann",
    "Anna Netrebko", "Amira Willighagen", "Jackie Evancho",
    "Laura Bretan", "Susan Boyle", "Paul Potts", "Pentatonix",
    "Sarah Brightman", "Jose Carreras", "Renee Fleming",
    "Cecilia Bartoli", "Diana Damrau", "Jakub Jozef Orlinski",
    "Emma Kok", "Malakai Bayoh", "Pretty Yende", "Angela Gheorghiu",
    "Juan Diego Florez", "Rolando Villazon", "Bryn Terfel",
]

VOICE_KEYWORDS = [
    "soprano", "tenor", "baritone", "mezzo", "countertenor",
    "aria", "opera", "classical voice", "live concert",
]

INSTITUTIONAL_CHANNELS = [
    "royal opera", "met opera", "metropolitan opera", "la scala",
    "wiener staatsoper", "bbc", "arte concert", "deutsche oper",
    "opera de paris", "sydney opera", "andre rieu",
]

CATEGORY_SPECIALTY = {
    "icones": ["three tenors", "la scala", "royal opera", "pavarotti and friends", "farewell", "legendary"],
    "estrelas": ["recital", "gala concert", "concert hall", "philharmonic", "arena di verona"],
    "hits": ["encore", "standing ovation", "duet", "best version", "iconic"],
    "surpreendente": ["flash mob", "street", "theremin", "overtone", "handpan", "surprise", "viral"],
    "talent": ["audition", "golden buzzer", "got talent", "x factor", "the voice", "judges"],
    "corais": ["choir", "ensemble", "a cappella", "choral", "voices", "gospel"],
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
                s = row.get("Nome da Musica","").strip()
                if a: posted_registry.add((normalize_str(a), normalize_str(s)))
        print(f"✅ Posted registry: {len(posted_registry)} entries")


# ─── SCORING V7 ───
def calc_score_v7(v: dict, category: str = None) -> dict:
    reasons = []
    total = 0
    title_low = (v.get("title") or "").lower()
    artist_low = (v.get("artist") or "").lower()
    song_low = (v.get("song") or "").lower()
    channel_low = (v.get("channel") or "").lower()

    # 1. elite_hits +15
    hit_match = None
    for hit in ELITE_HITS:
        hl = hit.lower()
        if hl in song_low or hl in title_low:
            hit_match = hit
            break
    if hit_match:
        total += 15
        reasons.append({"tag": "elite_hit", "label": hit_match, "points": 15})

    # 2. power_names +15
    name_match = None
    for name in POWER_NAMES:
        nl = name.lower()
        if nl in artist_low or nl in channel_low or nl in title_low:
            name_match = name
            break
    if name_match:
        total += 15
        reasons.append({"tag": "power_name", "label": name_match, "points": 15})

    # 3. specialty +25 (dual match OR deep category keywords)
    specialty_match = None
    if hit_match and name_match:
        specialty_match = f"{name_match} + {hit_match}"
    elif category and category in CATEGORY_SPECIALTY:
        for kw in CATEGORY_SPECIALTY[category]:
            if kw in title_low or kw in channel_low:
                specialty_match = kw
                break
    if specialty_match:
        total += 25
        reasons.append({"tag": "specialty", "label": specialty_match, "points": 25})

    # 4. voice +15
    voice_match = None
    for kw in VOICE_KEYWORDS:
        if kw in title_low:
            voice_match = kw
            break
    if voice_match:
        total += 15
        reasons.append({"tag": "voice", "label": voice_match, "points": 15})

    # 5. institutional +10
    inst_match = None
    for ch in INSTITUTIONAL_CHANNELS:
        if ch in channel_low:
            inst_match = v.get("channel", "")
            break
    if inst_match:
        total += 10
        reasons.append({"tag": "institutional", "label": inst_match, "points": 10})

    # 6. quality +10 (HD)
    if v.get("hd"):
        total += 10
        reasons.append({"tag": "quality", "label": "HD", "points": 10})

    # 7. views +10
    views = v.get("views", 0)
    if views > 100000:
        total += 10
        reasons.append({"tag": "views", "label": f"{views:,}", "points": 10})

    total = min(total, 100)

    return {
        "total": total,
        "reasons": reasons,
        # Compat fields for DB storage
        "fixed": 0,
        "guia": 0.0,
        "artist_match": name_match,
        "song_match": hit_match,
    }


# ─── HELPERS ───
def parse_iso_dur(iso: str) -> int:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m: return 0
    h, mn, s = (int(x) if x else 0 for x in m.groups())
    return h*3600 + mn*60 + s

def extract_artist_song(title: str) -> tuple:
    clean = re.sub(r"\s*[\(\[](?:Official|Live|HD|4K|Lyrics|Audio|Video|Concert|Performance|Full).*?[\)\]]", "", title, flags=re.I).strip()
    for p in [r"^(.+?)\s*[-\u2013\u2014]\s*[\"'](.+?)[\"']", r"^(.+?)\s*[-\u2013\u2014]\s*(.+?)$",
              r"^(.+?)\s*[:|]\s*(.+?)$", r"^(.+?)\s+(?:sings?|performs?)\s+(.+?)$"]:
        m = re.match(p, clean, re.I)
        if m: return m.group(1).strip(), m.group(2).strip()
    return clean, ""


# ─── YOUTUBE API v3 (with anti-spam & quota tracking) ───
async def yt_search(query: str, max_results: int = 25) -> list:
    if not YOUTUBE_API_KEY: return []
    async with httpx.AsyncClient(timeout=15) as client:
        r1 = await client.get("https://www.googleapis.com/youtube/v3/search", params={
            "part": "snippet", "q": query, "type": "video",
            "maxResults": min(max_results, 50),
            "key": YOUTUBE_API_KEY, "videoCategoryId": "10", "order": "relevance"
        })
        if r1.status_code != 200:
            print(f"⚠️ YT search error {r1.status_code}: {r1.text[:200]}")
            return []
        items = r1.json().get("items", [])
        if not items: return []

        vids = [it["id"]["videoId"] for it in items if "videoId" in it.get("id", {})]
        if not vids: return []

        r2 = await client.get("https://www.googleapis.com/youtube/v3/videos", params={
            "part": "contentDetails,statistics", "id": ",".join(vids), "key": YOUTUBE_API_KEY
        })
        dm = {}
        if r2.status_code == 200:
            for v in r2.json().get("items", []): dm[v["id"]] = v

        # Register quota usage
        try:
            db.register_quota_usage(search_calls=1, detail_calls=1)
        except Exception as e:
            print(f"⚠️ Quota tracking error: {e}")

        results = []
        for it in items:
            vid = it["id"].get("videoId", "")
            if not vid: continue
            sn = it.get("snippet", {})
            title = sn.get("title", "")
            pub = sn.get("publishedAt", "")[:10]
            yr = int(pub[:4]) if pub else 0
            thumb = sn.get("thumbnails", {}).get("high", {}).get("url", "")
            det = dm.get(vid, {})
            dur = parse_iso_dur(det.get("contentDetails", {}).get("duration", ""))
            defn = det.get("contentDetails", {}).get("definition", "sd")
            views = int(det.get("statistics", {}).get("viewCount", 0))
            artist, song = extract_artist_song(title)
            results.append({
                "video_id": vid, "url": f"https://www.youtube.com/watch?v={vid}",
                "title": title, "artist": artist, "song": song or title,
                "channel": sn.get("channelTitle", ""), "year": yr, "published": pub,
                "duration": dur, "views": views, "hd": defn in ("hd", "4k"),
                "thumbnail": thumb, "category": ""
            })
        return results


async def yt_playlist(playlist_id: str, max_results: int = 50) -> list:
    if not YOUTUBE_API_KEY: return []
    async with httpx.AsyncClient(timeout=15) as client:
        r1 = await client.get("https://www.googleapis.com/youtube/v3/playlistItems", params={
            "part": "snippet", "playlistId": playlist_id,
            "maxResults": max_results, "key": YOUTUBE_API_KEY
        })
        if r1.status_code != 200:
            print(f"⚠️ YT playlist error {r1.status_code}: {r1.text[:200]}")
            return []
        items = r1.json().get("items", [])
        if not items: return []

        vids = [it["snippet"]["resourceId"]["videoId"] for it in items]
        if not vids: return []

        r2 = await client.get("https://www.googleapis.com/youtube/v3/videos", params={
            "part": "contentDetails,statistics", "id": ",".join(vids), "key": YOUTUBE_API_KEY
        })
        dm = {}
        if r2.status_code == 200:
            for v in r2.json().get("items", []): dm[v["id"]] = v

        try:
            db.register_quota_usage(search_calls=0, detail_calls=1)
        except Exception:
            pass

        results = []
        for it in items:
            vid = it["snippet"]["resourceId"]["videoId"]
            sn = it.get("snippet", {})
            title = sn.get("title", "")
            pub = sn.get("publishedAt", "")[:10]
            yr = int(pub[:4]) if pub else 0
            thumb = sn.get("thumbnails", {}).get("high", {}).get("url", "")
            det = dm.get(vid, {})
            dur = parse_iso_dur(det.get("contentDetails", {}).get("duration", ""))
            defn = det.get("contentDetails", {}).get("definition", "sd")
            views = int(det.get("statistics", {}).get("viewCount", 0))
            artist, song = extract_artist_song(title)
            results.append({
                "video_id": vid, "url": f"https://www.youtube.com/watch?v={vid}",
                "title": title, "artist": artist, "song": song or title,
                "channel": sn.get("channelTitle", ""), "year": yr, "published": pub,
                "duration": dur, "views": views, "hd": defn in ("hd", "4k"),
                "thumbnail": thumb, "category": "Playlist"
            })
        return results


# ─── APP ───
@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    load_posted()
    print(f"{'✅' if YOUTUBE_API_KEY else '⚠️'} YouTube API {'configured' if YOUTUBE_API_KEY else 'NOT SET'}")
    if db.is_cache_empty():
        print("🔄 Cache empty — auto-populating with V7 seeds...")
        asyncio.create_task(populate_initial_cache())
    yield

app = FastAPI(title="Best of Opera — Motor V7", version="7.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── PROCESS V7 ───
def _process_v7(videos, query, hide_posted, category=None):
    scored = []
    for v in videos:
        if category: v["category"] = category
        sc = calc_score_v7(v, category)
        p = is_posted(v.get("artist", ""), v.get("song", ""))
        scored.append({**v, "score": sc, "posted": p})
    scored.sort(key=lambda x: x["score"]["total"], reverse=True)
    pc = sum(1 for v in scored if v["posted"])
    vis = [v for v in scored if not v["posted"]] if hide_posted else scored
    return {"query": query, "category": category, "total_found": len(scored),
            "posted_hidden": pc if hide_posted else 0, "videos": vis}


def _rescore_cached(videos, category=None):
    """Recompute V7 scores for cached videos (adds reasons)"""
    for v in videos:
        v["score"] = calc_score_v7(v, category)
    videos.sort(key=lambda x: x["score"]["total"], reverse=True)
    return videos


async def populate_initial_cache():
    """Background: populate cache using seed 0 for each V7 category"""
    print("🚀 Starting V7 initial cache population...")
    for cat_key, cat_data in CATEGORIES_V7.items():
        try:
            seed_query = cat_data["seeds"][0]
            full_query = f"{seed_query} {ANTI_SPAM}"
            raw = await yt_search(full_query, 25)
            result = _process_v7(raw, seed_query, False, cat_key)
            db.save_cached_videos(result["videos"], cat_key)
            db.save_last_seed(cat_key, 0)
            print(f"✅ Cached {len(result['videos'])} videos for {cat_key}")
        except Exception as e:
            print(f"❌ Error caching {cat_key}: {e}")
    db.set_config("last_category_refresh", datetime.now().isoformat())
    print("🎉 V7 cache population complete!")


async def refresh_playlist():
    print("🔄 Refreshing playlist...")
    raw = await yt_playlist(PLAYLIST_ID, 50)
    processed = _process_v7(raw, "Playlist", False, "Playlist")
    db.save_playlist_videos(processed["videos"])
    db.set_config("last_playlist_refresh", datetime.now().isoformat())
    print(f"✅ Playlist refreshed: {len(processed['videos'])} videos")


# ─── ENDPOINTS ───

@app.post("/api/auth")
async def auth(password: str = Query(...)):
    if password == APP_PASSWORD:
        return {"ok": True}
    raise HTTPException(401, "Senha incorreta")

@app.get("/api/health")
async def health():
    quota = db.get_quota_status()
    return {
        "status": "ok", "version": "V7",
        "youtube_api": bool(YOUTUBE_API_KEY),
        "posted_count": len(posted_registry),
        "quota_remaining": quota["remaining"],
    }

@app.get("/api/search")
async def search(q: str = Query(...), max_results: int = Query(10, ge=1, le=50), hide_posted: bool = Query(True)):
    """Manual search with anti-spam filtering"""
    full_query = f"{q} opera live {ANTI_SPAM}"
    raw = await yt_search(full_query, max_results)
    return _process_v7(raw, q, hide_posted)

@app.get("/api/category/{category}")
async def search_category(category: str, hide_posted: bool = Query(True), force_refresh: bool = Query(False)):
    """Category search with V7 seed rotation"""
    cat_data = CATEGORIES_V7.get(category)
    if not cat_data:
        raise HTTPException(404, f"Categoria nao encontrada: {category}")

    last_seed = db.get_last_seed(category)
    total_seeds = len(cat_data["seeds"])

    # Serve from cache unless force_refresh
    if not force_refresh:
        cached = db.get_cached_videos(category, hide_posted)
        if cached:
            cached = _rescore_cached(cached, category)
            print(f"✅ Serving {len(cached)} cached videos for {category}")
            return {
                "query": category, "category": category,
                "total_found": len(cached), "posted_hidden": 0,
                "videos": cached, "cached": True,
                "seed_index": last_seed, "total_seeds": total_seeds,
                "seed_query": cat_data["seeds"][last_seed % total_seeds],
            }

    # Rotate to next seed
    next_seed = (last_seed + 1) % total_seeds
    seed_query = cat_data["seeds"][next_seed]
    full_query = f"{seed_query} {ANTI_SPAM}"

    print(f"🔍 V7 category '{category}' seed {next_seed}/{total_seeds}: {seed_query[:50]}...")
    raw = await yt_search(full_query, 25)
    db.save_last_seed(category, next_seed)

    result = _process_v7(raw, seed_query, hide_posted, category)
    db.save_cached_videos(result["videos"], category)
    result["cached"] = False
    result["seed_index"] = next_seed
    result["total_seeds"] = total_seeds
    result["seed_query"] = seed_query
    return result

@app.get("/api/ranking")
async def ranking(hide_posted: bool = Query(True)):
    """Ranking across all V7 categories using first seed each"""
    all_q = [(key, data["seeds"][0]) for key, data in CATEGORIES_V7.items()]
    tasks = [yt_search(f"{q} {ANTI_SPAM}", 10) for _, q in all_q]
    batches = await asyncio.gather(*tasks, return_exceptions=True)
    seen = set(); merged = []
    for i, batch in enumerate(batches):
        if isinstance(batch, Exception): continue
        cat = all_q[i][0]
        for v in batch:
            if v["video_id"] not in seen:
                seen.add(v["video_id"]); v["category"] = cat; merged.append(v)
    return _process_v7(merged, "ranking", hide_posted)

@app.get("/api/categories")
async def list_categories():
    """List V7 categories with seed info"""
    cats = []
    for key, data in CATEGORIES_V7.items():
        last_seed = db.get_last_seed(key)
        cats.append({
            "key": key, "name": data["name"], "emoji": data["emoji"],
            "desc": data["desc"], "total_seeds": len(data["seeds"]),
            "last_seed": last_seed,
            "seed_query": data["seeds"][last_seed % len(data["seeds"])],
        })
    return {"categories": cats}

@app.get("/api/posted")
async def get_posted():
    return {"count": len(posted_registry)}

@app.get("/api/posted/check")
async def check_posted(artist: str = "", song: str = ""):
    return {"posted": is_posted(artist, song)}


# ─── CACHE ENDPOINTS ───
@app.get("/api/cache/status")
async def cache_status():
    return db.get_cache_status()

@app.post("/api/cache/populate-initial")
async def populate_cache(background_tasks: BackgroundTasks):
    background_tasks.add_task(populate_initial_cache)
    return {"status": "started", "message": "V7 cache population started"}

@app.post("/api/cache/refresh-categories")
async def refresh_categories(background_tasks: BackgroundTasks):
    background_tasks.add_task(populate_initial_cache)
    return {"status": "started", "message": "V7 category refresh started"}


# ─── PLAYLIST ENDPOINTS ───
@app.get("/api/playlist/videos")
async def get_playlist(hide_posted: bool = Query(True)):
    videos = db.get_playlist_videos(hide_posted)
    if not videos:
        await refresh_playlist()
        videos = db.get_playlist_videos(hide_posted)
    return {"total_found": len(videos), "videos": videos, "playlist_id": PLAYLIST_ID, "cached": True}

@app.post("/api/playlist/refresh")
async def refresh_playlist_endpoint(background_tasks: BackgroundTasks):
    background_tasks.add_task(refresh_playlist)
    return {"status": "started", "message": "Playlist refresh started"}


# ─── QUOTA ENDPOINTS (V7) ───
@app.get("/api/quota/status")
async def quota_status():
    return db.get_quota_status()

@app.post("/api/quota/register")
async def quota_register(search_calls: int = Query(0), detail_calls: int = Query(0)):
    db.register_quota_usage(search_calls, detail_calls)
    return db.get_quota_status()


# ─── DOWNLOAD ENDPOINTS ───
@app.get("/api/download/{video_id}")
async def download_video(video_id: str, artist: str = Query("Unknown"), song: str = Query("Video")):
    safe_artist = sanitize_filename(artist)
    safe_song = sanitize_filename(song)
    filename = f"{safe_artist} - {safe_song}.mp4"
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"

    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, filename)

    async with download_semaphore:
        try:
            import yt_dlp
            ydl_opts = {
                'format': 'best[ext=mp4]/best',
                'outtmpl': tmp_path,
                'noplaylist': True,
                'quiet': True,
                'no_warnings': True,
                'match_filter': yt_dlp.utils.match_filter_func('duration < 900'),
                'socket_timeout': 30,
            }
            def _download():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([youtube_url])
            await asyncio.to_thread(_download)

            if not os.path.exists(tmp_path):
                import glob
                files = glob.glob(os.path.join(tmp_dir, '*'))
                if files:
                    tmp_path_actual = files[0]
                else:
                    raise HTTPException(500, "Download failed: output file not found")
            else:
                tmp_path_actual = tmp_path

            try:
                db.save_download(video_id, filename, artist, song, youtube_url)
            except Exception as e:
                print(f"⚠️ Failed to save download record: {e}")

            def iter_file():
                try:
                    with open(tmp_path_actual, 'rb') as f:
                        while chunk := f.read(1024 * 1024):
                            yield chunk
                finally:
                    import shutil
                    shutil.rmtree(tmp_dir, ignore_errors=True)

            return StreamingResponse(
                iter_file(), media_type="video/mp4",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )
        except HTTPException:
            raise
        except Exception as e:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
            print(f"❌ Download error for {video_id}: {e}")
            raise HTTPException(500, f"Download failed: {str(e)}")

@app.get("/api/downloads")
async def list_downloads():
    return {"downloads": db.get_downloads()}

@app.get("/api/downloads/export")
async def export_downloads():
    csv_content = db.export_downloads_csv()
    return Response(content=csv_content, media_type="text/csv",
                    headers={"Content-Disposition": 'attachment; filename="downloads.csv"'})


# ─── SERVE FRONTEND ───
possible_paths = [STATIC_PATH / "index.html", Path("./index.html"), Path("./static/index.html")]
static_index = next((p for p in possible_paths if p.exists()), None)
if static_index:
    static_dir = static_index.parent
    @app.get("/")
    async def index(): return FileResponse(static_index)
    app.mount("/", StaticFiles(directory=str(static_dir)), name="static")
