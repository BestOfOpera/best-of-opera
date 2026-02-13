# ══════════════════════════════════════════════════════════════
# BEST OF OPERA — MOTOR V7
# Seed Rotation · V7 Scoring · Anti-Spam · Quota Control
# ══════════════════════════════════════════════════════════════

import os, re, csv, json, unicodedata, asyncio, tempfile, subprocess, shutil, zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, File, UploadFile, Form, Body
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

# ─── PRODUCTION MODULE CONFIG ───
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GOOGLE_TRANSLATE_API_KEY = os.getenv("GOOGLE_TRANSLATE_API_KEY", "").strip()
PROJECTS_DIR = Path("/tmp/best-of-opera-projects")
PROJECTS_DIR.mkdir(parents=True, exist_ok=True)

# Find ffmpeg binary — use imageio-ffmpeg (ships static ffmpeg binary)
try:
    import imageio_ffmpeg
    FFMPEG_BIN = imageio_ffmpeg.get_ffmpeg_exe()
except ImportError:
    FFMPEG_BIN = shutil.which("ffmpeg") or "ffmpeg"

# ffprobe lives next to ffmpeg in imageio-ffmpeg
_ffmpeg_dir = os.path.dirname(FFMPEG_BIN)
_ffprobe_candidate = os.path.join(_ffmpeg_dir, "ffprobe")
FFPROBE_BIN = _ffprobe_candidate if os.path.isfile(_ffprobe_candidate) else (shutil.which("ffprobe") or "ffprobe")

print(f"🎬 FFmpeg: {FFMPEG_BIN}")
print(f"🎬 FFprobe: {FFPROBE_BIN}")

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

@app.get("/api/debug/ffmpeg")
async def debug_ffmpeg():
    import glob as _glob
    info = {"FFMPEG_BIN": FFMPEG_BIN, "FFPROBE_BIN": FFPROBE_BIN, "PATH": os.environ.get("PATH", "")}
    try:
        r = subprocess.run([FFMPEG_BIN, "-version"], capture_output=True, text=True, timeout=5)
        info["ffmpeg_version"] = r.stdout.split("\n")[0] if r.returncode == 0 else f"ERROR: {r.stderr[:200]}"
    except Exception as e:
        info["ffmpeg_error"] = str(e)
    # Search for ffmpeg in common locations
    info["nix_ffmpeg"] = _glob.glob("/nix/store/*/bin/ffmpeg")[:5]
    info["usr_ffmpeg"] = _glob.glob("/usr/bin/ffmpeg") + _glob.glob("/usr/local/bin/ffmpeg")
    try:
        r2 = subprocess.run(["which", "ffmpeg"], capture_output=True, text=True, timeout=5)
        info["which_ffmpeg"] = r2.stdout.strip()
    except Exception:
        info["which_ffmpeg"] = "not found"
    return info

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
    project_name = f"{safe_artist} - {safe_song}"
    filename = f"{project_name}.mp4"
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"

    # Save to shared project folder (App1 + App2 share the same folder)
    project_dir = PROJECTS_DIR / project_name
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "video").mkdir(exist_ok=True)
    dl_path = str(project_dir / "video" / filename)

    async with download_semaphore:
        try:
            import yt_dlp
            ydl_opts = {
                'format': 'best[ext=mp4]/best',
                'outtmpl': dl_path,
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

            if not os.path.exists(dl_path):
                import glob as _glob
                files = _glob.glob(str(project_dir / "video" / '*'))
                if files:
                    dl_path_actual = files[0]
                else:
                    raise HTTPException(500, "Download failed: output file not found")
            else:
                dl_path_actual = dl_path

            try:
                db.save_download(video_id, filename, artist, song, youtube_url)
            except Exception as e:
                print(f"⚠️ Failed to save download record: {e}")

            def iter_file():
                with open(dl_path_actual, 'rb') as f:
                    while chunk := f.read(1024 * 1024):
                        yield chunk

            return StreamingResponse(
                iter_file(), media_type="video/mp4",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )
        except HTTPException:
            raise
        except Exception as e:
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


# ══════════════════════════════════════════════════════════════
# APP2 — PRODUCTION MODULE ENDPOINTS
# ══════════════════════════════════════════════════════════════

PROD_LANGUAGES = ["en", "pt", "es", "fr", "de", "it", "pl"]
LANG_NAMES = {"en": "English", "pt": "Portugues", "es": "Espanol", "fr": "Francais", "de": "Deutsch", "it": "Italiano", "pl": "Polski"}


@app.get("/api/prod/projects")
async def prod_list_projects():
    return {"projects": db.get_production_projects()}


@app.post("/api/prod/projects")
async def prod_create_project(
    video: UploadFile = File(...),
    artist: str = Form(...),
    song: str = Form(...),
    hook: str = Form(""),
    cut_start: float = Form(0),
    cut_end: float = Form(0),
    language: str = Form("en"),
):
    safe_artist = re.sub(r'[<>:"/\\|?*]', '', artist).strip()
    safe_song = re.sub(r'[<>:"/\\|?*]', '', song).strip()
    project_name = f"{safe_artist} - {safe_song}"
    project_dir = PROJECTS_DIR / project_name
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "video").mkdir(exist_ok=True)

    video_path = project_dir / "video" / f"{project_name}.mp4"
    with open(video_path, "wb") as f:
        content = await video.read()
        f.write(content)

    # Get duration via ffmpeg (no ffprobe needed)
    duration = None
    try:
        result = subprocess.run(
            [FFMPEG_BIN, "-i", str(video_path), "-f", "null", "-"],
            capture_output=True, text=True, timeout=30
        )
        # Parse duration from stderr: "Duration: HH:MM:SS.ms"
        dur_match = re.search(r"Duration:\s*(\d+):(\d+):(\d+)\.(\d+)", result.stderr)
        if dur_match:
            h, m, s, ms = dur_match.groups()
            duration = int(h)*3600 + int(m)*60 + int(s) + int(ms)/100
    except Exception as e:
        print(f"⚠️ duration detection error: {e}")

    pid = db.create_production_project(
        artist=artist, song=song, hook=hook or None,
        cut_start=cut_start, cut_end=cut_end if cut_end > 0 else None,
        video_filename=f"{project_name}.mp4", video_path=str(video_path),
        duration=duration, language=language
    )
    return {"id": pid, "status": "uploaded"}


@app.get("/api/prod/projects/{project_id}")
async def prod_get_project(project_id: int):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    return proj


@app.delete("/api/prod/projects/{project_id}")
async def prod_delete_project(project_id: int):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    # Clean up project folder
    if proj.get("video_path"):
        video_file = Path(proj["video_path"])
        project_dir = video_file.parent.parent  # video is in project_dir/video/
        if project_dir.exists() and str(project_dir).startswith(str(PROJECTS_DIR)):
            shutil.rmtree(project_dir, ignore_errors=True)
    db.delete_production_project(project_id)
    return {"ok": True}


@app.get("/api/prod/projects/{project_id}/video")
async def prod_video(project_id: int):
    proj = db.get_production_project(project_id)
    if not proj or not proj.get("video_path"):
        raise HTTPException(404, "Video not found")
    video_path = Path(proj["video_path"])
    if not video_path.exists():
        raise HTTPException(404, "Video file not found on disk")
    return FileResponse(str(video_path), media_type="video/mp4")


@app.get("/api/prod/projects/{project_id}/status")
async def prod_get_status(project_id: int):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    return {
        "id": proj["id"], "status": proj["status"],
        "overlay_approved": proj["overlay_approved"],
        "post_approved": proj["post_approved"],
        "error_message": proj.get("error_message"),
    }


# ─── TRANSCRIPTION ───

async def _bg_transcribe(project_id: int):
    try:
        proj = db.get_production_project(project_id)
        if not proj or not proj.get("video_path"):
            db.update_production_status(project_id, "error", "Video file not found")
            return

        db.update_production_status(project_id, "transcribing")
        video_path = Path(proj["video_path"])
        audio_path = video_path.parent / "audio.wav"

        # Extract audio with FFmpeg (wav for max compatibility)
        cmd = [FFMPEG_BIN, "-y", "-i", str(video_path), "-vn",
               "-ar", "16000", "-ac", "1", "-f", "wav", str(audio_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            # Get the actual error (skip version header)
            err_lines = [l for l in result.stderr.split("\n") if l.strip() and not l.startswith("  ")]
            err_msg = "\n".join(err_lines[-5:])  # last 5 meaningful lines
            db.update_production_status(project_id, "error", f"FFmpeg audio extraction failed: {err_msg[:400]}")
            return

        if not OPENAI_API_KEY:
            db.update_production_status(project_id, "error", "OPENAI_API_KEY not configured")
            return

        # Send to Whisper API
        async with httpx.AsyncClient(timeout=300) as client:
            with open(audio_path, "rb") as af:
                files = {"file": ("audio.wav", af, "audio/wav")}
                whisper_lang = proj.get("language") or "en"
                data = {"model": "whisper-1", "response_format": "verbose_json", "language": whisper_lang}
                resp = await client.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    files=files, data=data
                )
            if resp.status_code != 200:
                db.update_production_status(project_id, "error", f"Whisper API error: {resp.text[:300]}")
                return

            whisper_data = resp.json()
            text = whisper_data.get("text", "")
            segments = whisper_data.get("segments", [])
            clean_segments = [{"start": s["start"], "end": s["end"], "text": s["text"]} for s in segments]

        db.update_production_transcription(project_id, text, clean_segments)
        # Clean up audio file
        audio_path.unlink(missing_ok=True)

    except Exception as e:
        print(f"❌ Transcription error for project {project_id}: {e}")
        db.update_production_status(project_id, "error", f"Transcription failed: {str(e)[:300]}")


@app.post("/api/prod/projects/{project_id}/transcribe")
async def prod_transcribe(project_id: int, background_tasks: BackgroundTasks):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    if proj["status"] not in ("uploaded", "error", "transcribed"):
        raise HTTPException(400, f"Cannot transcribe in status '{proj['status']}'")
    background_tasks.add_task(_bg_transcribe, project_id)
    return {"status": "transcribing"}


@app.put("/api/prod/projects/{project_id}/transcription")
async def prod_update_transcription(project_id: int, body: dict = Body(...)):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    transcription = body.get("transcription", "")
    segments = body.get("segments", proj.get("transcription_segments"))
    db.update_production_transcription(project_id, transcription, segments)
    return {"ok": True}


# ─── OFFICIAL LYRICS ───

@app.put("/api/prod/projects/{project_id}/official-lyrics")
async def prod_update_official_lyrics(project_id: int, body: dict = Body(...)):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    db.update_production_official_lyrics(project_id, body.get("official_lyrics", ""))
    return {"ok": True}


# ─── CONTENT GENERATION (Claude) ───

CLAUDE_PROMPT = """You are the content producer for "Best of Opera", the #1 Instagram page for opera and classical music (750K followers). Generate content for a video post.

ARTIST: {artist}
SONG: {song}
HOOK/CONTEXT: {hook}
FINAL VIDEO DURATION: {cut_duration}s (the delivered video is already cut, starts at 00:00)

TRANSCRIPTION:
{transcription}

Return a JSON object with exactly 3 keys:

1. "overlay" — Array of subtitle objects for the video overlay. Each has:
   - "start": start time in seconds (float)
   - "end": end time in seconds (float)
   - "text": subtitle text (MAX 70 characters, impactful, emotional)
   Rules:
   - 4-8 segments covering the ENTIRE video from 0.0s to {cut_duration}s
   - MUST start at 0.0 and end at or near {cut_duration}
   - NOT literal transcription — catchy, emotional, inspiring text
   - Use the hook/context to create powerful storytelling moments
   - Each segment 2-5 seconds long
   - English language

2. "post" — Instagram post text. MUST follow this EXACT structure. Each block is separated by a blank line (\\n\\n). Follow it FAITHFULLY:

   BLOCK 1 — ABERTURA (1 line):
   A playful icon + Artist/Performer name + Song/Aria name.
   Format: "[icon] [Artist] — [Song]"
   Example: "✨ Maria Callas — Casta Diva"

   BLOCK 2 — STORYTELLING (4-8 lines):
   Narrative text with a playful, curious, and storytelling tone. Write about the mystery, the magic, the secrets behind this performance or piece. Use icons like 🏰, 🕊️, ✨ naturally within the text. The tone should be engaging, like telling a fascinating story to a friend who loves music. Include interesting historical or cultural details woven into the narrative.

   BLOCK 3 — CREDITS (artist info, formatted exactly like this):
   [icon] [Singer full name] [country flag emoji of singer's nationality]
   [Voice type: Soprano/Tenor/Baritone/Mezzo-soprano/etc.]
   [Date of birth in format: DD/MM/YYYY]

   BLOCK 4 — CREDITS (song info, formatted exactly like this):
   [icon] [Song/Aria name] — [Album or Opera it belongs to]
   [Composer full name]
   [Composition date or year]

   BLOCK 5 — CTA SENSORIAL (1 line):
   A binary question designed to generate quick comments. Must be simple, sensory, emotional.
   Examples: "🔥 or ❄️?", "🌹 or 🥀?", "😭 or 😍?"

   BLOCK 6 — HASHTAGS (1 line):
   Exactly 4 relevant hashtags. No more, no less.
   Example: "#opera #mariacallas #castadiva #belcanto"

   Rules:
   - Each block separated by a blank line (\\n\\n)
   - The post MUST have exactly these 6 blocks in this order
   - Research and include accurate biographical data (birth date, nationality, voice type, composer, opera name)
   - Total: 1400-1900 characters

3. "seo" — YouTube SEO object with:
   - "title": YouTube title (50-70 chars, artist + song + emotional hook)
   - "description": YouTube description (300-500 chars, keyword-rich)
   - "tags": Array of 15-20 YouTube tags

Return ONLY valid JSON, no markdown wrapping, no explanation."""


async def _bg_generate(project_id: int):
    try:
        proj = db.get_production_project(project_id)
        if not proj:
            db.update_production_status(project_id, "error", "Project not found")
            return

        db.update_production_status(project_id, "generating")

        if not ANTHROPIC_API_KEY:
            db.update_production_status(project_id, "error", "ANTHROPIC_API_KEY not configured")
            return

        duration = proj.get("duration") or 60
        cut_start = proj.get("cut_start") or 0
        cut_end = proj.get("cut_end") or duration
        cut_duration = round(cut_end - cut_start, 1)

        prompt = CLAUDE_PROMPT.format(
            artist=proj["artist"], song=proj["song"],
            hook=proj.get("hook") or "Opera performance",
            cut_duration=cut_duration,
            transcription=proj.get("transcription") or "(no transcription available)"
        )

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 4096,
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            if resp.status_code != 200:
                db.update_production_status(project_id, "error", f"Claude API error: {resp.text[:300]}")
                return

            response_data = resp.json()
            text_content = response_data["content"][0]["text"]

            # Parse JSON from response (handle potential markdown wrapping)
            json_text = text_content.strip()
            if json_text.startswith("```"):
                json_text = re.sub(r'^```(?:json)?\s*', '', json_text)
                json_text = re.sub(r'\s*```$', '', json_text)

            content = json.loads(json_text)

        overlay = content.get("overlay", [])
        post_text = content.get("post", "")
        seo = content.get("seo", {})

        db.update_production_content(project_id, overlay, post_text, seo)

    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error for project {project_id}: {e}")
        db.update_production_status(project_id, "error", f"Failed to parse Claude response as JSON: {str(e)[:200]}")
    except Exception as e:
        print(f"❌ Generation error for project {project_id}: {e}")
        db.update_production_status(project_id, "error", f"Content generation failed: {str(e)[:300]}")


@app.post("/api/prod/projects/{project_id}/generate")
async def prod_generate(project_id: int, background_tasks: BackgroundTasks):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    if proj["status"] not in ("transcribed", "generated", "error"):
        raise HTTPException(400, f"Cannot generate in status '{proj['status']}'")
    background_tasks.add_task(_bg_generate, project_id)
    return {"status": "generating"}


# ─── REGENERATE (overlay only or post only) ───

async def _bg_regenerate(project_id: int, regen_type: str):
    """Regenerate only overlay or only post, keeping the other unchanged."""
    try:
        proj = db.get_production_project(project_id)
        if not proj:
            db.update_production_status(project_id, "error", "Project not found")
            return

        if not ANTHROPIC_API_KEY:
            db.update_production_status(project_id, "error", "ANTHROPIC_API_KEY not configured")
            return

        duration = proj.get("duration") or 60
        cut_start = proj.get("cut_start") or 0
        cut_end = proj.get("cut_end") or duration
        cut_duration = round(cut_end - cut_start, 1)

        prompt = CLAUDE_PROMPT.format(
            artist=proj["artist"], song=proj["song"],
            hook=proj.get("hook") or "Opera performance",
            cut_duration=cut_duration,
            transcription=proj.get("transcription") or "(no transcription available)"
        )

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 4096,
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            if resp.status_code != 200:
                db.update_production_status(project_id, "error", f"Claude API error: {resp.text[:300]}")
                return

            response_data = resp.json()
            text_content = response_data["content"][0]["text"]
            json_text = text_content.strip()
            if json_text.startswith("```"):
                json_text = re.sub(r'^```(?:json)?\s*', '', json_text)
                json_text = re.sub(r'\s*```$', '', json_text)
            content = json.loads(json_text)

        if regen_type == "overlay":
            overlay = content.get("overlay", [])
            db.update_production_overlay(project_id, overlay, False)
        elif regen_type == "post":
            post_text = content.get("post", "")
            seo = content.get("seo", {})
            db.update_production_post(project_id, post_text, False)
            # Also update SEO since it's related to post
            conn = db._conn()
            c = conn.cursor()
            c.execute("UPDATE production_projects SET youtube_seo = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                      (json.dumps(seo), project_id))
            conn.commit()
            conn.close()

        db.update_production_status(project_id, "generated")

    except json.JSONDecodeError as e:
        db.update_production_status(project_id, "error", f"Failed to parse Claude response: {str(e)[:200]}")
    except Exception as e:
        db.update_production_status(project_id, "error", f"Regeneration failed: {str(e)[:300]}")


@app.post("/api/prod/projects/{project_id}/regenerate-overlay")
async def prod_regenerate_overlay(project_id: int, background_tasks: BackgroundTasks):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    if not proj.get("overlay_subtitles"):
        raise HTTPException(400, "Generate content first")
    background_tasks.add_task(_bg_regenerate, project_id, "overlay")
    return {"status": "generating"}


@app.post("/api/prod/projects/{project_id}/regenerate-post")
async def prod_regenerate_post(project_id: int, background_tasks: BackgroundTasks):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    if not proj.get("post_text"):
        raise HTTPException(400, "Generate content first")
    background_tasks.add_task(_bg_regenerate, project_id, "post")
    return {"status": "generating"}


# ─── APPROVAL ───

@app.put("/api/prod/projects/{project_id}/overlay")
async def prod_update_overlay(project_id: int, body: dict = Body(...), background_tasks: BackgroundTasks = None):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    overlay = body.get("overlay", proj.get("overlay_subtitles") or [])
    approved = body.get("approved", False)
    db.update_production_overlay(project_id, overlay, approved)

    # Auto-trigger translation if both approved
    if approved:
        proj_updated = db.get_production_project(project_id)
        if proj_updated and proj_updated.get("post_approved"):
            background_tasks.add_task(_bg_translate, project_id)
            db.update_production_status(project_id, "translating")
    return {"ok": True, "overlay_approved": approved}


@app.put("/api/prod/projects/{project_id}/post")
async def prod_update_post(project_id: int, body: dict = Body(...), background_tasks: BackgroundTasks = None):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    post_text = body.get("post_text", proj.get("post_text") or "")
    approved = body.get("approved", False)
    db.update_production_post(project_id, post_text, approved)

    # Auto-trigger translation if both approved
    if approved:
        proj_updated = db.get_production_project(project_id)
        if proj_updated and proj_updated.get("overlay_approved"):
            background_tasks.add_task(_bg_translate, project_id)
            db.update_production_status(project_id, "translating")
    return {"ok": True, "post_approved": approved}


# ─── TRANSLATION (Google Translate API) ───

async def _google_translate(text: str, target_lang: str, client: httpx.AsyncClient) -> str:
    if not GOOGLE_TRANSLATE_API_KEY:
        return text
    resp = await client.post(
        f"https://translation.googleapis.com/language/translate/v2",
        params={"key": GOOGLE_TRANSLATE_API_KEY},
        json={"q": text, "target": target_lang, "source": "en", "format": "text"}
    )
    if resp.status_code == 200:
        data = resp.json()
        translations = data.get("data", {}).get("translations", [])
        if translations:
            return translations[0].get("translatedText", text)
    return text


async def _bg_translate(project_id: int):
    try:
        proj = db.get_production_project(project_id)
        if not proj:
            db.update_production_status(project_id, "error", "Project not found")
            return

        db.update_production_status(project_id, "translating")

        if not GOOGLE_TRANSLATE_API_KEY:
            db.update_production_status(project_id, "error", "GOOGLE_TRANSLATE_API_KEY not configured")
            return

        overlay = proj.get("overlay_subtitles") or []
        post_text = proj.get("post_text") or ""
        seo = proj.get("youtube_seo") or {}
        lyrics_segments = proj.get("transcription_segments") or []

        target_langs = [l for l in PROD_LANGUAGES if l != "en"]
        translations = {"en": {
            "overlay": overlay,
            "post": post_text,
            "seo": seo,
            "lyrics": lyrics_segments,
        }}

        async with httpx.AsyncClient(timeout=60) as client:
            for lang in target_langs:
                try:
                    # Translate overlay texts
                    translated_overlay = []
                    for sub in overlay:
                        translated_text = await _google_translate(sub.get("text", ""), lang, client)
                        translated_overlay.append({
                            "start": sub["start"], "end": sub["end"], "text": translated_text
                        })

                    # Translate lyrics/transcription segments
                    translated_lyrics = []
                    for seg in lyrics_segments:
                        translated_text = await _google_translate(seg.get("text", ""), lang, client)
                        translated_lyrics.append({
                            "start": seg["start"], "end": seg["end"], "text": translated_text
                        })

                    # Translate post
                    translated_post = await _google_translate(post_text, lang, client)

                    # Translate SEO
                    translated_seo = {}
                    if seo.get("title"):
                        translated_seo["title"] = await _google_translate(seo["title"], lang, client)
                    if seo.get("description"):
                        translated_seo["description"] = await _google_translate(seo["description"], lang, client)
                    translated_seo["tags"] = seo.get("tags", [])  # Keep tags in English

                    translations[lang] = {
                        "overlay": translated_overlay,
                        "post": translated_post,
                        "seo": translated_seo,
                        "lyrics": translated_lyrics,
                    }
                except Exception as e:
                    print(f"⚠️ Translation error for {lang}: {e}")
                    translations[lang] = {"overlay": overlay, "post": post_text, "seo": seo, "lyrics": lyrics_segments}

        db.update_production_translations(project_id, translations)

    except Exception as e:
        print(f"❌ Translation error for project {project_id}: {e}")
        db.update_production_status(project_id, "error", f"Translation failed: {str(e)[:300]}")


@app.post("/api/prod/projects/{project_id}/translate")
async def prod_translate(project_id: int, background_tasks: BackgroundTasks):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    background_tasks.add_task(_bg_translate, project_id)
    return {"status": "translating"}


# ─── PROCESSING (SRT + FFmpeg + ZIP) ───

def _generate_srt(subtitles: list) -> str:
    lines = []
    for i, sub in enumerate(subtitles, 1):
        start = sub["start"]
        end = sub["end"]
        text = sub["text"]
        s_h, s_m = int(start // 3600), int((start % 3600) // 60)
        s_s, s_ms = int(start % 60), int((start % 1) * 1000)
        e_h, e_m = int(end // 3600), int((end % 3600) // 60)
        e_s, e_ms = int(end % 60), int((end % 1) * 1000)
        lines.append(f"{i}")
        lines.append(f"{s_h:02d}:{s_m:02d}:{s_s:02d},{s_ms:03d} --> {e_h:02d}:{e_m:02d}:{e_s:02d},{e_ms:03d}")
        lines.append(text)
        lines.append("")
    return "\n".join(lines)


async def _bg_process(project_id: int):
    try:
        proj = db.get_production_project(project_id)
        if not proj:
            db.update_production_status(project_id, "error", "Project not found")
            return

        db.update_production_status(project_id, "processing")

        video_path = Path(proj["video_path"])
        if not video_path.exists():
            db.update_production_status(project_id, "error", "Video file not found on disk")
            return

        translations = proj.get("translations") or {}
        if not translations:
            db.update_production_status(project_id, "error", "No translations available. Run translate first.")
            return

        artist = proj["artist"]
        song = proj["song"]
        safe_name = re.sub(r'[<>:"/\\|?*]', '', f"{artist} - {song}")

        # Use the shared project folder (Artist - Song)
        project_dir = video_path.parent.parent  # video is in project_dir/video/
        (project_dir / "subtitles").mkdir(exist_ok=True)
        (project_dir / "posts").mkdir(exist_ok=True)
        (project_dir / "seo").mkdir(exist_ok=True)

        # Generate 3 types of SRT files for all languages
        lyrics_segments = proj.get("transcription_segments") or []

        for lang, data in translations.items():
            # 1. Overlay SRTs (catchy emotional text)
            overlay = data.get("overlay", [])
            srt_content = _generate_srt(overlay)
            srt_path = project_dir / "subtitles" / f"overlay_{lang}.srt"
            srt_path.write_text(srt_content, encoding="utf-8")

            # 2. Lyrics SRTs (translated lyrics from transcription)
            lyrics = data.get("lyrics", [])
            if lyrics:
                lyrics_srt = _generate_srt(lyrics)
                lyrics_path = project_dir / "subtitles" / f"lyrics_{lang}.srt"
                lyrics_path.write_text(lyrics_srt, encoding="utf-8")

            # Save post text
            post_path = project_dir / "posts" / f"post_{lang}.txt"
            post_path.write_text(data.get("post", ""), encoding="utf-8")

            # Save SEO JSON
            seo_path = project_dir / "seo" / f"seo_{lang}.json"
            seo_path.write_text(json.dumps(data.get("seo", {}), indent=2, ensure_ascii=False), encoding="utf-8")

        # 3. Original lyrics SRT (always in original language, from transcription)
        if lyrics_segments:
            original_lyrics_srt = _generate_srt(lyrics_segments)
            orig_path = project_dir / "subtitles" / "lyrics_original.srt"
            orig_path.write_text(original_lyrics_srt, encoding="utf-8")

        # Cut video if needed
        cut_start = proj.get("cut_start") or 0
        cut_end = proj.get("cut_end")
        cut_video_path = project_dir / "video" / f"{safe_name}.mp4"

        # Only process if source != destination
        if str(video_path) != str(cut_video_path) or cut_start > 0 or cut_end:
            ffmpeg_cmd = [FFMPEG_BIN, "-y", "-i", str(video_path)]
            if cut_start > 0:
                ffmpeg_cmd += ["-ss", str(cut_start)]
            if cut_end:
                ffmpeg_cmd += ["-to", str(cut_end)]
            ffmpeg_cmd += ["-c", "copy", str(cut_video_path)]
            subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=300)

        # Auto-save: files are already in the project folder, no ZIP needed
        db.update_production_output(project_id, str(project_dir))

    except Exception as e:
        print(f"❌ Processing error for project {project_id}: {e}")
        db.update_production_status(project_id, "error", f"Processing failed: {str(e)[:300]}")


@app.post("/api/prod/projects/{project_id}/process")
async def prod_process(project_id: int, background_tasks: BackgroundTasks):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    if proj["status"] not in ("translated", "completed", "error"):
        raise HTTPException(400, f"Cannot process in status '{proj['status']}'")
    background_tasks.add_task(_bg_process, project_id)
    return {"status": "processing"}


# ─── EXPORT (folder listing) ───

@app.get("/api/prod/projects/{project_id}/export")
async def prod_export(project_id: int):
    proj = db.get_production_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    if not proj.get("output_path"):
        raise HTTPException(400, "Project not yet processed")
    output_dir = Path(proj["output_path"])
    if not output_dir.exists():
        raise HTTPException(404, "Project folder not found on disk")
    # Return folder contents listing
    files = []
    for f in sorted(output_dir.rglob("*")):
        if f.is_file():
            files.append({
                "path": str(f.relative_to(output_dir)),
                "size": f.stat().st_size,
            })
    safe_name = re.sub(r'[<>:"/\\|?*]', '', f"{proj['artist']} - {proj['song']}")
    return {
        "project_name": safe_name,
        "folder": str(output_dir),
        "files": files,
        "total_files": len(files),
    }


# ─── SERVE FRONTEND ───
possible_paths = [STATIC_PATH / "index.html", Path("./index.html"), Path("./static/index.html")]
static_index = next((p for p in possible_paths if p.exists()), None)
if static_index:
    static_dir = static_index.parent
    @app.get("/")
    async def index(): return FileResponse(static_index)
    app.mount("/", StaticFiles(directory=str(static_dir)), name="static")
