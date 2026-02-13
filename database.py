# ══════════════════════════════════════════════════════════════
# DATABASE MODULE V7 — PostgreSQL Cache, Seeds & Quota
# Uses psycopg 3 (modern driver with bundled libpq binary)
# ══════════════════════════════════════════════════════════════

import os, io, csv, json
from datetime import datetime, date
from typing import List, Dict, Optional

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    DATABASE_URL = "postgresql://postgres:PWlhCmhfTQOFywLdRKexzGfKKxEfXGgs@postgres.railway.internal:5432/railway"
    print("⚠️ DATABASE_URL env var not found — using Railway internal fallback")
else:
    print(f"🔗 DATABASE_URL loaded from env var")


def _conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set — add PostgreSQL on Railway")
    return psycopg.connect(DATABASE_URL)


def init_db():
    try:
        conn = _conn()
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        raise
    c = conn.cursor()

    # Table: cached_videos
    c.execute("""
        CREATE TABLE IF NOT EXISTS cached_videos (
            id SERIAL PRIMARY KEY,
            video_id TEXT NOT NULL,
            url TEXT,
            title TEXT,
            artist TEXT,
            song TEXT,
            channel TEXT,
            year INTEGER,
            published TEXT,
            duration INTEGER,
            views INTEGER,
            hd BOOLEAN,
            thumbnail TEXT,
            category TEXT,
            score_total INTEGER,
            score_fixed INTEGER,
            score_guia REAL,
            artist_match TEXT,
            song_match TEXT,
            posted BOOLEAN,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(video_id, category)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_category ON cached_videos(category)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_score ON cached_videos(score_total DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_video_id ON cached_videos(video_id)")

    # Table: playlist_videos
    c.execute("""
        CREATE TABLE IF NOT EXISTS playlist_videos (
            id SERIAL PRIMARY KEY,
            video_id TEXT UNIQUE NOT NULL,
            url TEXT,
            title TEXT,
            artist TEXT,
            song TEXT,
            channel TEXT,
            year INTEGER,
            published TEXT,
            duration INTEGER,
            views INTEGER,
            hd BOOLEAN,
            thumbnail TEXT,
            score_total INTEGER,
            score_fixed INTEGER,
            score_guia REAL,
            artist_match TEXT,
            song_match TEXT,
            posted BOOLEAN,
            position INTEGER,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_playlist_score ON playlist_videos(score_total DESC)")

    # Table: system_config
    c.execute("""
        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Table: downloads
    c.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            id SERIAL PRIMARY KEY,
            video_id TEXT NOT NULL,
            filename TEXT,
            artist TEXT,
            song TEXT,
            youtube_url TEXT,
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Table: category_seeds (V7 seed rotation)
    c.execute("""
        CREATE TABLE IF NOT EXISTS category_seeds (
            category_id TEXT PRIMARY KEY,
            last_seed INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Table: quota_usage (V7 daily quota tracking)
    c.execute("""
        CREATE TABLE IF NOT EXISTS quota_usage (
            usage_date DATE PRIMARY KEY,
            search_calls INTEGER DEFAULT 0,
            detail_calls INTEGER DEFAULT 0,
            total_points INTEGER DEFAULT 0
        )
    """)

    # Table: production_projects (APP2 — Content Production)
    c.execute("""
        CREATE TABLE IF NOT EXISTS production_projects (
            id SERIAL PRIMARY KEY,
            artist TEXT NOT NULL,
            song TEXT NOT NULL,
            hook TEXT,
            cut_start REAL DEFAULT 0,
            cut_end REAL,
            video_filename TEXT,
            video_path TEXT,
            duration REAL,
            status TEXT DEFAULT 'uploaded',
            transcription TEXT,
            transcription_segments TEXT,
            overlay_subtitles TEXT,
            post_text TEXT,
            youtube_seo TEXT,
            overlay_approved BOOLEAN DEFAULT FALSE,
            post_approved BOOLEAN DEFAULT FALSE,
            translations TEXT,
            output_path TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT,
            official_lyrics TEXT,
            language TEXT DEFAULT 'en'
        )
    """)
    # Add columns for existing databases
    for col in ["official_lyrics", "language"]:
        try:
            c.execute(f"ALTER TABLE production_projects ADD COLUMN IF NOT EXISTS {col} TEXT")
        except Exception:
            pass

    conn.commit()
    conn.close()
    print("✅ Database initialized (PostgreSQL V7)")


# ─── CACHED VIDEOS ───

def save_cached_videos(videos: List[Dict], category: str):
    if not videos:
        print(f"⚠️ Skipping cache save for {category}: no videos")
        return
    conn = _conn()
    c = conn.cursor()
    c.execute("DELETE FROM cached_videos WHERE category = %s", (category,))
    for v in videos:
        score = v.get("score", {})
        c.execute("""
            INSERT INTO cached_videos
            (video_id, url, title, artist, song, channel, year, published, duration,
             views, hd, thumbnail, category, score_total, score_fixed, score_guia,
             artist_match, song_match, posted)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (video_id, category) DO UPDATE SET
                url=EXCLUDED.url, title=EXCLUDED.title, artist=EXCLUDED.artist,
                song=EXCLUDED.song, channel=EXCLUDED.channel, year=EXCLUDED.year,
                published=EXCLUDED.published, duration=EXCLUDED.duration, views=EXCLUDED.views,
                hd=EXCLUDED.hd, thumbnail=EXCLUDED.thumbnail, score_total=EXCLUDED.score_total,
                score_fixed=EXCLUDED.score_fixed, score_guia=EXCLUDED.score_guia,
                artist_match=EXCLUDED.artist_match, song_match=EXCLUDED.song_match,
                posted=EXCLUDED.posted, fetched_at=CURRENT_TIMESTAMP
        """, (
            v["video_id"], v["url"], v["title"], v["artist"], v["song"],
            v["channel"], v["year"], v["published"], v["duration"],
            v["views"], v["hd"], v["thumbnail"], category,
            score.get("total", 0), score.get("fixed", 0), score.get("guia", 0.0),
            score.get("artist_match"), score.get("song_match"), v.get("posted", False)
        ))
    conn.commit()
    conn.close()
    print(f"💾 Cached {len(videos)} videos for: {category}")


def get_cached_videos(category: str, hide_posted: bool = True) -> List[Dict]:
    conn = _conn()
    c = conn.cursor(row_factory=dict_row)
    query = "SELECT * FROM cached_videos WHERE category = %s"
    params: list = [category]
    if hide_posted:
        query += " AND posted = FALSE"
    query += " ORDER BY score_total DESC"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return [
        {
            "video_id": r["video_id"], "url": r["url"], "title": r["title"],
            "artist": r["artist"], "song": r["song"], "channel": r["channel"],
            "year": r["year"], "published": r["published"], "duration": r["duration"],
            "views": r["views"], "hd": bool(r["hd"]), "thumbnail": r["thumbnail"],
            "category": r["category"],
            "score": {
                "total": r["score_total"], "fixed": r.get("score_fixed", 0),
                "guia": r.get("score_guia", 0),
                "artist_match": r["artist_match"], "song_match": r["song_match"]
            },
            "posted": bool(r["posted"])
        }
        for r in rows
    ]


# ─── PLAYLIST ───

def save_playlist_videos(videos: List[Dict]):
    if not videos:
        print("⚠️ Skipping playlist save: no videos")
        return
    conn = _conn()
    c = conn.cursor()
    c.execute("DELETE FROM playlist_videos")
    for idx, v in enumerate(videos):
        score = v.get("score", {})
        c.execute("""
            INSERT INTO playlist_videos
            (video_id, url, title, artist, song, channel, year, published, duration,
             views, hd, thumbnail, score_total, score_fixed, score_guia,
             artist_match, song_match, posted, position)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (video_id) DO UPDATE SET
                url=EXCLUDED.url, title=EXCLUDED.title, artist=EXCLUDED.artist,
                song=EXCLUDED.song, channel=EXCLUDED.channel, year=EXCLUDED.year,
                published=EXCLUDED.published, duration=EXCLUDED.duration, views=EXCLUDED.views,
                hd=EXCLUDED.hd, thumbnail=EXCLUDED.thumbnail, score_total=EXCLUDED.score_total,
                score_fixed=EXCLUDED.score_fixed, score_guia=EXCLUDED.score_guia,
                artist_match=EXCLUDED.artist_match, song_match=EXCLUDED.song_match,
                posted=EXCLUDED.posted, position=EXCLUDED.position, fetched_at=CURRENT_TIMESTAMP
        """, (
            v["video_id"], v["url"], v["title"], v["artist"], v["song"],
            v["channel"], v["year"], v["published"], v["duration"],
            v["views"], v["hd"], v["thumbnail"],
            score.get("total", 0), score.get("fixed", 0), score.get("guia", 0.0),
            score.get("artist_match"), score.get("song_match"), v.get("posted", False), idx
        ))
    conn.commit()
    conn.close()
    print(f"💾 Cached {len(videos)} playlist videos")


def get_playlist_videos(hide_posted: bool = True) -> List[Dict]:
    conn = _conn()
    c = conn.cursor(row_factory=dict_row)
    query = "SELECT * FROM playlist_videos"
    if hide_posted:
        query += " WHERE posted = FALSE"
    query += " ORDER BY score_total DESC"
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    return [
        {
            "video_id": r["video_id"], "url": r["url"], "title": r["title"],
            "artist": r["artist"], "song": r["song"], "channel": r["channel"],
            "year": r["year"], "published": r["published"], "duration": r["duration"],
            "views": r["views"], "hd": bool(r["hd"]), "thumbnail": r["thumbnail"],
            "score": {
                "total": r["score_total"], "fixed": r.get("score_fixed", 0),
                "guia": r.get("score_guia", 0),
                "artist_match": r["artist_match"], "song_match": r["song_match"]
            },
            "posted": bool(r["posted"]), "position": r["position"]
        }
        for r in rows
    ]


# ─── CONFIG ───

def set_config(key: str, value: str):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO system_config (key, value, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
    """, (key, value))
    conn.commit()
    conn.close()


def get_config(key: str) -> Optional[str]:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT value FROM system_config WHERE key = %s", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


# ─── CACHE STATUS ───

def get_cache_status() -> Dict:
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        SELECT category, COUNT(*) as count, MAX(fetched_at) as last_update
        FROM cached_videos GROUP BY category
    """)
    categories = {}
    for row in c.fetchall():
        categories[row[0]] = {"count": row[1], "last_update": row[2].isoformat() if row[2] else None}
    c.execute("SELECT COUNT(*) FROM playlist_videos")
    playlist_count = c.fetchone()[0]
    c.execute("SELECT MAX(fetched_at) FROM playlist_videos")
    prow = c.fetchone()
    playlist_update = prow[0].isoformat() if prow and prow[0] else None
    conn.close()
    return {
        "categories": categories,
        "playlist": {"count": playlist_count, "last_update": playlist_update},
        "last_category_refresh": get_config("last_category_refresh"),
        "last_playlist_refresh": get_config("last_playlist_refresh"),
        "cache_initialized": len(categories) > 0 or playlist_count > 0
    }


def is_cache_empty() -> bool:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM cached_videos")
    count = c.fetchone()[0]
    conn.close()
    return count == 0


# ─── DOWNLOADS ───

def save_download(video_id: str, filename: str, artist: str, song: str, youtube_url: str):
    conn = _conn()
    c = conn.cursor()
    c.execute("INSERT INTO downloads (video_id, filename, artist, song, youtube_url) VALUES (%s,%s,%s,%s,%s)",
              (video_id, filename, artist, song, youtube_url))
    conn.commit()
    conn.close()


def get_downloads() -> List[Dict]:
    conn = _conn()
    c = conn.cursor(row_factory=dict_row)
    c.execute("SELECT * FROM downloads ORDER BY downloaded_at DESC")
    rows = c.fetchall()
    conn.close()
    return [
        {"id": r["id"], "video_id": r["video_id"], "filename": r["filename"],
         "artist": r["artist"], "song": r["song"], "youtube_url": r["youtube_url"],
         "downloaded_at": r["downloaded_at"].isoformat() if r["downloaded_at"] else None}
        for r in rows
    ]


def export_downloads_csv() -> str:
    downloads = get_downloads()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "video_id", "filename", "artist", "song", "youtube_url", "downloaded_at"])
    for d in downloads:
        writer.writerow([d["id"], d["video_id"], d["filename"], d["artist"], d["song"], d["youtube_url"], d["downloaded_at"]])
    return output.getvalue()


# ─── V7: SEED ROTATION ───

def get_last_seed(category_id: str) -> int:
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT last_seed FROM category_seeds WHERE category_id = %s", (category_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0


def save_last_seed(category_id: str, seed_index: int):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO category_seeds (category_id, last_seed, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (category_id) DO UPDATE SET
            last_seed = EXCLUDED.last_seed, updated_at = CURRENT_TIMESTAMP
    """, (category_id, seed_index))
    conn.commit()
    conn.close()


# ─── V7: QUOTA TRACKING ───

def register_quota_usage(search_calls: int = 0, detail_calls: int = 0):
    today = date.today()
    points = search_calls * 100 + detail_calls * 1
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO quota_usage (usage_date, search_calls, detail_calls, total_points)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (usage_date) DO UPDATE SET
            search_calls = quota_usage.search_calls + EXCLUDED.search_calls,
            detail_calls = quota_usage.detail_calls + EXCLUDED.detail_calls,
            total_points = quota_usage.total_points + EXCLUDED.total_points
    """, (today, search_calls, detail_calls, points))
    conn.commit()
    conn.close()


def get_quota_status() -> Dict:
    today = date.today()
    conn = _conn()
    c = conn.cursor(row_factory=dict_row)
    c.execute("SELECT * FROM quota_usage WHERE usage_date = %s", (today,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "date": str(row["usage_date"]),
            "search_calls": row["search_calls"],
            "detail_calls": row["detail_calls"],
            "total_points": row["total_points"],
            "limit": 10000,
            "remaining": max(0, 10000 - row["total_points"]),
        }
    return {
        "date": str(today),
        "search_calls": 0, "detail_calls": 0, "total_points": 0,
        "limit": 10000, "remaining": 10000,
    }


# ─── PRODUCTION PROJECTS (APP2) ───

def _parse_json_field(val):
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return val


def _prod_row_to_dict(r: Dict) -> Dict:
    return {
        "id": r["id"],
        "artist": r["artist"],
        "song": r["song"],
        "hook": r.get("hook"),
        "cut_start": r.get("cut_start", 0),
        "cut_end": r.get("cut_end"),
        "video_filename": r.get("video_filename"),
        "video_path": r.get("video_path"),
        "duration": r.get("duration"),
        "status": r.get("status", "uploaded"),
        "transcription": r.get("transcription"),
        "transcription_segments": _parse_json_field(r.get("transcription_segments")),
        "overlay_subtitles": _parse_json_field(r.get("overlay_subtitles")),
        "post_text": r.get("post_text"),
        "youtube_seo": _parse_json_field(r.get("youtube_seo")),
        "overlay_approved": bool(r.get("overlay_approved")),
        "post_approved": bool(r.get("post_approved")),
        "translations": _parse_json_field(r.get("translations")),
        "output_path": r.get("output_path"),
        "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
        "updated_at": r["updated_at"].isoformat() if r.get("updated_at") else None,
        "error_message": r.get("error_message"),
        "official_lyrics": r.get("official_lyrics"),
        "language": r.get("language", "en"),
    }


def create_production_project(artist: str, song: str, hook: str = None,
                              cut_start: float = 0, cut_end: float = None,
                              video_filename: str = None, video_path: str = None,
                              duration: float = None, language: str = "en") -> int:
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO production_projects
        (artist, song, hook, cut_start, cut_end, video_filename, video_path, duration, language)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (artist, song, hook, cut_start, cut_end, video_filename, video_path, duration, language))
    pid = c.fetchone()[0]
    conn.commit()
    conn.close()
    return pid


def get_production_projects() -> List[Dict]:
    conn = _conn()
    c = conn.cursor(row_factory=dict_row)
    c.execute("""
        SELECT id, artist, song, status, video_filename, duration,
               overlay_approved, post_approved, created_at, updated_at, error_message
        FROM production_projects ORDER BY created_at DESC
    """)
    rows = c.fetchall()
    conn.close()
    return [{
        "id": r["id"], "artist": r["artist"], "song": r["song"],
        "status": r["status"], "video_filename": r.get("video_filename"),
        "duration": r.get("duration"),
        "overlay_approved": bool(r.get("overlay_approved")),
        "post_approved": bool(r.get("post_approved")),
        "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
        "updated_at": r["updated_at"].isoformat() if r.get("updated_at") else None,
        "error_message": r.get("error_message"),
    } for r in rows]


def get_production_project(project_id: int) -> Optional[Dict]:
    conn = _conn()
    c = conn.cursor(row_factory=dict_row)
    c.execute("SELECT * FROM production_projects WHERE id = %s", (project_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return _prod_row_to_dict(row)


def update_production_status(project_id: int, status: str, error_message: str = None):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        UPDATE production_projects
        SET status = %s, error_message = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (status, error_message, project_id))
    conn.commit()
    conn.close()


def update_production_transcription(project_id: int, transcription: str, segments: list = None):
    conn = _conn()
    c = conn.cursor()
    seg_json = json.dumps(segments) if segments else None
    c.execute("""
        UPDATE production_projects
        SET transcription = %s, transcription_segments = %s,
            status = 'transcribed', updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (transcription, seg_json, project_id))
    conn.commit()
    conn.close()


def update_production_content(project_id: int, overlay: list, post_text: str, seo: dict):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        UPDATE production_projects
        SET overlay_subtitles = %s, post_text = %s, youtube_seo = %s,
            status = 'generated', updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (json.dumps(overlay), post_text, json.dumps(seo), project_id))
    conn.commit()
    conn.close()


def update_production_overlay(project_id: int, overlay: list, approved: bool):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        UPDATE production_projects
        SET overlay_subtitles = %s, overlay_approved = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (json.dumps(overlay), approved, project_id))
    conn.commit()
    conn.close()


def update_production_post(project_id: int, post_text: str, approved: bool):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        UPDATE production_projects
        SET post_text = %s, post_approved = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (post_text, approved, project_id))
    conn.commit()
    conn.close()


def update_production_translations(project_id: int, translations: dict):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        UPDATE production_projects
        SET translations = %s, status = 'translated', updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (json.dumps(translations), project_id))
    conn.commit()
    conn.close()


def update_production_output(project_id: int, output_path: str):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        UPDATE production_projects
        SET output_path = %s, status = 'completed', updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (output_path, project_id))
    conn.commit()
    conn.close()


def update_production_official_lyrics(project_id: int, official_lyrics: str):
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        UPDATE production_projects
        SET official_lyrics = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (official_lyrics, project_id))
    conn.commit()
    conn.close()


def delete_production_project(project_id: int) -> bool:
    conn = _conn()
    c = conn.cursor()
    c.execute("DELETE FROM production_projects WHERE id = %s", (project_id,))
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    return deleted
