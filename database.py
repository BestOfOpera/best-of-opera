# ══════════════════════════════════════════════════════════════
# DATABASE MODULE V7 — PostgreSQL Cache, Seeds & Quota
# Uses psycopg 3 (modern driver with bundled libpq binary)
# ══════════════════════════════════════════════════════════════

import os, io, csv
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
