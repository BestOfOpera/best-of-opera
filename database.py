# ══════════════════════════════════════════════════════════════
# DATABASE MODULE - PostgreSQL Cache & Playlist Management
# Uses psycopg 3 (modern driver with bundled libpq binary)
# Railway provides DATABASE_URL automatically when PostgreSQL
# is added as a service.
# ══════════════════════════════════════════════════════════════

import os
from datetime import datetime
from typing import List, Dict, Optional

import psycopg
from psycopg.rows import dict_row

# Railway may use different variable names for PostgreSQL
# Try multiple common names
DATABASE_URL = ""
for var_name in ["DATABASE_URL", "DATABASE_PUBLIC_URL", "DATABASE_PRIVATE_URL", "POSTGRES_URL"]:
    val = os.getenv(var_name, "")
    if val:
        DATABASE_URL = val
        print(f"🔗 Found PostgreSQL URL in {var_name}")
        break

# Debug: show what env vars exist
all_var_names = sorted(os.environ.keys())
print(f"🔍 ALL env vars ({len(all_var_names)}): {all_var_names}")
print(f"🔗 DATABASE_URL configured: {bool(DATABASE_URL)}")


def _conn():
    """Create a new database connection"""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set — add PostgreSQL on Railway")
    return psycopg.connect(DATABASE_URL)


def init_db():
    """Initialize database with tables"""
    try:
        conn = _conn()
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        raise
    c = conn.cursor()

    # Table: cached_videos (category searches)
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

    # Table: playlist_videos (pre-approved content)
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

    # Table: system_config (timestamps & quotas)
    c.execute("""
        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Database initialized (PostgreSQL)")


def save_cached_videos(videos: List[Dict], category: str):
    """Save videos to cache (removes old entries for this category first)"""
    if not videos:
        print(f"⚠️ Skipping cache save for {category}: no videos to save")
        return
    conn = _conn()
    c = conn.cursor()

    # Clear old cache for this category
    c.execute("DELETE FROM cached_videos WHERE category = %s", (category,))

    # Insert new videos
    for v in videos:
        score = v.get("score", {})
        c.execute("""
            INSERT INTO cached_videos
            (video_id, url, title, artist, song, channel, year, published, duration,
             views, hd, thumbnail, category, score_total, score_fixed, score_guia,
             artist_match, song_match, posted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            score["total"], score["fixed"], score["guia"],
            score.get("artist_match"), score.get("song_match"), v["posted"]
        ))

    conn.commit()
    conn.close()
    print(f"💾 Cached {len(videos)} videos for category: {category}")


def get_cached_videos(category: str, hide_posted: bool = True) -> List[Dict]:
    """Retrieve cached videos for a category"""
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

    videos = []
    for row in rows:
        videos.append({
            "video_id": row["video_id"],
            "url": row["url"],
            "title": row["title"],
            "artist": row["artist"],
            "song": row["song"],
            "channel": row["channel"],
            "year": row["year"],
            "published": row["published"],
            "duration": row["duration"],
            "views": row["views"],
            "hd": bool(row["hd"]),
            "thumbnail": row["thumbnail"],
            "category": row["category"],
            "score": {
                "total": row["score_total"],
                "fixed": row["score_fixed"],
                "guia": row["score_guia"],
                "artist_match": row["artist_match"],
                "song_match": row["song_match"]
            },
            "posted": bool(row["posted"])
        })

    return videos


def save_playlist_videos(videos: List[Dict]):
    """Save playlist videos (clears old entries first)"""
    if not videos:
        print("⚠️ Skipping playlist save: no videos to save")
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            score["total"], score["fixed"], score["guia"],
            score.get("artist_match"), score.get("song_match"), v["posted"], idx
        ))

    conn.commit()
    conn.close()
    print(f"💾 Cached {len(videos)} playlist videos")


def get_playlist_videos(hide_posted: bool = True) -> List[Dict]:
    """Retrieve cached playlist videos"""
    conn = _conn()
    c = conn.cursor(row_factory=dict_row)

    query = "SELECT * FROM playlist_videos"
    if hide_posted:
        query += " WHERE posted = FALSE"
    query += " ORDER BY score_total DESC"

    c.execute(query)
    rows = c.fetchall()
    conn.close()

    videos = []
    for row in rows:
        videos.append({
            "video_id": row["video_id"],
            "url": row["url"],
            "title": row["title"],
            "artist": row["artist"],
            "song": row["song"],
            "channel": row["channel"],
            "year": row["year"],
            "published": row["published"],
            "duration": row["duration"],
            "views": row["views"],
            "hd": bool(row["hd"]),
            "thumbnail": row["thumbnail"],
            "score": {
                "total": row["score_total"],
                "fixed": row["score_fixed"],
                "guia": row["score_guia"],
                "artist_match": row["artist_match"],
                "song_match": row["song_match"]
            },
            "posted": bool(row["posted"]),
            "position": row["position"]
        })

    return videos


def set_config(key: str, value: str):
    """Set config value"""
    conn = _conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO system_config (key, value, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
    """, (key, value))
    conn.commit()
    conn.close()


def get_config(key: str) -> Optional[str]:
    """Get config value"""
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT value FROM system_config WHERE key = %s", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def get_cache_status() -> Dict:
    """Get cache statistics"""
    conn = _conn()
    c = conn.cursor()

    # Count cached videos per category
    c.execute("""
        SELECT category, COUNT(*) as count, MAX(fetched_at) as last_update
        FROM cached_videos
        GROUP BY category
    """)
    categories = {}
    for row in c.fetchall():
        categories[row[0]] = {
            "count": row[1],
            "last_update": row[2].isoformat() if row[2] else None
        }

    # Count playlist videos
    c.execute("SELECT COUNT(*) FROM playlist_videos")
    playlist_count = c.fetchone()[0]

    c.execute("SELECT MAX(fetched_at) FROM playlist_videos")
    prow = c.fetchone()
    playlist_update = prow[0].isoformat() if prow and prow[0] else None

    conn.close()

    # Get last config updates
    last_category_refresh = get_config("last_category_refresh")
    last_playlist_refresh = get_config("last_playlist_refresh")

    return {
        "categories": categories,
        "playlist": {
            "count": playlist_count,
            "last_update": playlist_update
        },
        "last_category_refresh": last_category_refresh,
        "last_playlist_refresh": last_playlist_refresh,
        "cache_initialized": len(categories) > 0 or playlist_count > 0
    }


def is_cache_empty() -> bool:
    """Check if cache has been populated"""
    conn = _conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM cached_videos")
    count = c.fetchone()[0]
    conn.close()
    return count == 0
