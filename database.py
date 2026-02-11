# ══════════════════════════════════════════════════════════════
# DATABASE MODULE - SQLite Cache & Playlist Management
# ══════════════════════════════════════════════════════════════

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

DB_PATH = Path("./data/cache.db")

def init_db():
    """Initialize database with tables"""
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Table: cached_videos (category searches)
    c.execute("""
        CREATE TABLE IF NOT EXISTS cached_videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    print("✅ Database initialized")


def save_cached_videos(videos: List[Dict], category: str):
    """Save videos to cache (removes old entries for this category first)"""
    if not videos:
        print(f"⚠️ Skipping cache save for {category}: no videos to save")
        return
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Clear old cache for this category
    c.execute("DELETE FROM cached_videos WHERE category = ?", (category,))
    
    # Insert new videos
    for v in videos:
        score = v.get("score", {})
        c.execute("""
            INSERT OR REPLACE INTO cached_videos 
            (video_id, url, title, artist, song, channel, year, published, duration, 
             views, hd, thumbnail, category, score_total, score_fixed, score_guia,
             artist_match, song_match, posted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    query = "SELECT * FROM cached_videos WHERE category = ?"
    params = [category]
    
    if hide_posted:
        query += " AND posted = 0"
    
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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DELETE FROM playlist_videos")
    
    for idx, v in enumerate(videos):
        score = v.get("score", {})
        c.execute("""
            INSERT OR REPLACE INTO playlist_videos
            (video_id, url, title, artist, song, channel, year, published, duration,
             views, hd, thumbnail, score_total, score_fixed, score_guia,
             artist_match, song_match, posted, position)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    query = "SELECT * FROM playlist_videos"
    if hide_posted:
        query += " WHERE posted = 0"
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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO system_config (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
    """, (key, value))
    conn.commit()
    conn.close()


def get_config(key: str) -> Optional[str]:
    """Get config value"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT value FROM system_config WHERE key = ?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def get_cache_status() -> Dict:
    """Get cache statistics"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Count cached videos per category
    c.execute("""
        SELECT category, COUNT(*) as count, MAX(fetched_at) as last_update
        FROM cached_videos
        GROUP BY category
    """)
    categories = {row[0]: {"count": row[1], "last_update": row[2]} for row in c.fetchall()}
    
    # Count playlist videos
    c.execute("SELECT COUNT(*) FROM playlist_videos")
    playlist_count = c.fetchone()[0]
    
    c.execute("SELECT MAX(fetched_at) FROM playlist_videos")
    playlist_update = c.fetchone()[0]
    
    # Get last config updates
    last_category_refresh = get_config("last_category_refresh")
    last_playlist_refresh = get_config("last_playlist_refresh")
    
    conn.close()
    
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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM cached_videos")
    count = c.fetchone()[0]
    conn.close()
    return count == 0
