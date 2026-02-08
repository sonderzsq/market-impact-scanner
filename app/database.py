import aiosqlite
import os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "news.db")


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                source TEXT NOT NULL,
                summary TEXT,
                published_at TEXT,
                fetched_at TEXT NOT NULL,
                impact_level TEXT DEFAULT 'unanalyzed',
                impact_score INTEGER DEFAULT 0,
                impact_summary TEXT,
                affected_sectors TEXT,
                market_direction TEXT,
                analyzed_at TEXT
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_articles_impact
            ON articles(impact_level, impact_score DESC)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_articles_published
            ON articles(published_at DESC)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_articles_url
            ON articles(url)
        """)
        await db.commit()


async def insert_article(
    title: str,
    url: str,
    source: str,
    summary: str | None = None,
    published_at: str | None = None,
) -> int | None:
    """Insert an article, returning its ID. Returns None if duplicate."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute(
                """
                INSERT INTO articles (title, url, source, summary, published_at, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (title, url, source, summary, published_at, datetime.utcnow().isoformat()),
            )
            await db.commit()
            return cursor.lastrowid
        except aiosqlite.IntegrityError:
            return None


async def update_analysis(
    article_id: int,
    impact_level: str,
    impact_score: int,
    impact_summary: str,
    affected_sectors: str,
    market_direction: str,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE articles
            SET impact_level = ?, impact_score = ?, impact_summary = ?,
                affected_sectors = ?, market_direction = ?, analyzed_at = ?
            WHERE id = ?
            """,
            (
                impact_level,
                impact_score,
                impact_summary,
                affected_sectors,
                market_direction,
                datetime.utcnow().isoformat(),
                article_id,
            ),
        )
        await db.commit()


async def get_articles(
    impact_level: str | None = None,
    source: str | None = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "published_at",
    sort_order: str = "DESC",
) -> list[dict]:
    allowed_sort = {"published_at", "impact_score", "fetched_at", "source", "impact_level"}
    if sort_by not in allowed_sort:
        sort_by = "published_at"
    if sort_order.upper() not in ("ASC", "DESC"):
        sort_order = "DESC"

    conditions = []
    params: list = []

    if impact_level and impact_level != "all":
        conditions.append("impact_level = ?")
        params.append(impact_level)
    if source and source != "all":
        conditions.append("source = ?")
        params.append(source)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall(
            f"SELECT * FROM articles {where} ORDER BY {sort_by} {sort_order} LIMIT ? OFFSET ?",
            params + [limit, offset],
        )
        return [dict(row) for row in rows]


async def get_unanalyzed_articles(limit: int = 20) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall(
            "SELECT * FROM articles WHERE impact_level = 'unanalyzed' ORDER BY fetched_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in rows]


async def get_article_count() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        total = await db.execute_fetchall("SELECT COUNT(*) as count FROM articles")
        analyzed = await db.execute_fetchall(
            "SELECT COUNT(*) as count FROM articles WHERE impact_level != 'unanalyzed'"
        )
        high = await db.execute_fetchall(
            "SELECT COUNT(*) as count FROM articles WHERE impact_level = 'high'"
        )
        medium = await db.execute_fetchall(
            "SELECT COUNT(*) as count FROM articles WHERE impact_level = 'medium'"
        )
        low = await db.execute_fetchall(
            "SELECT COUNT(*) as count FROM articles WHERE impact_level = 'low'"
        )
        return {
            "total": total[0]["count"],
            "analyzed": analyzed[0]["count"],
            "high_impact": high[0]["count"],
            "medium_impact": medium[0]["count"],
            "low_impact": low[0]["count"],
        }


async def get_new_article_count_since(since_iso: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        rows = await db.execute_fetchall(
            "SELECT COUNT(*) as count FROM articles WHERE analyzed_at > ? AND impact_level != 'unanalyzed'",
            (since_iso,),
        )
        return rows[0][0] if rows else 0


async def get_sources() -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await db.execute_fetchall(
            "SELECT DISTINCT source FROM articles ORDER BY source"
        )
        return [row["source"] for row in rows]


async def get_market_summary(since_hours: int | None = None) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        if since_hours:
            since = (datetime.utcnow() - timedelta(hours=since_hours)).isoformat()
            rows = await db.execute_fetchall(
                """SELECT title, url, source, impact_level, impact_score,
                          impact_summary, affected_sectors, market_direction, published_at
                   FROM articles
                   WHERE impact_level != 'unanalyzed' AND impact_level IS NOT NULL
                     AND analyzed_at > ?
                   ORDER BY impact_score DESC""",
                (since,),
            )
        else:
            rows = await db.execute_fetchall(
                """SELECT title, url, source, impact_level, impact_score,
                          impact_summary, affected_sectors, market_direction, published_at
                   FROM articles
                   WHERE impact_level != 'unanalyzed' AND impact_level IS NOT NULL
                   ORDER BY impact_score DESC"""
            )
        analyzed = [dict(r) for r in rows]

        if not analyzed:
            return {
                "total_analyzed": 0,
                "overall_direction": "neutral",
                "direction_breakdown": {"bullish": 0, "bearish": 0, "neutral": 0, "mixed": 0},
                "impact_breakdown": {"high": 0, "medium": 0, "low": 0, "none": 0},
                "avg_score": 0,
                "top_drivers": [],
                "sector_sentiment": {},
            }

        # Direction breakdown
        directions = {"bullish": 0, "bearish": 0, "neutral": 0, "mixed": 0}
        for a in analyzed:
            d = a.get("market_direction", "neutral")
            if d in directions:
                directions[d] += 1

        # Overall direction: weighted by impact_score
        direction_scores = {"bullish": 0, "bearish": 0, "neutral": 0, "mixed": 0}
        for a in analyzed:
            d = a.get("market_direction", "neutral")
            score = a.get("impact_score", 0)
            if d in direction_scores:
                direction_scores[d] += score
        overall = max(direction_scores, key=direction_scores.get)

        # Impact breakdown
        impacts = {"high": 0, "medium": 0, "low": 0, "none": 0}
        for a in analyzed:
            lvl = a.get("impact_level", "none")
            if lvl in impacts:
                impacts[lvl] += 1

        # Average score
        scores = [a.get("impact_score", 0) for a in analyzed]
        avg_score = round(sum(scores) / len(scores), 1) if scores else 0

        # Top drivers: high-impact articles (top 5 by score)
        top_drivers = []
        for a in analyzed[:5]:
            top_drivers.append({
                "title": a["title"],
                "url": a.get("url", ""),
                "source": a.get("source", ""),
                "impact_score": a.get("impact_score", 0),
                "impact_level": a.get("impact_level", ""),
                "impact_summary": a.get("impact_summary", ""),
                "market_direction": a.get("market_direction", "neutral"),
            })

        # Sector sentiment: aggregate direction per sector
        import json as _json
        sector_data: dict[str, dict] = {}
        for a in analyzed:
            raw = a.get("affected_sectors")
            if not raw:
                continue
            try:
                sectors = _json.loads(raw)
            except (ValueError, TypeError):
                sectors = [s.strip() for s in raw.split(",") if s.strip()]
            d = a.get("market_direction", "neutral")
            score = a.get("impact_score", 0)
            for s in sectors:
                if s not in sector_data:
                    sector_data[s] = {"bullish": 0, "bearish": 0, "neutral": 0, "mixed": 0, "count": 0, "total_score": 0}
                sector_data[s]["count"] += 1
                sector_data[s]["total_score"] += score
                if d in sector_data[s]:
                    sector_data[s][d] += 1

        # Compute dominant direction per sector
        sector_sentiment = {}
        for sector, data in sorted(sector_data.items(), key=lambda x: x[1]["total_score"], reverse=True):
            dominant = max(["bullish", "bearish", "neutral", "mixed"], key=lambda k: data[k])
            sector_sentiment[sector] = {
                "direction": dominant,
                "count": data["count"],
                "avg_score": round(data["total_score"] / data["count"], 1) if data["count"] else 0,
            }

        return {
            "total_analyzed": len(analyzed),
            "overall_direction": overall,
            "direction_breakdown": directions,
            "impact_breakdown": impacts,
            "avg_score": avg_score,
            "top_drivers": top_drivers,
            "sector_sentiment": sector_sentiment,
        }
