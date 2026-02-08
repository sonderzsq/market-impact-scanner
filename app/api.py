import logging
from fastapi import APIRouter, Query

from app.database import get_articles, get_article_count, get_sources, get_market_summary
from app.feeds import fetch_all_feeds, RSS_FEEDS
from app.analyzer import analyze_pending_articles, check_ollama_available
from app.discord_bot import send_summary_now
from app.email_summary import send_email_summary

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


@router.get("/articles")
async def list_articles(
    impact_level: str = Query("all", description="Filter by impact level"),
    source: str = Query("all", description="Filter by source"),
    sort_by: str = Query("published_at", description="Sort field"),
    sort_order: str = Query("DESC", description="ASC or DESC"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    articles = await get_articles(
        impact_level=impact_level,
        source=source,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    return articles


@router.get("/stats")
async def article_stats():
    return await get_article_count()


@router.get("/sources")
async def list_sources():
    return await get_sources()


@router.get("/feeds")
async def list_feeds():
    return list(RSS_FEEDS.keys())


@router.post("/fetch")
async def trigger_fetch():
    """Fetch all RSS feeds and store new articles."""
    stats = await fetch_all_feeds()
    return stats


@router.post("/analyze")
async def trigger_analysis(
    batch_size: int = Query(20, ge=1, le=100, description="Articles to analyze per batch"),
):
    """Analyze unanalyzed articles using Ollama."""
    if not check_ollama_available():
        return {
            "error": "Ollama is not available. Make sure Ollama is running and the model is pulled.",
            "fix": "Run: ollama serve  (then in another terminal) ollama pull llama3.1:8b",
        }
    stats = await analyze_pending_articles(batch_size=batch_size)
    return stats


@router.post("/discord-summary")
async def trigger_discord_summary():
    await send_summary_now()
    return {"status": "sent"}


@router.post("/email-summary")
async def trigger_email_summary():
    result = await send_email_summary()
    return result


@router.get("/market-summary")
async def market_summary():
    """Overall market moves, sentiment, and key drivers."""
    return await get_market_summary()


@router.get("/health")
async def health_check():
    ollama_ok = check_ollama_available()
    stats = await get_article_count()
    return {
        "status": "ok",
        "ollama_available": ollama_ok,
        "articles": stats,
    }
