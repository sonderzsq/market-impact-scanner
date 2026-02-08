import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.feeds import fetch_all_feeds
from app.analyzer import analyze_pending_articles, check_ollama_available
from app.email_summary import send_email_summary

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def scheduled_fetch():
    """Periodic feed fetch job."""
    logger.info("Scheduled feed fetch starting...")
    try:
        stats = await fetch_all_feeds()
        logger.info(f"Scheduled fetch done: {stats}")
    except Exception as e:
        logger.error(f"Scheduled fetch failed: {e}")


async def scheduled_analyze():
    """Periodic analysis job — runs after fetch."""
    if not check_ollama_available():
        logger.warning("Skipping scheduled analysis: Ollama not available")
        return
    logger.info("Scheduled analysis starting...")
    try:
        stats = await analyze_pending_articles(batch_size=15)
        logger.info(f"Scheduled analysis done: {stats}")
    except Exception as e:
        logger.error(f"Scheduled analysis failed: {e}")


async def scheduled_email():
    logger.info("Scheduled email summary starting...")
    try:
        result = await send_email_summary()
        logger.info(f"Scheduled email done: {result}")
    except Exception as e:
        logger.error(f"Scheduled email failed: {e}")


def start_scheduler(
    fetch_interval_minutes: int = 15,
    analyze_interval_minutes: int = 5,
    email_interval_hours: int = 3,
):
    scheduler.add_job(
        scheduled_fetch,
        trigger=IntervalTrigger(minutes=fetch_interval_minutes),
        id="feed_fetch",
        name="Fetch RSS feeds",
        replace_existing=True,
    )

    scheduler.add_job(
        scheduled_analyze,
        trigger=IntervalTrigger(minutes=analyze_interval_minutes),
        id="article_analysis",
        name="Analyze pending articles",
        replace_existing=True,
    )

    scheduler.add_job(
        scheduled_email,
        trigger=IntervalTrigger(hours=email_interval_hours),
        id="email_summary",
        name="Send email summary",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        f"Scheduler started: feeds every {fetch_interval_minutes}min, "
        f"analysis every {analyze_interval_minutes}min, "
        f"email every {email_interval_hours}h"
    )


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
