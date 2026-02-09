import logging
import asyncio
import aiohttp

from app.database import get_articles_without_archive, update_archive_url

logger = logging.getLogger(__name__)

WAYBACK_SAVE_URL = "https://web.archive.org/save/"
WAYBACK_CHECK_URL = "https://archive.org/wayback/available?url="


async def save_to_wayback(url: str) -> str | None:
    """Submit a URL to Wayback Machine and return the archive URL."""
    try:
        async with aiohttp.ClientSession() as session:
            # First check if it's already archived recently
            async with session.get(
                f"{WAYBACK_CHECK_URL}{url}",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    snapshot = data.get("archived_snapshots", {}).get("closest")
                    if snapshot and snapshot.get("available"):
                        archive_url = snapshot["url"]
                        logger.info(f"Already archived: {url} -> {archive_url}")
                        return archive_url

            # Not archived yet — request a save
            async with session.get(
                f"{WAYBACK_SAVE_URL}{url}",
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True,
            ) as resp:
                if resp.status == 200:
                    # The final URL after redirect is the archive URL
                    archive_url = str(resp.url)
                    if "web.archive.org" in archive_url:
                        logger.info(f"Saved to Wayback: {url} -> {archive_url}")
                        return archive_url

                # Fallback: construct the availability URL
                # Sometimes save returns non-200 but still archives
                await asyncio.sleep(2)
                async with session.get(
                    f"{WAYBACK_CHECK_URL}{url}",
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as check_resp:
                    if check_resp.status == 200:
                        data = await check_resp.json()
                        snapshot = data.get("archived_snapshots", {}).get("closest")
                        if snapshot and snapshot.get("available"):
                            archive_url = snapshot["url"]
                            logger.info(f"Saved to Wayback (verified): {url} -> {archive_url}")
                            return archive_url

    except asyncio.TimeoutError:
        logger.warning(f"Wayback save timed out for {url}")
    except Exception as e:
        logger.warning(f"Wayback save failed for {url}: {e}")

    return None


async def archive_pending_articles(batch_size: int = 20) -> dict:
    """Archive articles that don't have an archive URL yet."""
    articles = await get_articles_without_archive(limit=batch_size)
    if not articles:
        return {"archived": 0, "failed": 0, "total": 0}

    archived = 0
    failed = 0

    for article in articles:
        archive_url = await save_to_wayback(article["url"])
        if archive_url:
            await update_archive_url(article["id"], archive_url)
            archived += 1
        else:
            failed += 1
        # Rate limit: be nice to Wayback Machine
        await asyncio.sleep(1)

    logger.info(f"Archiving done: {archived} saved, {failed} failed out of {len(articles)}")
    return {"archived": archived, "failed": failed, "total": len(articles)}
