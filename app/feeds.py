import feedparser
import asyncio
import logging
from datetime import datetime
from time import mktime
from bs4 import BeautifulSoup

from app.database import insert_article

logger = logging.getLogger(__name__)

# Financial news RSS feeds — curated for market-moving content
RSS_FEEDS = {
    "CNBC Top News": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "CNBC World": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
    "CNBC Economy": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258",
    "CNBC Finance": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "MarketWatch Top Stories": "https://feeds.marketwatch.com/marketwatch/topstories/",
    "MarketWatch Markets": "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "Reuters Business": "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex",
    "Seeking Alpha Market News": "https://seekingalpha.com/market_currents.xml",
    "Seeking Alpha Wall St": "https://seekingalpha.com/tag/wall-st-breakfast.xml",
    "Investing.com News": "https://www.investing.com/rss/news.rss",
    "WSJ Markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "WSJ World": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    "FT Home": "https://www.ft.com/?format=rss",
    "BBC Business": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "AP Business": "https://rsshub.app/apnews/topics/business",
    "NY Times Business": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "Bloomberg": "https://feeds.bloomberg.com/markets/news.rss",
    "The Economist Finance": "https://www.economist.com/finance-and-economics/rss.xml",
    "Barrons": "https://www.barrons.com/feed",
}


def clean_html(raw_html: str | None) -> str:
    """Strip HTML tags from summary text."""
    if not raw_html:
        return ""
    # Skip if it doesn't look like HTML
    if "<" not in raw_html:
        text = raw_html.strip()
    else:
        soup = BeautifulSoup(raw_html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
    # Trim to reasonable length
    return text[:1000] if len(text) > 1000 else text


def parse_published_date(entry) -> str | None:
    """Extract published date from feed entry."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            return datetime.fromtimestamp(mktime(entry.published_parsed)).isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        try:
            return datetime.fromtimestamp(mktime(entry.updated_parsed)).isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    return None


def parse_feed(feed_name: str, feed_url: str) -> list[dict]:
    """Parse a single RSS feed and return article dicts."""
    try:
        feed = feedparser.parse(feed_url)
        articles = []

        for entry in feed.entries:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()

            if not title or not link:
                continue

            summary = clean_html(
                entry.get("summary") or entry.get("description") or ""
            )
            published = parse_published_date(entry)

            articles.append({
                "title": title,
                "url": link,
                "source": feed_name,
                "summary": summary,
                "published_at": published,
            })

        logger.info(f"Parsed {len(articles)} articles from {feed_name}")
        return articles

    except Exception as e:
        logger.error(f"Error parsing feed '{feed_name}': {e}")
        return []


async def fetch_all_feeds() -> dict:
    """Fetch all RSS feeds and store new articles in the database."""
    loop = asyncio.get_event_loop()
    stats = {"total_fetched": 0, "new_articles": 0, "duplicates": 0, "errors": 0}

    # Parse feeds concurrently using thread pool (feedparser is synchronous)
    tasks = []
    for name, url in RSS_FEEDS.items():
        tasks.append(loop.run_in_executor(None, parse_feed, name, url))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Feed fetch error: {result}")
            stats["errors"] += 1
            continue

        for article in result:
            stats["total_fetched"] += 1
            article_id = await insert_article(
                title=article["title"],
                url=article["url"],
                source=article["source"],
                summary=article["summary"],
                published_at=article["published_at"],
            )
            if article_id is not None:
                stats["new_articles"] += 1
            else:
                stats["duplicates"] += 1

    logger.info(
        f"Feed fetch complete: {stats['new_articles']} new, "
        f"{stats['duplicates']} duplicates, {stats['errors']} errors"
    )
    return stats


async def fetch_single_feed(feed_name: str) -> dict:
    """Fetch a single feed by name."""
    if feed_name not in RSS_FEEDS:
        return {"error": f"Unknown feed: {feed_name}"}

    loop = asyncio.get_event_loop()
    articles = await loop.run_in_executor(
        None, parse_feed, feed_name, RSS_FEEDS[feed_name]
    )

    new_count = 0
    for article in articles:
        article_id = await insert_article(
            title=article["title"],
            url=article["url"],
            source=article["source"],
            summary=article["summary"],
            published_at=article["published_at"],
        )
        if article_id is not None:
            new_count += 1

    return {"feed": feed_name, "fetched": len(articles), "new": new_count}
