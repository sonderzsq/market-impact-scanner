import os
import json
import logging
import asyncio
from datetime import datetime, timedelta

import discord
from discord.ext import tasks
from dotenv import load_dotenv

from app.database import get_articles, get_article_count, get_new_article_count_since

load_dotenv()

logger = logging.getLogger(__name__)

DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))

SECTOR_CHANNELS = {
    "TMT": int(os.getenv("DISCORD_CHANNEL_TMT", "0")),
    "Defensive": int(os.getenv("DISCORD_CHANNEL_DEFENSIVE", "0")),
    "Macroeconomics": int(os.getenv("DISCORD_CHANNEL_MACRO", "0")),
    "Cyclical": int(os.getenv("DISCORD_CHANNEL_CYCLICAL", "0")),
}

EXTERNAL_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_EXTERNAL", "0"))

SECTOR_MAP = {
    "TMT": ["technology", "communications", "media", "telecom"],
    "Defensive": ["healthcare", "utilities", "consumer staples", "consumer"],
    "Macroeconomics": ["broad market", "bonds", "commodities", "crypto"],
    "Cyclical": ["finance", "energy", "industrial", "real estate", "materials"],
}

SECTOR_COLORS = {
    "TMT": 0x42A5F5,
    "Defensive": 0x26A69A,
    "Macroeconomics": 0xFFA726,
    "Cyclical": 0xAB47BC,
}

DIRECTION_ARROWS = {
    "bullish": "\u25B2",
    "bearish": "\u25BC",
    "mixed": "\u25C6",
    "neutral": "\u2014",
}


def classify_to_sector(affected_sectors_raw: str | None) -> list[str]:
    if not affected_sectors_raw:
        return []
    try:
        sectors = json.loads(affected_sectors_raw)
    except (json.JSONDecodeError, TypeError):
        sectors = [s.strip() for s in affected_sectors_raw.split(",") if s.strip()]

    matched = []
    for category, keywords in SECTOR_MAP.items():
        for s in sectors:
            if any(k in s.lower() for k in keywords):
                matched.append(category)
                break
    return matched


async def _get_analyzed_pool() -> list[dict]:
    """Fetch analyzed articles, preferring recent (3h) ones."""
    articles = await get_articles(
        impact_level="all",
        sort_by="impact_score",
        sort_order="DESC",
        limit=500,
    )
    analyzed = [a for a in articles if a.get("impact_level") not in (None, "unanalyzed")]
    if not analyzed:
        return []

    since = datetime.utcnow() - timedelta(hours=3)
    recent = [
        a for a in analyzed
        if a.get("published_at") and datetime.fromisoformat(a["published_at"]) > since
    ]
    return recent if len(recent) >= 3 else analyzed


async def build_header_embed() -> discord.Embed:
    """Build the overview header embed for the main channel."""
    stats = await get_article_count()
    header = discord.Embed(
        title="Market Impact Scanner \u2014 3h Summary",
        description=(
            f"**{stats['total']}** total articles | "
            f"**{stats['high_impact']}** high | "
            f"**{stats['medium_impact']}** medium | "
            f"**{stats['low_impact']}** low\n\n"
            "Sector summaries posted to their dedicated channels."
        ),
        color=0x26A69A,
        timestamp=datetime.utcnow(),
    )
    header.set_footer(text="Next summary in 3 hours")
    return header


def build_sector_embed(sector_name: str, sector_articles: list[dict]) -> discord.Embed:
    """Build an embed for a single sector's top articles."""
    top = sector_articles[:5]
    color = SECTOR_COLORS.get(sector_name, 0x545B67)

    lines = []
    for i, a in enumerate(top, 1):
        score = a.get("impact_score", 0)
        level = (a.get("impact_level") or "low").upper()
        direction = a.get("market_direction", "neutral")
        arrow = DIRECTION_ARROWS.get(direction, "\u2014")
        title = a.get("title", "Untitled")
        url = a.get("archive_url") or a.get("url", "")
        source = a.get("source", "")

        title_display = title[:80] + "..." if len(title) > 80 else title
        link = f"[{title_display}]({url})" if url else title_display
        lines.append(f"**{i}.** {arrow} `{level} {score}` {link}\n> _{source}_")

    sector_embed = discord.Embed(
        title=f"{sector_name} \u2014 Top Market Movers",
        description="\n\n".join(lines) if lines else "_No articles in this sector._",
        color=color,
        timestamp=datetime.utcnow(),
    )
    sector_embed.set_footer(text=f"{len(sector_articles)} articles in this sector")
    return sector_embed


async def build_sector_buckets() -> dict[str, list[dict]]:
    """Classify analyzed articles into sector buckets."""
    pool = await _get_analyzed_pool()
    buckets: dict[str, list[dict]] = {s: [] for s in SECTOR_MAP}
    for article in pool:
        for sector in classify_to_sector(article.get("affected_sectors")):
            buckets[sector].append(article)
    for sector in buckets:
        buckets[sector].sort(key=lambda a: a.get("impact_score", 0), reverse=True)
    return buckets


async def _get_high_impact_recent() -> list[dict]:
    """Fetch only HIGH impact articles analyzed in the past 6 hours."""
    articles = await get_articles(
        impact_level="high",
        sort_by="impact_score",
        sort_order="DESC",
        limit=500,
    )
    since = datetime.utcnow() - timedelta(hours=6)
    recent = []
    for a in articles:
        analyzed = a.get("analyzed_at") or a.get("published_at")
        if analyzed:
            try:
                if datetime.fromisoformat(analyzed) > since:
                    recent.append(a)
            except (ValueError, TypeError):
                pass
    return recent


async def build_external_summary_embeds() -> list[discord.Embed]:
    """Build compact high-impact-only summary for the external channel (past 6h)."""
    pool = await _get_high_impact_recent()
    embeds = []

    # Classify into sector buckets
    buckets: dict[str, list[dict]] = {s: [] for s in SECTOR_MAP}
    for article in pool:
        matched = classify_to_sector(article.get("affected_sectors"))
        for sector in matched:
            buckets[sector].append(article)

    total_high = len(pool)

    # Header
    header = discord.Embed(
        title="Market Impact Scanner \u2014 High Impact (Past 6h)",
        description=(
            f"**{total_high}** high-impact articles across all sectors"
        ),
        color=0xEF5350,
        timestamp=datetime.utcnow(),
    )
    if total_high == 0:
        header.description += "\n\n_No high-impact news in the past 6 hours._"
    embeds.append(header)

    if total_high == 0:
        return embeds

    # One embed per sector (only if it has articles)
    for sector_name in ["TMT", "Defensive", "Macroeconomics", "Cyclical"]:
        articles = buckets.get(sector_name, [])
        if not articles:
            continue
        top = articles[:5]
        color = SECTOR_COLORS.get(sector_name, 0x545B67)

        lines = []
        for i, a in enumerate(top, 1):
            score = a.get("impact_score", 0)
            direction = a.get("market_direction", "neutral")
            arrow = DIRECTION_ARROWS.get(direction, "\u2014")
            title = a.get("title", "Untitled")
            url = a.get("archive_url") or a.get("url", "")
            summary = a.get("impact_summary", "") or ""

            title_display = title[:70] + "..." if len(title) > 70 else title
            link = f"[{title_display}]({url})" if url else title_display
            summary_display = summary[:120] + "..." if len(summary) > 120 else summary
            lines.append(f"**{i}.** {arrow} `{score}` {link}\n> {summary_display}")

        embed = discord.Embed(
            title=f"{sector_name}",
            description="\n\n".join(lines),
            color=color,
        )
        embed.set_footer(text=f"{len(articles)} high-impact articles")
        embeds.append(embed)

    return embeds


class MarketBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.channel_id = CHANNEL_ID
        self.summary_loop_started = False
        self.last_sent_at: str | None = None

    async def on_ready(self):
        logger.info(f"Discord bot connected as {self.user}")
        if not self.summary_loop_started:
            self.summary_loop_started = True
            self.send_summary_loop.start()
            if EXTERNAL_CHANNEL_ID:
                self.send_external_loop.start()

    @tasks.loop(hours=3)
    async def send_summary_loop(self):
        await self.send_summary()

    @send_summary_loop.before_loop
    async def before_summary(self):
        await self.wait_until_ready()

    @tasks.loop(hours=6)
    async def send_external_loop(self):
        await self.send_external_summary()

    @send_external_loop.before_loop
    async def before_external(self):
        await self.wait_until_ready()

    async def _fetch_channel(self, channel_id: int):
        """Get a channel by ID, falling back to fetch_channel if not cached."""
        channel = self.get_channel(channel_id)
        if not channel:
            channel = await self.fetch_channel(channel_id)
        return channel

    async def send_summary(self, force: bool = False):
        if not force and self.last_sent_at:
            new_count = await get_new_article_count_since(self.last_sent_at)
            if new_count == 0:
                logger.info("No new analyzed articles since last send, skipping Discord summary")
                return

        try:
            header_embed = await build_header_embed()
            main_channel = await self._fetch_channel(self.channel_id)
            await main_channel.send(embed=header_embed)
            logger.info(f"Sent header summary to main channel {self.channel_id}")
        except Exception as e:
            logger.error(f"Failed to send header to main channel: {e}")

        try:
            buckets = await build_sector_buckets()
        except Exception as e:
            logger.error(f"Failed to build sector buckets: {e}")
            return

        for sector_name, channel_id in SECTOR_CHANNELS.items():
            if not channel_id:
                logger.warning(f"No channel ID configured for {sector_name}, skipping")
                continue
            try:
                sector_articles = buckets.get(sector_name, [])
                embed = build_sector_embed(sector_name, sector_articles)
                channel = await self._fetch_channel(channel_id)
                await channel.send(embed=embed)
                logger.info(f"Sent {sector_name} summary to channel {channel_id} ({len(sector_articles)} articles)")
            except Exception as e:
                logger.error(f"Failed to send {sector_name} summary to channel {channel_id}: {e}")

        self.last_sent_at = datetime.utcnow().isoformat()

    async def send_external_summary(self):
        """Send compact all-sector summary to the external server channel."""
        if not EXTERNAL_CHANNEL_ID:
            return
        try:
            embeds = await build_external_summary_embeds()
            channel = await self._fetch_channel(EXTERNAL_CHANNEL_ID)
            # Discord allows max 10 embeds per message
            await channel.send(embeds=embeds[:10])
            logger.info(f"Sent external summary to channel {EXTERNAL_CHANNEL_ID}")
        except Exception as e:
            logger.error(f"Failed to send external summary: {e}")


bot_instance: MarketBot | None = None


async def start_discord_bot():
    global bot_instance
    if not DISCORD_TOKEN:
        logger.warning("DISCORD_BOT_TOKEN not set, skipping Discord bot")
        return
    if not CHANNEL_ID:
        logger.warning("DISCORD_CHANNEL_ID not set, skipping Discord bot")
        return

    bot_instance = MarketBot()
    asyncio.create_task(bot_instance.start(DISCORD_TOKEN))
    logger.info("Discord bot starting in background...")


async def stop_discord_bot():
    global bot_instance
    if bot_instance and not bot_instance.is_closed():
        await bot_instance.close()
        logger.info("Discord bot stopped")


async def send_summary_now():
    if bot_instance and not bot_instance.is_closed():
        await bot_instance.send_summary(force=True)
    else:
        logger.warning("Discord bot is not running")


async def send_external_summary_now():
    if bot_instance and not bot_instance.is_closed():
        await bot_instance.send_external_summary()
    else:
        logger.warning("Discord bot is not running")
