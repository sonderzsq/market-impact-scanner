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
    """Fetch analyzed articles, preferring recent (6h) ones."""
    articles = await get_articles(
        impact_level="all",
        sort_by="impact_score",
        sort_order="DESC",
        limit=500,
    )
    analyzed = [a for a in articles if a.get("impact_level") not in (None, "unanalyzed")]
    if not analyzed:
        return []

    since = datetime.utcnow() - timedelta(hours=6)
    recent = [
        a for a in analyzed
        if a.get("published_at") and datetime.fromisoformat(a["published_at"]) > since
    ]
    return recent if len(recent) >= 3 else analyzed


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


def _format_summary_bullets(summary: str) -> str:
    """Break a summary into bullet points by sentence."""
    if not summary:
        return ""
    # Split on sentence boundaries
    sentences = [s.strip() for s in summary.replace(". ", ".\n").split("\n") if s.strip()]
    if len(sentences) <= 1:
        return f"> {summary}"
    return "\n".join(f"> \u2022 {s}" for s in sentences)


def _format_sectors(affected_sectors_raw: str | None) -> str:
    """Format affected sectors as inline tags."""
    if not affected_sectors_raw:
        return ""
    try:
        sectors = json.loads(affected_sectors_raw)
    except (json.JSONDecodeError, TypeError):
        sectors = [s.strip() for s in affected_sectors_raw.split(",") if s.strip()]
    if not sectors:
        return ""
    return " ".join(f"`{s}`" for s in sectors[:6])


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
        dir_label = direction.capitalize()
        title = a.get("title", "Untitled")
        url = a.get("archive_url") or a.get("url", "")
        source = a.get("source", "")
        summary = a.get("impact_summary", "") or ""
        sectors_str = _format_sectors(a.get("affected_sectors"))

        title_display = title[:80] + "..." if len(title) > 80 else title
        link = f"[{title_display}]({url})" if url else title_display
        bullets = _format_summary_bullets(summary)

        parts = [f"**{i}.** {arrow} `{level} {score}` \u2014 _{dir_label}_ | {link}"]
        if bullets:
            parts.append(bullets)
        if sectors_str:
            parts.append(f"> Sectors: {sectors_str}")
        parts.append(f"> \u2014 _{source}_")

        lines.append("\n".join(parts))

    sector_embed = discord.Embed(
        title=f"{sector_name} \u2014 Top Market Movers",
        description="\n\n".join(lines) if lines else "_No articles in this sector._",
        color=color,
        timestamp=datetime.utcnow(),
    )
    sector_embed.set_footer(text=f"{len(sector_articles)} articles in this sector")
    return sector_embed


class MarketBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.loop_started = False
        self.last_sent_at: str | None = None

    async def on_ready(self):
        logger.info(f"Discord bot connected as {self.user}")
        if not self.loop_started:
            self.loop_started = True
            self.update_loop.start()

    @tasks.loop(hours=6)
    async def update_loop(self):
        await self.send_update()

    @update_loop.before_loop
    async def before_update(self):
        await self.wait_until_ready()

    async def _fetch_channel(self, channel_id: int):
        channel = self.get_channel(channel_id)
        if not channel:
            channel = await self.fetch_channel(channel_id)
        return channel

    async def send_update(self, force: bool = False):
        """Single 6h update to all channels."""
        if not force and self.last_sent_at:
            new_count = await get_new_article_count_since(self.last_sent_at)
            if new_count == 0:
                logger.info("No new analyzed articles since last send, skipping")
                return

        stats = await get_article_count()
        try:
            buckets = await build_sector_buckets()
        except Exception as e:
            logger.error(f"Failed to build sector buckets: {e}")
            return

        # Main channel — header
        if CHANNEL_ID:
            try:
                header = discord.Embed(
                    title="Market Impact Scanner \u2014 6h Update",
                    description=(
                        f"**{stats['total']}** total articles | "
                        f"**{stats['high_impact']}** high | "
                        f"**{stats['medium_impact']}** medium | "
                        f"**{stats['low_impact']}** low"
                    ),
                    color=0x26A69A,
                    timestamp=datetime.utcnow(),
                )
                header.set_footer(text="Next update in 6 hours")
                channel = await self._fetch_channel(CHANNEL_ID)
                await channel.send(embed=header)
            except Exception as e:
                logger.error(f"Failed to send header: {e}")

        # Sector channels
        for sector_name, channel_id in SECTOR_CHANNELS.items():
            if not channel_id:
                continue
            try:
                embed = build_sector_embed(sector_name, buckets.get(sector_name, []))
                channel = await self._fetch_channel(channel_id)
                await channel.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send {sector_name}: {e}")

        if EXTERNAL_CHANNEL_ID:
            try:
                all_high = [
                    a for a in await _get_analyzed_pool()
                    if (a.get("impact_level") or "").lower() == "high"
                ]
                all_high.sort(key=lambda a: a.get("impact_score", 0), reverse=True)
                top = all_high[:10]

                lines = []
                for i, a in enumerate(top, 1):
                    score = a.get("impact_score", 0)
                    direction = a.get("market_direction", "neutral")
                    arrow = DIRECTION_ARROWS.get(direction, "\u2014")
                    dir_label = direction.capitalize()
                    title = a.get("title", "Untitled")
                    url = a.get("archive_url") or a.get("url", "")
                    summary = a.get("impact_summary", "") or ""
                    sectors_str = _format_sectors(a.get("affected_sectors"))

                    title_display = title[:80] + "..." if len(title) > 80 else title
                    link = f"[{title_display}]({url})" if url else title_display
                    bullets = _format_summary_bullets(summary)

                    parts = [f"**{i}.** {arrow} `HIGH {score}` \u2014 _{dir_label}_ | {link}"]
                    if bullets:
                        parts.append(bullets)
                    if sectors_str:
                        parts.append(f"> Sectors: {sectors_str}")
                    lines.append("\n".join(parts))

                embed = discord.Embed(
                    title="Market Impact Scanner \u2014 6h High Impact Summary",
                    description="\n\n".join(lines) if lines else "_No high-impact articles in the last 6 hours._",
                    color=0xEF5350,
                    timestamp=datetime.utcnow(),
                )
                embed.set_footer(
                    text=f"{len(all_high)} high-impact articles | "
                         f"{stats['medium_impact']} medium | {stats['low_impact']} low"
                )
                channel = await self._fetch_channel(EXTERNAL_CHANNEL_ID)
                await channel.send(embed=embed)
            except Exception as e:
                logger.error(f"Failed to send external summary: {e}")

        self.last_sent_at = datetime.utcnow().isoformat()
        logger.info("6h update sent to all channels")


bot_instance: MarketBot | None = None


async def start_discord_bot():
    global bot_instance
    if not DISCORD_TOKEN:
        logger.warning("DISCORD_BOT_TOKEN not set, skipping Discord bot")
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
        await bot_instance.send_update(force=True)
    else:
        logger.warning("Discord bot is not running")


async def test_external_channel() -> dict:
    result = {
        "external_channel_id": EXTERNAL_CHANNEL_ID,
        "bot_running": False,
        "bot_user": None,
        "channel_found": False,
        "channel_name": None,
        "guild_name": None,
        "send_success": False,
        "error": None,
    }

    if not bot_instance or bot_instance.is_closed():
        result["error"] = "Bot is not running or is closed"
        return result

    result["bot_running"] = True
    result["bot_user"] = str(bot_instance.user)

    if not EXTERNAL_CHANNEL_ID:
        result["error"] = "DISCORD_CHANNEL_EXTERNAL env var not set or is 0"
        return result

    result["guilds"] = [
        {"id": g.id, "name": g.name, "member_count": g.member_count}
        for g in bot_instance.guilds
    ]

    try:
        channel = await bot_instance._fetch_channel(EXTERNAL_CHANNEL_ID)
        result["channel_found"] = True
        result["channel_name"] = getattr(channel, "name", "unknown")
        result["guild_name"] = getattr(getattr(channel, "guild", None), "name", "unknown")
    except Exception as e:
        result["error"] = f"Failed to fetch channel: {type(e).__name__}: {e}"
        return result

    try:
        test_embed = discord.Embed(
            title="External Channel Test",
            description="If you see this, the bot can send to this channel.",
            color=0x00FF00,
            timestamp=datetime.utcnow(),
        )
        await channel.send(embed=test_embed)
        result["send_success"] = True
    except Exception as e:
        result["error"] = f"Failed to send message: {type(e).__name__}: {e}"

    return result
