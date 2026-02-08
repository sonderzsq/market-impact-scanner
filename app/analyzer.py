import os
import logging
import json
from pydantic import BaseModel

from app.database import update_analysis, get_unanalyzed_articles

logger = logging.getLogger(__name__)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")


def _get_backend() -> str:
    if GROQ_API_KEY:
        return "groq"
    try:
        from ollama import list as ollama_list
        models = ollama_list()
        available = [m.model for m in models.models] if models.models else []
        if any(OLLAMA_MODEL.split(":")[0] in m for m in available):
            return "ollama"
    except Exception:
        pass
    return "none"


class MarketImpactAnalysis(BaseModel):
    impact_level: str
    impact_score: int
    impact_summary: str
    affected_sectors: list[str]
    market_direction: str


SYSTEM_PROMPT = """You are a senior financial analyst specializing in market impact assessment.

Given a news article headline and summary, analyze its potential impact on financial markets.

Rules:
- impact_level: "high" (major market mover — Fed rate decisions, GDP reports, major M&A, geopolitical crises), "medium" (notable sector impact — earnings beats/misses, sector regulations, commodity shifts), "low" (minor market relevance — small company news, opinion pieces, general business), "none" (no market relevance)
- impact_score: 0-100, where 100 is maximum market impact (e.g., 2008 crisis level) and 0 is zero relevance
- impact_summary: 2-3 concise sentences explaining HOW this impacts markets and WHY
- affected_sectors: list of affected market sectors from: ["Technology", "Healthcare", "Finance", "Energy", "Consumer", "Industrial", "Real Estate", "Utilities", "Materials", "Communications", "Crypto", "Commodities", "Bonds", "Broad Market"]
- market_direction: overall expected direction — "bullish" (positive), "bearish" (negative), "neutral", "mixed" (different effects on different sectors)

Be specific about causation. Don't just say "could affect markets" — explain the mechanism.

Respond with valid JSON matching this schema:
{"impact_level": "...", "impact_score": 0, "impact_summary": "...", "affected_sectors": ["..."], "market_direction": "..."}"""


def _analyze_via_groq(title: str, summary: str) -> MarketImpactAnalysis | None:
    from groq import Groq

    client = Groq(api_key=GROQ_API_KEY)
    user_content = f"HEADLINE: {title}\n\nSUMMARY: {summary}" if summary else f"HEADLINE: {title}"

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )

    raw = response.choices[0].message.content
    return MarketImpactAnalysis.model_validate_json(raw)


def _analyze_via_ollama(title: str, summary: str) -> MarketImpactAnalysis | None:
    from ollama import chat

    user_content = f"HEADLINE: {title}\n\nSUMMARY: {summary}" if summary else f"HEADLINE: {title}"

    response = chat(
        model=OLLAMA_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        format=MarketImpactAnalysis.model_json_schema(),
        options={"temperature": 0.1},
    )

    return MarketImpactAnalysis.model_validate_json(response.message.content)


def analyze_single_article(title: str, summary: str) -> MarketImpactAnalysis | None:
    backend = _get_backend()
    if backend == "none":
        logger.error("No LLM backend available (set GROQ_API_KEY or run Ollama)")
        return None

    try:
        if backend == "groq":
            analysis = _analyze_via_groq(title, summary)
        else:
            analysis = _analyze_via_ollama(title, summary)

        analysis.impact_score = max(0, min(100, analysis.impact_score))
        if analysis.impact_level not in ("high", "medium", "low", "none"):
            analysis.impact_level = "low"

        return analysis

    except Exception as e:
        logger.error(f"Analysis failed ({backend}) for '{title[:50]}...': {e}")
        return None


async def analyze_pending_articles(batch_size: int = 10) -> dict:
    articles = await get_unanalyzed_articles(limit=batch_size)
    stats = {"analyzed": 0, "failed": 0, "total": len(articles)}

    for article in articles:
        analysis = analyze_single_article(
            title=article["title"],
            summary=article.get("summary", ""),
        )

        if analysis:
            await update_analysis(
                article_id=article["id"],
                impact_level=analysis.impact_level,
                impact_score=analysis.impact_score,
                impact_summary=analysis.impact_summary,
                affected_sectors=json.dumps(analysis.affected_sectors),
                market_direction=analysis.market_direction,
            )
            stats["analyzed"] += 1
            logger.info(
                f"[{analysis.impact_level.upper():6}] ({analysis.impact_score:3d}) "
                f"{article['title'][:80]}"
            )
        else:
            stats["failed"] += 1

    return stats


def check_ollama_available() -> bool:
    return _get_backend() != "none"
