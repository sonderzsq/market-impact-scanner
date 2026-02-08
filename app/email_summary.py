import os
import logging
from datetime import datetime

import resend

from app.database import get_market_summary

logger = logging.getLogger(__name__)

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "Market Impact Scanner <onboarding@resend.dev>")

DIRECTION_ARROWS = {"bullish": "▲", "bearish": "▼", "mixed": "◆", "neutral": "—"}
DIRECTION_COLORS = {"bullish": "#26a69a", "bearish": "#ef5350", "mixed": "#ffa726", "neutral": "#8a919e"}
IMPACT_COLORS = {"high": "#ef5350", "medium": "#ffa726", "low": "#ffee58", "none": "#545b67"}


def _build_email_html(data: dict, title: str = "Market Summary") -> str:
    overall = data.get("overall_direction", "neutral")
    arrow = DIRECTION_ARROWS.get(overall, "—")
    color = DIRECTION_COLORS.get(overall, "#8a919e")
    bd = data.get("direction_breakdown", {})
    imp = data.get("impact_breakdown", {})
    now = datetime.utcnow().strftime("%B %d, %Y %H:%M UTC")

    drivers_html = ""
    for i, d in enumerate(data.get("top_drivers", []), 1):
        d_dir = d.get("market_direction", "neutral")
        d_arrow = DIRECTION_ARROWS.get(d_dir, "—")
        d_color = DIRECTION_COLORS.get(d_dir, "#8a919e")
        s_color = IMPACT_COLORS.get(d.get("impact_level", "low"), "#545b67")
        link = f'<a href="{d["url"]}" style="color:#42a5f5;text-decoration:none;">{d["title"]}</a>' if d.get("url") else d.get("title", "")
        drivers_html += f"""
        <tr>
            <td style="padding:10px 12px;border-bottom:1px solid #1e2636;">
                <span style="color:{d_color};font-weight:700;font-size:16px;">{d_arrow}</span>
            </td>
            <td style="padding:10px 8px;border-bottom:1px solid #1e2636;">
                <div style="font-size:13px;font-weight:600;color:#e1e4ea;line-height:1.4;">{link}</div>
                <div style="font-size:11px;color:#545b67;margin-top:3px;">{d.get("source", "")}</div>
                <div style="font-size:12px;color:#8a919e;margin-top:5px;line-height:1.5;">{d.get("impact_summary", "")}</div>
            </td>
            <td style="padding:10px 12px;border-bottom:1px solid #1e2636;text-align:right;">
                <span style="font-size:14px;font-weight:800;color:{s_color};">{d.get("impact_score", 0)}</span>
            </td>
        </tr>"""

    sectors_html = ""
    for name, info in list(data.get("sector_sentiment", {}).items())[:15]:
        s_dir = info.get("direction", "neutral")
        s_arrow = DIRECTION_ARROWS.get(s_dir, "—")
        s_color = DIRECTION_COLORS.get(s_dir, "#8a919e")
        sectors_html += f"""
        <td style="padding:6px 10px;text-align:center;">
            <span style="color:{s_color};font-weight:700;">{s_arrow}</span>
            <span style="color:#e1e4ea;font-size:12px;font-weight:600;">{name}</span>
            <span style="color:#545b67;font-size:10px;">({info.get("count", 0)})</span>
        </td>"""

    sector_rows = ""
    items = list(data.get("sector_sentiment", {}).items())[:15]
    for i in range(0, len(items), 4):
        chunk = items[i:i + 4]
        cells = ""
        for name, info in chunk:
            s_dir = info.get("direction", "neutral")
            s_arrow = DIRECTION_ARROWS.get(s_dir, "—")
            s_color = DIRECTION_COLORS.get(s_dir, "#8a919e")
            cells += f"""<td style="padding:8px 10px;border-bottom:1px solid #1e2636;">
                <span style="color:{s_color};font-weight:700;font-size:14px;">{s_arrow}</span>
                <span style="color:#e1e4ea;font-size:12px;font-weight:600;">&nbsp;{name}</span>
                <span style="color:#545b67;font-size:10px;">&nbsp;({info.get("count", 0)})</span>
            </td>"""
        sector_rows += f"<tr>{cells}</tr>"

    return f"""
    <div style="background:#0a0e17;color:#e1e4ea;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:0;margin:0;">
        <div style="max-width:640px;margin:0 auto;padding:20px;">
            <div style="text-align:center;padding:20px 0;border-bottom:1px solid #1e2636;">
                <h1 style="margin:0;font-size:22px;color:#e1e4ea;">MIS<span style="color:#26a69a;">.</span> {title}</h1>
                <p style="margin:6px 0 0;font-size:12px;color:#545b67;">{now}</p>
            </div>

            <div style="text-align:center;padding:24px 0;border-bottom:1px solid #1e2636;">
                <div style="font-size:36px;font-weight:700;color:{color};">{arrow}</div>
                <div style="font-size:22px;font-weight:700;color:{color};text-transform:uppercase;letter-spacing:1px;margin-top:4px;">{overall}</div>
                <div style="font-size:11px;color:#545b67;margin-top:4px;">Overall Market Sentiment</div>
            </div>

            <table width="100%" cellpadding="0" cellspacing="0" style="border-bottom:1px solid #1e2636;">
                <tr>
                    <td style="padding:16px;text-align:center;">
                        <div style="font-size:18px;font-weight:700;">{data.get("total_analyzed", 0)}</div>
                        <div style="font-size:10px;color:#545b67;text-transform:uppercase;">Analyzed</div>
                    </td>
                    <td style="padding:16px;text-align:center;">
                        <div style="font-size:18px;font-weight:700;">{data.get("avg_score", 0)}</div>
                        <div style="font-size:10px;color:#545b67;text-transform:uppercase;">Avg Score</div>
                    </td>
                    <td style="padding:16px;text-align:center;">
                        <div style="font-size:18px;font-weight:700;color:#26a69a;">{bd.get("bullish", 0)}</div>
                        <div style="font-size:10px;color:#545b67;text-transform:uppercase;">Bullish</div>
                    </td>
                    <td style="padding:16px;text-align:center;">
                        <div style="font-size:18px;font-weight:700;color:#ef5350;">{bd.get("bearish", 0)}</div>
                        <div style="font-size:10px;color:#545b67;text-transform:uppercase;">Bearish</div>
                    </td>
                    <td style="padding:16px;text-align:center;">
                        <div style="font-size:18px;font-weight:700;color:#8a919e;">{bd.get("neutral", 0)}</div>
                        <div style="font-size:10px;color:#545b67;text-transform:uppercase;">Neutral</div>
                    </td>
                </tr>
            </table>

            <div style="padding:20px 0;">
                <h2 style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#545b67;margin:0 0 12px;">Key Drivers</h2>
                <table width="100%" cellpadding="0" cellspacing="0" style="background:#131722;border-radius:8px;">
                    {drivers_html}
                </table>
            </div>

            <div style="padding:0 0 20px;">
                <h2 style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#545b67;margin:0 0 12px;">Sector Sentiment</h2>
                <table width="100%" cellpadding="0" cellspacing="0" style="background:#131722;border-radius:8px;">
                    {sector_rows}
                </table>
            </div>

            <div style="padding:16px 0;border-top:1px solid #1e2636;text-align:center;">
                <p style="font-size:11px;color:#545b67;margin:0;">
                    High Impact: {imp.get("high", 0)} · Medium: {imp.get("medium", 0)} · Low: {imp.get("low", 0)}
                </p>
            </div>
        </div>
    </div>"""


async def send_email_summary() -> dict:
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set, skipping email summary")
        return {"status": "skipped", "reason": "no API key"}
    if not EMAIL_TO:
        logger.warning("EMAIL_TO not set, skipping email summary")
        return {"status": "skipped", "reason": "no recipient"}

    resend.api_key = RESEND_API_KEY
    data = await get_market_summary()

    if data.get("total_analyzed", 0) == 0:
        logger.info("No analyzed articles, skipping email summary")
        return {"status": "skipped", "reason": "no data"}

    overall = data.get("overall_direction", "neutral").upper()
    arrow = DIRECTION_ARROWS.get(data.get("overall_direction", "neutral"), "—")
    subject = f"{arrow} Market {overall} — {data['total_analyzed']} articles analyzed | MIS Summary"

    html = _build_email_html(data)

    try:
        result = resend.Emails.send({
            "from": EMAIL_FROM,
            "to": [addr.strip() for addr in EMAIL_TO.split(",")],
            "subject": subject,
            "html": html,
        })
        logger.info(f"Sent email summary to {EMAIL_TO}")
        return {"status": "sent", "id": result.get("id", "")}
    except Exception as e:
        logger.error(f"Failed to send email summary: {e}")
        return {"status": "error", "error": str(e)}


async def send_daily_digest() -> dict:
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set, skipping daily digest")
        return {"status": "skipped", "reason": "no API key"}
    if not EMAIL_TO:
        logger.warning("EMAIL_TO not set, skipping daily digest")
        return {"status": "skipped", "reason": "no recipient"}

    resend.api_key = RESEND_API_KEY
    data = await get_market_summary(since_hours=24)

    if data.get("total_analyzed", 0) == 0:
        logger.info("No articles in last 24h, skipping daily digest")
        return {"status": "skipped", "reason": "no data in last 24h"}

    overall = data.get("overall_direction", "neutral").upper()
    arrow = DIRECTION_ARROWS.get(data.get("overall_direction", "neutral"), "—")
    today = datetime.utcnow().strftime("%b %d, %Y")
    subject = f"{arrow} Daily Digest — Market {overall} | {data['total_analyzed']} articles | {today}"

    html = _build_email_html(data, title="Daily Digest — Past 24 Hours")

    try:
        result = resend.Emails.send({
            "from": EMAIL_FROM,
            "to": [addr.strip() for addr in EMAIL_TO.split(",")],
            "subject": subject,
            "html": html,
        })
        logger.info(f"Sent daily digest to {EMAIL_TO}")
        return {"status": "sent", "id": result.get("id", "")}
    except Exception as e:
        logger.error(f"Failed to send daily digest: {e}")
        return {"status": "error", "error": str(e)}
