import logging
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.responses import HTMLResponse

from dotenv import load_dotenv
load_dotenv()

from app.database import init_db
from app.api import router as api_router
from app.feeds import fetch_all_feeds
from app.scheduler import start_scheduler, stop_scheduler
from app.discord_bot import start_discord_bot, stop_discord_bot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic."""
    # Startup
    logger.info("Initializing database...")
    await init_db()

    logger.info("Running initial feed fetch...")
    asyncio.create_task(fetch_all_feeds())

    logger.info("Starting background scheduler...")
    start_scheduler(fetch_interval_minutes=15, analyze_interval_minutes=5)

    logger.info("Starting Discord bot...")
    await start_discord_bot()

    yield

    await stop_discord_bot()
    stop_scheduler()
    logger.info("Application shut down.")


app = FastAPI(
    title="Market Impact Scanner",
    description="Financial news aggregator with AI-powered market impact analysis",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Register API routes
app.include_router(api_router)


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8050, reload=True)
