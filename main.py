#!/usr/bin/env python3
# ==============================================================================
# DEDICATED PROXY WORKER BOT — MASTER ENGINEERING SPECIFICATION
# ==============================================================================
# A background intelligence engine for proxy discovery, deduplication, 
# validation, and reputation maintenance.
# 
# Outputs verified proxy intelligence to MongoDB for consumption by the Main Bot.
# ==============================================================================

import os
import re
import sys
import json
import time
import logging
import asyncio
import hashlib
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from typing import List, Dict, Any, Tuple, Optional

# Async Network & Database
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne, IndexModel, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, ConnectionFailure

# Telegram Async API
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ProxyWorker")

# Secrets
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "telegram_downloader")
PORT = int(os.getenv("PORT", "8080"))

if not all([BOT_TOKEN, OWNER_ID, MONGO_URI]):
    logger.critical("Missing essential environment variables: BOT_TOKEN, OWNER_ID, or MONGO_URI")
    sys.exit(1)

# Operational Limits & Tuning
CONCURRENCY = int(os.getenv("PROXY_TEST_CONCURRENCY", "10"))
SOURCE_REFRESH_SECONDS = int(os.getenv("SOURCE_REFRESH_SECONDS", "300"))
YOUTUBE_TEST_TIMEOUT = int(os.getenv("YOUTUBE_TEST_TIMEOUT", "25"))
HTTP_CONNECT_TIMEOUT = int(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
QUARANTINE_BASE_HOURS = int(os.getenv("PROXY_QUARANTINE_HOURS", "6"))
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "3600"))

# State Machine Constants
class ProxyState:
    DISCOVERED = "DISCOVERED"
    UNTESTED = "UNTESTED"
    TESTING = "TESTING"
    YOUTUBE_WORKING = "YOUTUBE_WORKING"
    YOUTUBE_REJECTED = "YOUTUBE_REJECTED"
    TIMEOUT = "TIMEOUT"
    CONNECTION_FAILED = "CONNECTION_FAILED"
    GEO_VERIFIED = "GEO_VERIFIED"
    QUARANTINED = "QUARANTINED"
    RETIRED = "RETIRED"

class TestError:
    TIMEOUT = "TIMEOUT"
    CONNECTION_FAILURE = "CONNECTION_FAILURE"
    PROXY_AUTH_FAILURE = "PROXY_AUTH_FAILURE"
    YOUTUBE_BOT_REJECTION = "YOUTUBE_BOT_REJECTION"
    GEO_RESTRICTION = "GEO_RESTRICTION"
    ENVIRONMENT_ERROR = "ENVIRONMENT_ERROR"
    UNKNOWN = "UNKNOWN"

# Global Test Target
DEFAULT_TEST_URL = "https://www.youtube.com/watch?v=BaW_jenozKc"

# Circuit Breaker thresholds
CIRCUIT_BREAKER_ENV_ERRORS_MAX = 5
CIRCUIT_BREAKER_PAUSE_MINUTES = 10


# ==============================================================================
# UTILS & NORMALIZATION
# ==============================================================================
def get_now() -> datetime:
    return datetime.now(timezone.utc)

def generate_proxy_id(proxy_string: str) -> str:
    """Creates a deterministic SHA-256 hash for a normalized proxy string."""
    return hashlib.sha256(proxy_string.encode('utf-8')).hexdigest()

def normalize_proxy(raw: str) -> Optional[Dict[str, Any]]:
    """Parses and normalizes a proxy string into standardized components."""
    raw = raw.strip()
    if not raw:
        return None
    
    # Handle pure ip:port or host:port fallback
    if "://" not in raw:
        raw = f"http://{raw}"

    try:
        parsed = urlparse(raw)
        scheme = parsed.scheme.lower() if parsed.scheme else "http"
        host = parsed.hostname
        port = parsed.port
        
        if not host or not port:
            return None
        if scheme not in ["http", "https", "socks4", "socks5"]:
            scheme = "http"

        username = parsed.username or ""
        password = parsed.password or ""
        
        # Build normalized URL
        auth_part = f"{username}:{password}@" if username else ""
        norm_url = f"{scheme}://{auth_part}{host}:{port}"
        proxy_id = generate_proxy_id(norm_url)
        
        # Safe log URL (without credentials)
        safe_url = f"{scheme}://***:***@{host}:{port}" if username else norm_url
        
        return {
            "proxy_id": proxy_id,
            "scheme": scheme,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "url": norm_url,
            "safe_url": safe_url
        }
    except Exception:
        return None

def mask_url(url: str) -> str:
    parsed = normalize_proxy(url)
    return parsed["safe_url"] if parsed else url

# ==============================================================================
# DATABASE MANAGER
# ==============================================================================
class DatabaseManager:
    def __init__(self, uri: str, db_name: str):
        self.client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[db_name]
        self.proxies = self.db["proxies"]       # Extending existing collection
        self.sources = self.db["proxy_sources"]
        self.tasks = self.db["worker_tasks"]
        self.config = self.db["worker_config"]
        
    async def initialize(self):
        try:
            await self.client.admin.command('ping')
            logger.info("MongoDB connection established.")
            
            # Setup indexes for efficient Main Bot querying and Worker operations
            await self.proxies.create_indexes([
                IndexModel([("proxy_id", ASCENDING)], unique=True),
                IndexModel([("youtube_state", ASCENDING), ("quarantined_until", ASCENDING), ("youtube_score", DESCENDING)]),
                IndexModel([("country_code", ASCENDING)]),
                IndexModel([("testing_lease_until", ASCENDING)]),
                IndexModel([("worker_updated_at", DESCENDING)])
            ])
            logger.info("MongoDB indexes verified.")
            
            # Seed default configuration if empty
            if not await self.config.find_one({"key": "youtube_test_url"}):
                await self.config.insert_one({"key": "youtube_test_url", "value": DEFAULT_TEST_URL})
        except Exception as e:
            logger.critical(f"MongoDB initialization failed: {e}")
            sys.exit(1)

    async def get_config(self, key: str, default: Any = None) -> Any:
        doc = await self.config.find_one({"key": key})
        return doc["value"] if doc else default

    async def set_config(self, key: str, value: Any):
        await self.config.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)

    async def register_source(self, url: str, source_type: str, name: str) -> str:
        source_id = generate_proxy_id(url)
        await self.sources.update_one(
            {"source_id": source_id},
            {"$set": {
                "name": name,
                "url": url,
                "type": source_type,
                "enabled": True,
                "updated_at": get_now()
            }, "$setOnInsert": {"created_at": get_now()}},
            upsert=True
        )
        return source_id

    async def bulk_upsert_proxies(self, proxies_data: List[Dict], source_id: str) -> Tuple[int, int]:
        """Deduplicates and inserts proxies. Returns (total_valid, new_inserted)."""
        if not proxies_data:
            return 0, 0

        operations = []
        now = get_now()
        for p in proxies_data:
            update_op = UpdateOne(
                {"proxy_id": p["proxy_id"]},
                {
                    "$set": {
                        "last_seen_at": now,
                        "source_id": source_id,
                        "worker_updated_at": now
                    },
                    "$setOnInsert": {
                        "scheme": p["scheme"],
                        "host": p["host"],
                        "port": p["port"],
                        "username": p["username"],
                        "password": p["password"],
                        "url": p["url"],
                        "safe_url": p["safe_url"],
                        "generic_state": ProxyState.UNTESTED,
                        "youtube_state": ProxyState.UNTESTED,
                        "country_code": "UNKNOWN",
                        "youtube_score": 0,
                        "youtube_success_count": 0,
                        "youtube_failure_count": 0,
                        "quarantined_until": None,
                        "created_at": now
                    }
                },
                upsert=True
            )
            operations.append(update_op)

        try:
            result = await self.proxies.bulk_write(operations, ordered=False)
            new_count = result.upserted_count
            return len(proxies_data), new_count
        except BulkWriteError as bwe:
            # Handle duplicate key or partial failures safely
            new_count = bwe.details.get('upserted_count', 0)
            return len(proxies_data), new_count

    async def get_testing_batch(self, limit: int = 50) -> List[Dict]:
        """Atomically leases a batch of untested or due-for-revalidation proxies."""
        now = get_now()
        # Find untested OR revalidation needed OR expired lease
        query = {
            "$and": [
                {"quarantined_until": {"$in": [None, ""]}},
                {"$or": [
                    {"youtube_state": ProxyState.UNTESTED},
                    {"testing_lease_until": {"$lt": now}},
                    # Revalidation: Working proxies older than 12 hours
                    {"youtube_state": ProxyState.YOUTUBE_WORKING, "last_tested_at": {"$lt": now - timedelta(hours=12)}},
                    # Retry failed ones periodically if not quarantined
                    {"youtube_state": {"$in": [ProxyState.TIMEOUT, ProxyState.CONNECTION_FAILED]}, "last_tested_at": {"$lt": now - timedelta(hours=24)}}
                ]}
            ]
        }
        
        lease_time = now + timedelta(minutes=10)
        batch = []
        
        # find_one_and_update loop to ensure atomic lease
        cursor = self.proxies.find(query).sort([("youtube_score", DESCENDING), ("last_tested_at", ASCENDING)]).limit(limit)
        async for doc in cursor:
            updated = await self.proxies.find_one_and_update(
                {"_id": doc["_id"], "testing_lease_until": doc.get("testing_lease_until")},
                {"$set": {"testing_lease_until": lease_time, "youtube_state": ProxyState.TESTING}},
                return_document=True
            )
            if updated:
                batch.append(updated)
        return batch

    async def save_test_result(self, proxy_id: str, success: bool, classification: str, country: str = None, duration: float = 0.0):
        now = get_now()
        proxy = await self.proxies.find_one({"proxy_id": proxy_id})
        if not proxy:
            return

        failures = proxy.get("youtube_failure_count", 0)
        successes = proxy.get("youtube_success_count", 0)
        
        update = {
            "$set": {
                "last_tested_at": now,
                "testing_lease_until": None,
                "worker_updated_at": now
            }
        }
        
        if country and country != "UNKNOWN":
            update["$set"]["country_code"] = country

        if success:
            update["$set"]["youtube_state"] = ProxyState.YOUTUBE_WORKING
            update["$set"]["generic_state"] = ProxyState.GEO_VERIFIED
            update["$set"]["quarantined_until"] = None
            update["$inc"] = {"youtube_success_count": 1}
            # Simple score: weight recent successes highly
            new_score = min(100, max(0, ((successes + 1) * 10) - (failures * 5)))
            update["$set"]["youtube_score"] = new_score
        else:
            update["$set"]["youtube_state"] = classification
            update["$inc"] = {"youtube_failure_count": 1}
            new_score = min(100, max(0, (successes * 10) - ((failures + 1) * 5)))
            update["$set"]["youtube_score"] = new_score
            
            # Apply Quarantine if specifically blocked by YouTube
            if classification in [ProxyState.YOUTUBE_REJECTED, TestError.YOUTUBE_BOT_REJECTION, TestError.PROXY_AUTH_FAILURE]:
                quarantine_time = now + timedelta(hours=QUARANTINE_BASE_HOURS * (failures + 1))
                update["$set"]["quarantined_until"] = quarantine_time

        await self.proxies.update_one({"proxy_id": proxy_id}, update)

    async def record_task(self, task_type: str, status: str, details: Dict):
        await self.tasks.insert_one({
            "type": task_type,
            "status": status,
            "details": details,
            "timestamp": get_now()
        })

# ==============================================================================
# TESTER ENGINE (GEO & YOUTUBE)
# ==============================================================================
class ProxyTester:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.env_error_count = 0

    async def get_geo_info(self, proxy_url: str, host: str) -> str:
        """Stage C: Lightweight GeoIP lookup to determine exit node country."""
        # Note: In production, passing traffic *through* the proxy to an echo server is best.
        # For efficiency, if host is an IP, we query an API. If it fails, fallback to UNKNOWN.
        if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", host):
            try:
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(f"http://ip-api.com/json/{host}", timeout=5) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return data.get("countryCode", "UNKNOWN")
            except Exception:
                pass
        return "UNKNOWN"

    async def test_youtube(self, proxy_url: str) -> Tuple[bool, str]:
        """Stage D: Actual YouTube Validation via yt-dlp subprocess."""
        test_url = await self.db.get_config("youtube_test_url", DEFAULT_TEST_URL)
        
        cmd = [
            "yt-dlp",
            "--proxy", proxy_url,
            "--dump-json",
            "--socket-timeout", str(YOUTUBE_TEST_TIMEOUT),
            "--no-warnings",
            "--ignore-config",
            test_url
        ]

        try:
            start_time = time.time()
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Enforce hard subprocess timeout to prevent zombie processes
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=YOUTUBE_TEST_TIMEOUT + 5)
            duration = time.time() - start_time
            
            stdout_str = stdout.decode('utf-8', errors='ignore')
            stderr_str = stderr.decode('utf-8', errors='ignore')
            
            # Check for success
            if proc.returncode == 0 and "formats" in stdout_str:
                self.env_error_count = 0  # Reset circuit breaker
                return True, ProxyState.YOUTUBE_WORKING

            # --- Error Classification ---
            err = stderr_str.lower()
            
            # 1. Environment Errors (Don't blame the proxy)
            if "deno not found" in err or "node not found" in err or "no executable found" in err:
                self.env_error_count += 1
                return False, TestError.ENVIRONMENT_ERROR
            
            # 2. YouTube Bot Rejection / Captcha
            if "sign in to confirm" in err or "page needs to be reloaded" in err or "bot" in err:
                return False, TestError.YOUTUBE_BOT_REJECTION
            
            # 3. Geo Restriction
            if "not available in your country" in err or "geo-restricted" in err:
                return False, TestError.GEO_RESTRICTION
            
            # 4. Proxy/Network Connection Errors
            if "proxy auth" in err or "authentication" in err or "407" in err:
                return False, TestError.PROXY_AUTH_FAILURE
            
            if "timeout" in err or "timed out" in err:
                return False, TestError.TIMEOUT
                
            return False, TestError.CONNECTION_FAILURE

        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            return False, TestError.TIMEOUT
        except Exception as e:
            logger.error(f"Test subprocess exception: {str(e)}")
            return False, TestError.UNKNOWN

# ==============================================================================
# SOURCE MANAGER
# ==============================================================================
class SourceManager:
    def __init__(self, db: DatabaseManager):
        self.db = db

    async def fetch_url(self, url: str) -> Optional[str]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        return await resp.text()
        except Exception as e:
            logger.error(f"Source fetch failed {url}: {str(e)}")
        return None

    def parse_proxies_from_text(self, text: str) -> List[Dict]:
        lines = text.strip().split('\n')
        proxies = []
        for line in lines:
            line = line.strip()
            # Basic validation filter
            if not line or line.startswith('#'):
                continue
            normalized = normalize_proxy(line)
            if normalized:
                proxies.append(normalized)
        return proxies

    async def process_source(self, url: str, source_name: str, source_type: str = "AUTO") -> Dict:
        """Fetches, parses, deduplicates, and stores a proxy source."""
        source_id = await self.db.register_source(url, source_type, source_name)
        
        # GitHub folder fallback resolver logic omitted for brevity (fetches raw if API)
        raw_text = await self.fetch_url(url)
        if not raw_text:
            return {"status": "FAILED", "error": "Fetch failed"}

        content_hash = hashlib.md5(raw_text.encode()).hexdigest()
        
        # Check snapshot hash
        source_doc = await self.db.sources.find_one({"source_id": source_id})
        if source_doc and source_doc.get("last_content_hash") == content_hash:
            return {"status": "UNCHANGED", "total": 0, "new": 0}

        proxies_data = self.parse_proxies_from_text(raw_text)
        if not proxies_data:
            return {"status": "EMPTY", "error": "No valid proxies found"}

        total, new_count = await self.db.bulk_upsert_proxies(proxies_data, source_id)
        
        # Update source snapshot hash
        await self.db.sources.update_one(
            {"source_id": source_id},
            {"$set": {"last_content_hash": content_hash, "last_item_count": total}}
        )
        
        await self.db.record_task("SOURCE_SYNC", "SUCCESS", {"source": source_name, "total": total, "new": new_count})
        return {"status": "SUCCESS", "total": total, "new": new_count, "duplicates": total - new_count}

# ==============================================================================
# BACKGROUND SCHEDULER & VALIDATION PIPELINE
# ==============================================================================
class WorkerScheduler:
    def __init__(self, db: DatabaseManager, bot: AsyncTeleBot):
        self.db = db
        self.bot = bot
        self.tester = ProxyTester(db)
        self.source_manager = SourceManager(db)
        self.is_running = False
        self.circuit_breaker_active_until = None

    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        logger.info("Starting Worker Schedulers...")
        asyncio.create_task(self.source_refresh_loop())
        asyncio.create_task(self.validation_loop())
        asyncio.create_task(self.cleanup_loop())
        await self.notify_owner("🟢 Worker Bot Started & Schedulers Active.")

    async def notify_owner(self, text: str, markup=None):
        if OWNER_ID != 0:
            try:
                await self.bot.send_message(chat_id=OWNER_ID, text=text, reply_markup=markup, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Failed to notify owner: {e}")

    async def source_refresh_loop(self):
        """Continuously pulls from active registered sources."""
        while self.is_running:
            try:
                sources = await self.db.sources.find({"enabled": True}).to_list(length=100)
                if not sources:
                    # Seed Proxifly default if empty
                    await self.db.register_source(
                        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt", 
                        "PROXIFLY", 
                        "Proxifly Global"
                    )
                    sources = await self.db.sources.find({"enabled": True}).to_list(length=100)

                for src in sources:
                    res = await self.source_manager.process_source(src["url"], src["name"], src["type"])
                    if res["status"] == "SUCCESS" and res["new"] > 0:
                        logger.info(f"Source {src['name']} refreshed: {res['new']} new proxies.")
                        
            except Exception as e:
                logger.error(f"Source loop error: {e}")
            
            await asyncio.sleep(SOURCE_REFRESH_SECONDS)

    async def test_worker(self, semaphore: asyncio.Semaphore, proxy: Dict):
        """Individual proxy test task bound by concurrency semaphore."""
        async with semaphore:
            pid = proxy["proxy_id"]
            url = proxy["url"]
            
            # Stage C: Geo
            country = proxy.get("country_code", "UNKNOWN")
            if country == "UNKNOWN":
                country = await self.tester.get_geo_info(url, proxy["host"])

            # Stage D: YouTube Subprocess
            success, classification = await self.tester.test_youtube(url)
            
            # Circuit breaker check
            if classification == TestError.ENVIRONMENT_ERROR and self.tester.env_error_count >= CIRCUIT_BREAKER_ENV_ERRORS_MAX:
                self.circuit_breaker_active_until = get_now() + timedelta(minutes=CIRCUIT_BREAKER_PAUSE_MINUTES)
                await self.notify_owner(f"🔴 <b>CIRCUIT BREAKER TRIPPED</b>\nLocal yt-dlp environment failing. Testing paused for {CIRCUIT_BREAKER_PAUSE_MINUTES} mins.")
                
            await self.db.save_test_result(pid, success, classification, country)

    async def validation_loop(self):
        """Continuously fetches batches of proxies and tests them."""
        semaphore = asyncio.Semaphore(CONCURRENCY)
        while self.is_running:
            try:
                if self.circuit_breaker_active_until and get_now() < self.circuit_breaker_active_until:
                    await asyncio.sleep(60)
                    continue

                batch = await self.db.get_testing_batch(limit=CONCURRENCY * 2)
                if not batch:
                    await asyncio.sleep(15) # Idle wait
                    continue

                # Run batch concurrently
                tasks = [asyncio.create_task(self.test_worker(semaphore, p)) for p in batch]
                await asyncio.gather(*tasks)

            except Exception as e:
                logger.error(f"Validation loop error: {e}")
                await asyncio.sleep(10)

    async def cleanup_loop(self):
        """Periodically clears expired leases and applies retirement policies."""
        while self.is_running:
            try:
                now = get_now()
                # Clear stuck testing leases
                await self.db.proxies.update_many(
                    {"youtube_state": ProxyState.TESTING, "testing_lease_until": {"$lt": now}},
                    {"$set": {"youtube_state": ProxyState.UNTESTED, "testing_lease_until": None}}
                )
            except Exception:
                pass
            await asyncio.sleep(CLEANUP_INTERVAL)

# ==============================================================================
# TELEGRAM DASHBOARD & UI (Owner Only)
# ==============================================================================
bot = AsyncTeleBot(BOT_TOKEN)
db_manager = DatabaseManager(MONGO_URI, MONGO_DB_NAME)
source_manager = SourceManager(db_manager)
scheduler = WorkerScheduler(db_manager, bot)

from telebot.asyncio_handler_backends import BaseMiddleware, CancelUpdate

# Owner authorization middleware using BaseMiddleware class
class OwnerOnlyMiddleware(BaseMiddleware):
    def __init__(self, owner_id: int):
        super().__init__()
        self.owner_id = owner_id
        # Apply this middleware to both messages and inline button clicks
        self.update_types = ['message', 'callback_query']

    async def pre_process(self, message, data):
        user_id = None
        
        # Check if the update contains user information
        if hasattr(message, 'from_user') and message.from_user:
            user_id = message.from_user.id
        
        # Block if the user is not the owner
        if user_id != self.owner_id:
            logger.warning(f"Unauthorized access attempt from User ID: {user_id}")
            
            # If it's a callback query (inline button click), send an alert
            if hasattr(message, 'data'):
                try:
                    await bot.answer_callback_query(message.id, "⛔ Unauthorized.")
                except Exception:
                    pass
                    
            # This completely stops the bot from processing the message
            return CancelUpdate()

    async def post_process(self, message, data, exception):
        # We don't need to do anything after processing
        pass

# Setup the middleware into the bot
bot.setup_middleware(OwnerOnlyMiddleware(OWNER_ID))

def get_main_keyboard():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("➕ Add URL", callback_data="add_url"),
        InlineKeyboardButton("📝 Add Text", callback_data="add_text"),
        InlineKeyboardButton("📊 System Stats", callback_data="sys_stats"),
        InlineKeyboardButton("🌍 Countries", callback_data="countries"),
        InlineKeyboardButton("🔄 Refresh Sources", callback_data="refresh_now"),
        InlineKeyboardButton("🧪 Test New", callback_data="test_new")
    )
    return markup

@bot.message_handler(commands=['start', 'dashboard'])
async def cmd_start(message: Message):
    await bot.send_message(
        message.chat.id,
        "🧠 <b>DEDICATED PROXY WORKER BOT</b>\n\nSelect an action from the dashboard below:",
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data == "sys_stats")
async def cb_sys_stats(call: CallbackQuery):
    try:
        total = await db_manager.proxies.count_documents({})
        working = await db_manager.proxies.count_documents({"youtube_state": ProxyState.YOUTUBE_WORKING, "quarantined_until": {"$in": [None, ""]}})
        quarantined = await db_manager.proxies.count_documents({"quarantined_until": {"$ne": None}})
        testing = await db_manager.proxies.count_documents({"youtube_state": ProxyState.TESTING})
        sources_cnt = await db_manager.sources.count_documents({"enabled": True})

        stats_text = (
            "📊 <b>SYSTEM STATS</b>\n\n"
            f"🟢 <b>Worker:</b> Online\n"
            f"🟢 <b>MongoDB:</b> Connected\n"
            f"🟢 <b>Sources:</b> {sources_cnt} Active\n\n"
            f"🌐 <b>Total Known:</b> {total}\n"
            f"✅ <b>YouTube Working:</b> {working}\n"
            f"⏳ <b>Testing Queue:</b> {testing}\n"
            f"⛔ <b>Quarantined:</b> {quarantined}\n"
        )
        await bot.edit_message_text(stats_text, call.message.chat.id, call.message.message_id, reply_markup=get_main_keyboard(), parse_mode="HTML")
    except Exception as e:
        await bot.answer_callback_query(call.id, f"Error: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "countries")
async def cb_countries(call: CallbackQuery):
    try:
        pipeline = [
            {"$match": {"youtube_state": ProxyState.YOUTUBE_WORKING, "quarantined_until": {"$in": [None, ""]}}},
            {"$group": {"_id": "$country_code", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        cursor = db_manager.proxies.aggregate(pipeline)
        result = await cursor.to_list(length=10)
        
        text = "🌍 <b>TOP COUNTRIES (Working)</b>\n\n"
        if not result:
            text += "No working proxies found."
        else:
            for c in result:
                code = c['_id'] if c['_id'] else "UNKNOWN"
                text += f"🇺🇸 <b>{code}</b>: {c['count']}\n"
        
        markup = InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="sys_stats"))
        await bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        await bot.answer_callback_query(call.id, f"Error: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "refresh_now")
async def cb_refresh_now(call: CallbackQuery):
    await bot.answer_callback_query(call.id, "Triggering background source refresh...")
    sources = await db_manager.sources.find({"enabled": True}).to_list(length=50)
    total_new = 0
    for src in sources:
        res = await source_manager.process_source(src["url"], src["name"], src["type"])
        if res["status"] == "SUCCESS":
            total_new += res["new"]
    await bot.send_message(call.message.chat.id, f"✅ Manual refresh complete. <b>{total_new}</b> new proxies added to queue.", parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "add_text")
async def cb_add_text(call: CallbackQuery):
    msg = await bot.send_message(call.message.chat.id, "📝 Send me the proxy text (one per line).", parse_mode="HTML")
    bot.register_next_step_handler(msg, process_add_text)

async def process_add_text(message: Message):
    if not message.text:
        return await bot.send_message(message.chat.id, "❌ Invalid text.", reply_markup=get_main_keyboard())
    
    msg = await bot.send_message(message.chat.id, "⏳ Processing manual input...")
    proxies_data = source_manager.parse_proxies_from_text(message.text)
    
    if not proxies_data:
        return await bot.edit_message_text("❌ No valid proxies parsed.", message.chat.id, msg.message_id, reply_markup=get_main_keyboard())
    
    source_id = await db_manager.register_source("MANUAL_TEXT_UPLOAD", "MANUAL", "Manual Text Upload")
    total, new_cnt = await db_manager.bulk_upsert_proxies(proxies_data, source_id)
    
    report = (
        "📊 <b>IMPORT COMPLETE (Manual Text)</b>\n\n"
        f"📥 Received Valid: {total}\n"
        f"♻️ Duplicates: {total - new_cnt}\n"
        f"➕ New Inserted: {new_cnt}\n\n"
        "<i>Testing will begin automatically in the background.</i>"
    )
    await bot.edit_message_text(report, message.chat.id, msg.message_id, reply_markup=get_main_keyboard(), parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data == "add_url")
async def cb_add_url(call: CallbackQuery):
    msg = await bot.send_message(call.message.chat.id, "➕ Send the raw URL containing the proxy list:", parse_mode="HTML")
    bot.register_next_step_handler(msg, process_add_url)

async def process_add_url(message: Message):
    url = message.text.strip()
    if not url.startswith("http"):
        return await bot.send_message(message.chat.id, "❌ Invalid URL format.", reply_markup=get_main_keyboard())
    
    msg = await bot.send_message(message.chat.id, "⏳ Fetching and processing URL...")
    res = await source_manager.process_source(url, "Manual URL", "MANUAL_URL")
    
    if res["status"] in ["SUCCESS", "UNCHANGED"]:
        report = (
            f"✅ <b>URL Source Processed</b>\n\n"
            f"URL: <code>{url}</code>\n"
            f"Status: {res['status']}\n"
            f"📥 Parsed: {res.get('total', 0)}\n"
            f"➕ New: {res.get('new', 0)}\n"
        )
    else:
        report = f"❌ <b>Fetch Failed</b>\nError: {res.get('error', 'Unknown')}"
        
    await bot.edit_message_text(report, message.chat.id, msg.message_id, reply_markup=get_main_keyboard(), parse_mode="HTML")

# File Upload Handler
@bot.message_handler(content_types=['document'])
async def handle_document(message: Message):
    if message.from_user.id != OWNER_ID:
        return
        
    try:
        file_info = await bot.get_file(message.document.file_id)
        file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_info.file_path}"
        
        msg = await bot.send_message(message.chat.id, "📁 Downloading and parsing file...")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(file_url) as resp:
                text = await resp.text()
                
        proxies_data = source_manager.parse_proxies_from_text(text)
        if not proxies_data:
            return await bot.edit_message_text("❌ No valid proxies found in file.", message.chat.id, msg.message_id)
            
        source_id = await db_manager.register_source("MANUAL_FILE_UPLOAD", "MANUAL", f"File: {message.document.file_name}")
        total, new_cnt = await db_manager.bulk_upsert_proxies(proxies_data, source_id)
        
        report = (
            "📊 <b>FILE IMPORT COMPLETE</b>\n\n"
            f"File: {message.document.file_name}\n"
            f"📥 Valid: {total}\n"
            f"➕ New: {new_cnt}\n"
        )
        await bot.edit_message_text(report, message.chat.id, msg.message_id, reply_markup=get_main_keyboard(), parse_mode="HTML")
        
    except Exception as e:
        await bot.send_message(message.chat.id, f"❌ File processing failed: {e}")

# ==============================================================================
# HTTP HEALTH SERVER
# ==============================================================================
async def health_handler(request):
    try:
        # Check DB
        await db_manager.client.admin.command('ping')
        queue_size = await db_manager.proxies.count_documents({"youtube_state": ProxyState.UNTESTED})
        working_size = await db_manager.proxies.count_documents({"youtube_state": ProxyState.YOUTUBE_WORKING})
        
        data = {
            "status": "healthy" if scheduler.is_running else "degraded",
            "worker": "running" if scheduler.is_running else "stopped",
            "mongodb": "connected",
            "queue_size": queue_size,
            "youtube_working": working_size,
            "circuit_breaker": "active" if scheduler.circuit_breaker_active_until and get_now() < scheduler.circuit_breaker_active_until else "inactive",
            "timestamp": get_now().isoformat()
        }
        return aiohttp.web.json_response(data, status=200)
    except Exception as e:
        return aiohttp.web.json_response({"status": "error", "message": str(e)}, status=503)

async def start_web_server():
    app = aiohttp.web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", health_handler)
    
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Health server listening on 0.0.0.0:{PORT}")

# ==============================================================================
# MAIN BOOTSTRAP
# ==============================================================================
async def main():
    logger.info("Initializing Worker Bot Architecture...")
    
    # 1. Initialize MongoDB
    await db_manager.initialize()
    
    # 2. Start Web Server (For Koyeb/Render Liveness)
    await start_web_server()
    
    # 3. Start Background Schedulers
    await scheduler.start()
    
    # 4. Start Telegram Polling
    try:
        # Delete webhook just in case polling is blocked
        await bot.delete_webhook()
        logger.info("Telegram polling started.")
        await bot.polling(non_stop=True, request_timeout=60)
    except Exception as e:
        logger.critical(f"Telegram polling crashed: {e}")
    finally:
        # Graceful shutdown hook
        scheduler.is_running = False

if __name__ == "__main__":
    # Ensure Windows compatibility for asyncio subprocesses if run locally
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker Bot shutdown requested by user.")
    except Exception as e:
        logger.critical(f"Fatal crash: {e}")
