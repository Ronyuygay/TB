#!/usr/bin/env python3
"""
Proxy Worker Bot — Complete Implementation
===========================================
A dedicated proxy intelligence worker that discovers, normalizes, deduplicates,
persists, tests (generic + YouTube), scores, quarantines, and refreshes proxies.
It stores all verified proxy data in MongoDB for consumption by the Main Bot.
This bot does NOT download media or handle end-user requests.

Author: Production Engineering
"""

import asyncio
import aiohttp
import aiohttp.web
import asyncio.subprocess
import base64
import csv
import hashlib
import io
import json
import logging
import os
import re
import signal
import sys
import time
import urllib.parse
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# Third-party imports
import motor.motor_asyncio  # PyMongo Async
from telebot.async_telebot import AsyncTeleBot
from telebot import types as tgtypes
import yt_dlp  # only for version, actual test via subprocess

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger("ProxyWorker")


# ======================================================================
# CONFIGURATION
# ======================================================================
class Config:
    """Central configuration loaded from environment variables."""
    def __init__(self):
        self.BOT_TOKEN = os.getenv("BOT_TOKEN", "")
        self.OWNER_ID = int(os.getenv("OWNER_ID", "0"))
        self.MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "telegram_downloader")

        # HTTP server
        self.PORT = int(os.getenv("PORT", "8080"))
        self.HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
        self.WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # optional

        # Test limits
        self.PROXY_TEST_CONCURRENCY = int(os.getenv("PROXY_TEST_CONCURRENCY", "5"))
        self.YOUTUBE_TEST_TIMEOUT = int(os.getenv("YOUTUBE_TEST_TIMEOUT", "30"))
        self.HTTP_CONNECT_TIMEOUT = int(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
        self.SOURCE_REFRESH_SECONDS = int(os.getenv("SOURCE_REFRESH_SECONDS", "300"))
        self.DAILY_REPORT_HOUR = int(os.getenv("DAILY_REPORT_HOUR", "9"))
        self.YOUTUBE_MAX_FORMAT_CHECKS = int(os.getenv("YOUTUBE_MAX_FORMAT_CHECKS", "3"))
        self.PROXY_QUARANTINE_HOURS = int(os.getenv("PROXY_QUARANTINE_HOURS", "6"))
        self.MAX_PROXIES_PER_REFRESH = int(os.getenv("MAX_PROXIES_PER_REFRESH", "1000"))
        self.CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "3600"))
        self.REPORT_CHAT_ID = os.getenv("REPORT_CHAT_ID", "")  # defaults to owner
        self.MAX_PENDING_TESTS = int(os.getenv("MAX_PENDING_TESTS", "500"))
        self.TEST_ENGINE_VERSION = int(os.getenv("TEST_ENGINE_VERSION", "1"))

        # YouTube test URL (can be overridden in DB settings)
        self.DEFAULT_YOUTUBE_TEST_URL = "https://www.youtube.com/watch?v=jNQXAC9IVRw"
        self.YOUTUBE_TEST_URL = os.getenv("YOUTUBE_TEST_URL", self.DEFAULT_YOUTUBE_TEST_URL)

        # Internal state
        self.mongo_client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
        self.db = None
        self.bot: Optional[AsyncTeleBot] = None
        self.aiohttp_session: Optional[aiohttp.ClientSession] = None

    def validate(self) -> bool:
        if not self.BOT_TOKEN:
            logger.critical("BOT_TOKEN is missing")
            return False
        if self.OWNER_ID == 0:
            logger.critical("OWNER_ID is missing or invalid")
            return False
        if not self.MONGO_URI:
            logger.critical("MONGO_URI is missing")
            return False
        return True


config = Config()


# ======================================================================
# CONSTANTS AND STATE MACHINE
# ======================================================================
class ProxyState:
    DISCOVERED = "DISCOVERED"
    NORMALIZED = "NORMALIZED"
    UNTESTED = "UNTESTED"
    TESTING = "TESTING"
    GENERIC_WORKING = "GENERIC_WORKING"
    YOUTUBE_WORKING = "YOUTUBE_WORKING"
    YOUTUBE_REJECTED = "YOUTUBE_REJECTED"
    TIMEOUT = "TIMEOUT"
    CONNECTION_FAILED = "CONNECTION_FAILED"
    GEO_VERIFIED = "GEO_VERIFIED"
    QUARANTINED = "QUARANTINED"
    RETIRED = "RETIRED"
    DISABLED = "DISABLED"


class SourceType:
    GITHUB = "GITHUB"
    PROXIFLY = "PROXIFLY"
    RAW_TXT = "RAW_TXT"
    MANUAL_URL = "MANUAL_URL"
    MANUAL_TEXT = "MANUAL_TEXT"
    MANUAL_FILE = "MANUAL_FILE"
    API = "API"
    OTHER = "OTHER"


class TaskStatus:
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class YoutubeResult:
    SUCCESS = "SUCCESS"
    TIMEOUT = "TIMEOUT"
    CONNECTION_FAILURE = "CONNECTION_FAILURE"
    PROXY_AUTH_FAILURE = "PROXY_AUTH_FAILURE"
    YOUTUBE_BOT_REJECTION = "YOUTUBE_BOT_REJECTION"
    YOUTUBE_RELOAD_REJECTION = "YOUTUBE_RELOAD_REJECTION"
    GEO_RESTRICTION = "GEO_RESTRICTION"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    YT_DLP_ERROR = "YT_DLP_ERROR"
    JS_RUNTIME_ERROR = "JS_RUNTIME_ERROR"
    UNKNOWN = "UNKNOWN"


# Valid transitions for state machine (simplified, enforced in code)
VALID_TRANSITIONS = {
    ProxyState.DISCOVERED: {ProxyState.NORMALIZED, ProxyState.UNTESTED},
    ProxyState.NORMALIZED: {ProxyState.UNTESTED, ProxyState.DISABLED},
    ProxyState.UNTESTED: {ProxyState.TESTING, ProxyState.DISABLED},
    ProxyState.TESTING: {
        ProxyState.GENERIC_WORKING,
        ProxyState.YOUTUBE_WORKING,
        ProxyState.YOUTUBE_REJECTED,
        ProxyState.TIMEOUT,
        ProxyState.CONNECTION_FAILED,
        ProxyState.GEO_VERIFIED,
        ProxyState.QUARANTINED,
        ProxyState.DISABLED,
    },
    ProxyState.GENERIC_WORKING: {
        ProxyState.TESTING,
        ProxyState.YOUTUBE_WORKING,
        ProxyState.YOUTUBE_REJECTED,
        ProxyState.QUARANTINED,
        ProxyState.DISABLED,
    },
    ProxyState.YOUTUBE_WORKING: {
        ProxyState.TESTING,  # revalidation
        ProxyState.QUARANTINED,
        ProxyState.DISABLED,
        ProxyState.RETIRED,
    },
    ProxyState.YOUTUBE_REJECTED: {
        ProxyState.TESTING,
        ProxyState.QUARANTINED,
        ProxyState.DISABLED,
        ProxyState.RETIRED,
    },
    ProxyState.TIMEOUT: {
        ProxyState.UNTESTED,  # retry later
        ProxyState.TESTING,
        ProxyState.DISABLED,
    },
    ProxyState.CONNECTION_FAILED: {
        ProxyState.UNTESTED,
        ProxyState.TESTING,
        ProxyState.DISABLED,
    },
    ProxyState.GEO_VERIFIED: {
        ProxyState.TESTING,
        ProxyState.YOUTUBE_WORKING,
        ProxyState.DISABLED,
    },
    ProxyState.QUARANTINED: {
        ProxyState.TESTING,
        ProxyState.YOUTUBE_WORKING,
        ProxyState.UNTESTED,
        ProxyState.DISABLED,
        ProxyState.RETIRED,
    },
    ProxyState.RETIRED: {
        ProxyState.UNTESTED,
        ProxyState.DISABLED,
    },
    ProxyState.DISABLED: {
        ProxyState.UNTESTED,
        ProxyState.RETIRED,
    },
}


# ======================================================================
# UTILITY FUNCTIONS
# ======================================================================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_proxy_string(proxy_str: str) -> Optional[Dict[str, Any]]:
    """
    Normalize a proxy string into a dict with scheme, host, port, username, password.
    Returns None if invalid.
    """
    if not proxy_str or not isinstance(proxy_str, str):
        return None
    proxy_str = proxy_str.strip()
    if not proxy_str:
        return None

    # Remove surrounding quotes
    if len(proxy_str) >= 2 and proxy_str[0] == proxy_str[-1] and proxy_str[0] in ("'", '"'):
        proxy_str = proxy_str[1:-1]

    # Default scheme if missing
    scheme = None
    if "://" in proxy_str:
        scheme_part, rest = proxy_str.split("://", 1)
        scheme = scheme_part.lower()
        if scheme not in ("http", "https", "socks4", "socks5"):
            return None
        proxy_str = rest
    else:
        # assume http
        scheme = "http"

    # Check for userinfo
    username = None
    password = None
    if "@" in proxy_str:
        userinfo, hostport = proxy_str.rsplit("@", 1)
        if ":" in userinfo:
            username, password = userinfo.split(":", 1)
        else:
            username = userinfo
        proxy_str = hostport

    # Parse host:port
    if ":" not in proxy_str:
        return None
    host, port_str = proxy_str.rsplit(":", 1)
    if not host or not port_str:
        return None
    try:
        port = int(port_str)
        if not (1 <= port <= 65535):
            return None
    except ValueError:
        return None

    # Validate host (basic)
    host = host.strip()
    if not host:
        return None
    # Remove brackets if IPv6
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]

    return {
        "scheme": scheme,
        "host": host,
        "port": port,
        "username": username,
        "password": password,
    }


def generate_proxy_id(normalized: Dict[str, Any]) -> str:
    """Generate deterministic SHA-256 hash as proxy ID."""
    key = f"{normalized['scheme']}://{normalized['username'] or ''}:{normalized['password'] or ''}@{normalized['host']}:{normalized['port']}"
    return hashlib.sha256(key.encode('utf-8')).hexdigest()


def proxy_to_url(normalized: Dict[str, Any], include_credentials=False) -> str:
    """Convert normalized proxy dict to URL string."""
    scheme = normalized['scheme']
    auth = ""
    if include_credentials and normalized.get('username'):
        auth = f"{normalized['username']}"
        if normalized.get('password'):
            auth += f":{normalized['password']}"
        auth += "@"
    host = normalized['host']
    if ':' in host and not host.startswith('['):
        host = f"[{host}]"
    return f"{scheme}://{auth}{host}:{normalized['port']}"


def mask_proxy_url(url: str) -> str:
    """Mask credentials in proxy URL for logging."""
    if "://" in url:
        scheme, rest = url.split("://", 1)
        if "@" in rest:
            userinfo, hostport = rest.rsplit("@", 1)
            # mask username/password
            if ":" in userinfo:
                user, pwd = userinfo.split(":", 1)
                masked = f"{user[:2]}***:***"
            else:
                masked = f"{userinfo[:2]}***"
            return f"{scheme}://{masked}@{hostport}"
        return url
    return url


def parse_proxy_list_text(text: str) -> List[str]:
    """Parse a text blob into list of proxy strings (one per line)."""
    if not text:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


def parse_csv_proxies(data: str) -> List[str]:
    """Extract proxy strings from CSV content."""
    proxies = []
    try:
        reader = csv.reader(io.StringIO(data))
        for row in reader:
            for cell in row:
                cell = cell.strip()
                if cell and ("://" in cell or ":" in cell):
                    proxies.append(cell)
    except csv.Error:
        pass
    return proxies


def parse_json_proxies(data: str) -> List[str]:
    """Extract proxy strings from JSON structures."""
    proxies = []
    try:
        obj = json.loads(data)
    except json.JSONDecodeError:
        return []
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, str):
                proxies.append(item)
            elif isinstance(item, dict):
                # try common fields
                for key in ("proxy", "url", "host", "ip"):
                    if key in item and isinstance(item[key], str):
                        proxies.append(item[key])
    elif isinstance(obj, dict):
        # try to find arrays
        for key, value in obj.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        proxies.append(item)
                    elif isinstance(item, dict):
                        for k2 in ("proxy", "url", "host", "ip"):
                            if k2 in item and isinstance(item[k2], str):
                                proxies.append(item[k2])
    return proxies


def extract_country_from_filename(url: str) -> Optional[str]:
    """Attempt to extract country code from GitHub URL path."""
    match = re.search(r'/proxies/countries/([A-Za-z]{2})/', url)
    if match:
        return match.group(1).upper()
    return None


def parse_proxies_from_url_content(content: str, url: str = "") -> Tuple[List[Dict], int]:
    """
    Parse content from a proxy source URL into list of normalized proxies.
    Returns (list_of_normalized_dict, invalid_count).
    """
    # Try to detect format: raw txt, csv, json
    proxies_raw = []
    content = content.strip()
    if not content:
        return [], 0

    # Attempt JSON
    if content.startswith("[") or content.startswith("{"):
        proxies_raw = parse_json_proxies(content)
        if proxies_raw:
            # validate each
            normalized_list = []
            invalid = 0
            for p in proxies_raw:
                norm = normalize_proxy_string(p)
                if norm:
                    normalized_list.append(norm)
                else:
                    invalid += 1
            return normalized_list, invalid

    # Attempt CSV: detect if many commas
    if "," in content and "\n" in content:
        proxies_raw = parse_csv_proxies(content)
    else:
        proxies_raw = parse_proxy_list_text(content)

    normalized_list = []
    invalid = 0
    for p in proxies_raw:
        norm = normalize_proxy_string(p)
        if norm:
            normalized_list.append(norm)
        else:
            invalid += 1
    return normalized_list, invalid


# ======================================================================
# MONGODB HELPERS
# ======================================================================
async def get_db():
    if config.db is None:
        config.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(config.MONGO_URI)
        config.db = config.mongo_client[config.MONGO_DB_NAME]
        await ensure_indexes()
    return config.db


async def ensure_indexes():
    db = config.db
    # Proxies collection indexes (no unique host:port index)
    await db.proxies.create_index([("state", 1), ("quarantine_until", 1)])
    await db.proxies.create_index([("country_code", 1), ("youtube_score", -1)])
    await db.proxies.create_index([("host", 1), ("port", 1)])   # non-unique
    await db.proxies.create_index([("last_seen_at", -1)])
    await db.proxies.create_index([("source_id", 1)])
    # ... rest of indexes remain unchanged
    await db.proxy_sources.create_index("source_id", unique=True)
    await db.source_snapshots.create_index([("source_id", 1), ("fetched_at", -1)])
    await db.task_runs.create_index("task_id", unique=True)
    await db.task_runs.create_index([("status", 1), ("started_at", -1)])
    await db.proxy_events.create_index([("proxy_id", 1), ("created_at", -1)])
    await db.worker_settings.create_index("key", unique=True)
    await db.worker_state.create_index("key", unique=True)
    await db.proxy_feedback.create_index([("proxy_id", 1), ("created_at", -1)])


# ======================================================================
# PROXY REPOSITORY
# ======================================================================
class ProxyRepository:
    """Handles database operations for proxies."""
    def __init__(self, db):
        self.db = db
        self.proxies = db.proxies

    async def upsert_new_proxy(self, normalized: Dict[str, Any], source_id: str,
                               source_country: Optional[str] = None,
                               source_protocol: Optional[str] = None) -> Tuple[str, bool]:
        """
        Insert a new proxy if not exists. Returns (proxy_id, is_new).
        """
        proxy_id = generate_proxy_id(normalized)
        existing = await self.proxies.find_one({"_id": proxy_id})
        if existing:
            # Update last_seen and source info, but don't change state
            await self.proxies.update_one(
                {"_id": proxy_id},
                {
                    "$set": {
                        "last_seen_at": utcnow(),
                        "source_id": source_id,
                        "source_country": source_country if source_country else existing.get("source_country"),
                        "source_protocol": source_protocol if source_protocol else existing.get("source_protocol"),
                        "source_missing": False,  # if it reappears
                    }
                }
            )
            return proxy_id, False

        now = utcnow()
        doc = {
            "_id": proxy_id,
            "scheme": normalized["scheme"],
            "host": normalized["host"],
            "port": normalized["port"],
            "username": normalized.get("username"),
            "password": normalized.get("password"),  # stored but never exposed
            "state": ProxyState.UNTESTED,
            "created_at": now,
            "last_seen_at": now,
            "source_id": source_id,
            "source_country": source_country,
            "source_protocol": source_protocol or normalized["scheme"],
            "country_code": None,
            "country_name": None,
            "country_source": None,
            "country_verified_at": None,
            "generic_health": None,
            "youtube_health": None,
            "youtube_score": 0.0,
            "youtube_success_count": 0,
            "youtube_failure_count": 0,
            "youtube_consecutive_failures": 0,
            "youtube_last_success_at": None,
            "youtube_last_failure_at": None,
            "youtube_banned_until": None,
            "quarantine_until": None,
            "latency_ms": None,
            "last_test_duration": None,
            "formats_found": None,
            "tested_at": None,
            "yt_dlp_version": None,
            "test_engine_version": config.TEST_ENGINE_VERSION,
            "enabled": True,
            "source_missing": False,
            "retired_at": None,
            "retirement_reason": None,
            "last_activity_at": now,
        }
        try:
            await self.proxies.insert_one(doc)
            return proxy_id, True
        except Exception as e:
            logger.error(f"Failed to insert proxy {proxy_id}: {e}")
            # Race condition: another inserted? Try again find
            existing = await self.proxies.find_one({"_id": proxy_id})
            if existing:
                await self.proxies.update_one(
                    {"_id": proxy_id},
                    {"$set": {"last_seen_at": utcnow(), "source_missing": False}}
                )
                return proxy_id, False
            raise

    async def get_proxy_by_id(self, proxy_id: str) -> Optional[Dict]:
        return await self.proxies.find_one({"_id": proxy_id})

    async def get_proxy_for_testing(self, proxy_id: str) -> Optional[Dict]:
        # Atomic claim using lease
        now = utcnow()
        result = await self.proxies.find_one_and_update(
            {
                "_id": proxy_id,
                "state": {"$in": [ProxyState.UNTESTED, ProxyState.TIMEOUT, ProxyState.CONNECTION_FAILED, ProxyState.YOUTUBE_REJECTED, ProxyState.GENERIC_WORKING]},
                "$or": [
                    {"testing_lease_until": {"$exists": False}},
                    {"testing_lease_until": None},
                    {"testing_lease_until": {"$lt": now}},
                ],
                "enabled": True,
            },
            {
                "$set": {
                    "state": ProxyState.TESTING,
                    "testing_lease_until": now + timedelta(seconds=120),  # 2 min lease
                    "testing_by": "worker",
                    "testing_started_at": now,
                }
            },
            return_document=True
        )
        return result

    async def release_test_lease(self, proxy_id: str):
        await self.proxies.update_one(
            {"_id": proxy_id},
            {"$set": {"testing_lease_until": None, "testing_by": None, "testing_started_at": None}}
        )

    async def update_proxy_test_result(self, proxy_id: str, result: Dict[str, Any], state: str,
                                       generic_health: Optional[bool] = None,
                                       youtube_health: Optional[bool] = None,
                                       country_code: Optional[str] = None,
                                       country_name: Optional[str] = None,
                                       country_source: Optional[str] = None,
                                       latency_ms: Optional[int] = None,
                                       test_duration: Optional[float] = None,
                                       formats_found: Optional[int] = None,
                                       yt_dlp_version: Optional[str] = None,
                                       error_category: Optional[str] = None,
                                       error_details: Optional[str] = None):
        """Update proxy after a test attempt."""
        now = utcnow()
        update = {
            "$set": {
                "state": state,
                "tested_at": now,
                "last_activity_at": now,
                "testing_lease_until": None,
                "testing_by": None,
                "testing_started_at": None,
                "test_engine_version": config.TEST_ENGINE_VERSION,
            },
            "$inc": {
                "test_count": 1,
            }
        }
        if generic_health is not None:
            update["$set"]["generic_health"] = generic_health
        if youtube_health is not None:
            update["$set"]["youtube_health"] = youtube_health
        if country_code:
            update["$set"]["country_code"] = country_code
            update["$set"]["country_name"] = country_name
            update["$set"]["country_source"] = country_source or "EXIT_IP"
            update["$set"]["country_verified_at"] = now
        if latency_ms is not None:
            update["$set"]["latency_ms"] = latency_ms
        if test_duration is not None:
            update["$set"]["last_test_duration"] = test_duration
        if formats_found is not None:
            update["$set"]["formats_found"] = formats_found
        if yt_dlp_version:
            update["$set"]["yt_dlp_version"] = yt_dlp_version
        if error_category:
            update["$set"]["last_error_category"] = error_category
        if error_details:
            update["$set"]["last_error_details"] = error_details[:500]  # limit

        # Scoring and counters based on result
        if state == ProxyState.YOUTUBE_WORKING:
            update["$inc"]["youtube_success_count"] = 1
            update["$set"]["youtube_consecutive_failures"] = 0
            update["$set"]["youtube_last_success_at"] = now
            update["$set"]["youtube_banned_until"] = None
            # Score boost
            update["$inc"]["youtube_score"] = 10.0
        elif state in [ProxyState.YOUTUBE_REJECTED, ProxyState.TIMEOUT, ProxyState.CONNECTION_FAILED]:
            update["$inc"]["youtube_failure_count"] = 1
            update["$inc"]["youtube_consecutive_failures"] = 1
            update["$set"]["youtube_last_failure_at"] = now
            update["$inc"]["youtube_score"] = -5.0
            if state == ProxyState.YOUTUBE_REJECTED:
                # Quarantine maybe
                quarantine_hours = config.PROXY_QUARANTINE_HOURS
                quarantine_until = now + timedelta(hours=quarantine_hours)
                update["$set"]["quarantine_until"] = quarantine_until
                update["$set"]["state"] = ProxyState.QUARANTINED

        await self.proxies.update_one({"_id": proxy_id}, update)

    async def mark_source_missing(self, proxy_ids: List[str]):
        if proxy_ids:
            await self.proxies.update_many(
                {"_id": {"$in": proxy_ids}},
                {"$set": {"source_missing": True}}
            )

    async def retire_missing_proxies(self, older_than_hours: int = 24):
        """Retire proxies that have been source_missing for too long."""
        threshold = utcnow() - timedelta(hours=older_than_hours)
        result = await self.proxies.update_many(
            {"source_missing": True, "last_seen_at": {"$lt": threshold}, "state": {"$ne": ProxyState.RETIRED}},
            {"$set": {"state": ProxyState.RETIRED, "retired_at": utcnow(), "retirement_reason": "source_missing"}}
        )
        return result.modified_count

    async def find_working_proxies(self, country_code: Optional[str] = None,
                                   limit: int = 50) -> List[Dict]:
        query = {
            "state": ProxyState.YOUTUBE_WORKING,
            "enabled": True,
            "quarantine_until": None,
            "youtube_last_success_at": {"$ne": None},
        }
        if country_code:
            query["country_code"] = country_code
        cursor = self.proxies.find(query).sort([("youtube_score", -1), ("youtube_last_success_at", -1)]).limit(limit)
        return await cursor.to_list(length=limit)

    async def get_stats(self) -> Dict[str, Any]:
        pipeline = [
            {"$group": {
                "_id": "$state",
                "count": {"$sum": 1},
            }}
        ]
        state_counts = {}
        async for doc in self.proxies.aggregate(pipeline):
            state_counts[doc["_id"]] = doc["count"]

        total = sum(state_counts.values())
        working = state_counts.get(ProxyState.YOUTUBE_WORKING, 0)
        quarantined = state_counts.get(ProxyState.QUARANTINED, 0)
        untested = state_counts.get(ProxyState.UNTESTED, 0)
        testing = state_counts.get(ProxyState.TESTING, 0)
        retired = state_counts.get(ProxyState.RETIRED, 0)
        disabled = state_counts.get(ProxyState.DISABLED, 0)

        # country stats
        country_pipeline = [
            {"$match": {"country_code": {"$ne": None}}},
            {"$group": {"_id": "$country_code", "count": {"$sum": 1},
                        "working": {"$sum": {"$cond": [{"$eq": ["$state", ProxyState.YOUTUBE_WORKING]}, 1, 0]}}}},
        ]
        countries = {}
        async for doc in self.proxies.aggregate(country_pipeline):
            countries[doc["_id"]] = {"total": doc["count"], "working": doc["working"]}

        return {
            "total": total,
            "working": working,
            "quarantined": quarantined,
            "untested": untested,
            "testing": testing,
            "retired": retired,
            "disabled": disabled,
            "countries": countries,
            "state_counts": state_counts,
        }

    async def find_proxy_by_host_port(self, host: str, port: int) -> Optional[Dict]:
        return await self.proxies.find_one({"host": host, "port": port})

    async def get_recently_successful_due_for_revalidation(self, limit: int = 10) -> List[Dict]:
        threshold = utcnow() - timedelta(hours=config.PROXY_QUARANTINE_HOURS)
        cursor = self.proxies.find({
            "state": ProxyState.YOUTUBE_WORKING,
            "youtube_last_success_at": {"$lt": threshold},
            "enabled": True,
        }).sort("youtube_last_success_at", 1).limit(limit)
        return await cursor.to_list(length=limit)

    async def get_untested_proxies(self, limit: int = 50) -> List[Dict]:
        cursor = self.proxies.find({
            "state": {"$in": [ProxyState.UNTESTED, ProxyState.TIMEOUT, ProxyState.CONNECTION_FAILED]},
            "enabled": True,
        }).sort("created_at", 1).limit(limit)
        return await cursor.to_list(length=limit)

    async def force_retest_proxy(self, proxy_id: str):
        await self.proxies.update_one(
            {"_id": proxy_id},
            {"$set": {"state": ProxyState.UNTESTED, "quarantine_until": None}}
        )

    async def disable_proxy(self, proxy_id: str):
        await self.proxies.update_one({"_id": proxy_id}, {"$set": {"enabled": False}})

    async def enable_proxy(self, proxy_id: str):
        await self.proxies.update_one({"_id": proxy_id}, {"$set": {"enabled": True}})

    async def quarantine_proxy(self, proxy_id: str, hours: int):
        until = utcnow() + timedelta(hours=hours)
        await self.proxies.update_one(
            {"_id": proxy_id},
            {"$set": {"state": ProxyState.QUARANTINED, "quarantine_until": until}}
        )

    async def clear_quarantine(self, proxy_id: str):
        await self.proxies.update_one(
            {"_id": proxy_id},
            {"$set": {"state": ProxyState.UNTESTED, "quarantine_until": None}}
        )

    async def add_feedback_event(self, proxy_id: str, event_type: str, success: bool,
                                 error_category: Optional[str] = None, job_id: Optional[str] = None,
                                 platform: str = "main_bot"):
        doc = {
            "proxy_id": proxy_id,
            "event_type": event_type,
            "success": success,
            "error_category": error_category,
            "job_id": job_id,
            "platform": platform,
            "created_at": utcnow(),
        }
        await config.db.proxy_feedback.insert_one(doc)
        # Update proxy counters
        if success:
            await self.proxies.update_one(
                {"_id": proxy_id},
                {"$inc": {"real_success_count": 1, "youtube_success_count": 1},
                 "$set": {"real_last_success_at": utcnow(), "youtube_consecutive_failures": 0}}
            )
        else:
            await self.proxies.update_one(
                {"_id": proxy_id},
                {"$inc": {"real_failure_count": 1, "youtube_failure_count": 1},
                 "$set": {"real_last_failure_at": utcnow()}}
            )


# ======================================================================
# SOURCE MANAGER
# ======================================================================
class ProxySourceManager:
    """Fetches and processes proxy sources."""
    def __init__(self, db, session: aiohttp.ClientSession):
        self.db = db
        self.sources = db.proxy_sources
        self.snapshots = db.source_snapshots
        self.session = session

    async def initialize_default_sources(self):
        """Ensure default Proxifly sources exist."""
        default_sources = [
            {
                "source_id": "proxifly_all",
                "name": "Proxifly All",
                "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt",
                "source_type": SourceType.PROXIFLY,
                "enabled": True,
                "country": None,
                "protocol": None,
                "last_checked_at": None,
                "last_success_at": None,
                "last_failure_at": None,
                "last_content_hash": None,
                "last_item_count": 0,
                "fetch_interval": config.SOURCE_REFRESH_SECONDS,
                "priority": 100,
                "backoff_count": 0,
                "quality_score": 0.0,
                "created_at": utcnow(),
            },
        ]
        # Add some country-specific ones (optional, can be added manually)
        # We'll include a few common countries for demonstration.
        common_countries = ["US", "GB", "DE", "FR", "CA", "IN"]
        for cc in common_countries:
            default_sources.append({
                "source_id": f"proxifly_{cc.lower()}",
                "name": f"Proxifly {cc}",
                "url": f"https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/countries/{cc}/data.txt",
                "source_type": SourceType.PROXIFLY,
                "enabled": True,
                "country": cc,
                "protocol": None,
                "last_checked_at": None,
                "last_success_at": None,
                "last_failure_at": None,
                "last_content_hash": None,
                "last_item_count": 0,
                "fetch_interval": config.SOURCE_REFRESH_SECONDS,
                "priority": 100,
                "backoff_count": 0,
                "quality_score": 0.0,
                "created_at": utcnow(),
            })

        for src in default_sources:
            await self.sources.update_one(
                {"source_id": src["source_id"]},
                {"$setOnInsert": src},
                upsert=True
            )

    async def get_enabled_sources(self) -> List[Dict]:
        cursor = self.sources.find({"enabled": True}).sort("priority", -1)
        return await cursor.to_list(length=100)

    async def fetch_source_content(self, source: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Fetch content of a source. Returns (content, error)."""
        url = source["url"]
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    return None, f"HTTP {resp.status}"
                content = await resp.text()
                return content, None
        except asyncio.TimeoutError:
            return None, "timeout"
        except aiohttp.ClientError as e:
            return None, str(e)
        except Exception as e:
            return None, str(e)

    async def process_source(self, source: Dict, manual: bool = False) -> Dict[str, Any]:
        """
        Fetch, parse, dedup, and persist new proxies from a source.
        Returns summary dict.
        """
        source_id = source["source_id"]
        summary = {
            "source_id": source_id,
            "fetched": False,
            "total_fetched": 0,
            "valid": 0,
            "duplicates": 0,
            "invalid": 0,
            "new": 0,
            "already_known": 0,
            "removed": 0,
            "content_hash": None,
            "error": None,
        }

        content, error = await self.fetch_source_content(source)
        if error:
            summary["error"] = error
            await self.sources.update_one(
                {"source_id": source_id},
                {"$set": {"last_failure_at": utcnow(), "backoff_count": source.get("backoff_count", 0) + 1}}
            )
            return summary

        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        summary["content_hash"] = content_hash
        summary["fetched"] = True

        # Check if unchanged
        if source.get("last_content_hash") == content_hash:
            summary["skipped"] = "unchanged"
            await self.sources.update_one(
                {"source_id": source_id},
                {"$set": {"last_checked_at": utcnow()}}
            )
            return summary

        # Parse proxies
        normalized_proxies, invalid_count = parse_proxies_from_url_content(content, source["url"])
        summary["total_fetched"] = len(normalized_proxies) + invalid_count
        summary["valid"] = len(normalized_proxies)
        summary["invalid"] = invalid_count

        # Deduplicate and insert new
        repo = ProxyRepository(self.db)
        new_count = 0
        duplicate_count = 0
        known_proxy_ids = set()
        for norm in normalized_proxies:
            proxy_id, is_new = await repo.upsert_new_proxy(
                norm,
                source_id=source_id,
                source_country=source.get("country") or extract_country_from_filename(source["url"]),
                source_protocol=source.get("protocol") or norm["scheme"]
            )
            known_proxy_ids.add(proxy_id)
            if is_new:
                new_count += 1
            else:
                duplicate_count += 1

        # Detect removals from previous snapshot
        previous_snapshot = await self.snapshots.find_one({"source_id": source_id}, sort=[("fetched_at", -1)])
        if previous_snapshot and "proxy_ids" in previous_snapshot:
            prev_ids = set(previous_snapshot["proxy_ids"])
            removed_ids = prev_ids - known_proxy_ids
            if removed_ids:
                await repo.mark_source_missing(list(removed_ids))
                summary["removed"] = len(removed_ids)

        # Store snapshot
        snapshot_doc = {
            "source_id": source_id,
            "fetched_at": utcnow(),
            "content_hash": content_hash,
            "count": len(normalized_proxies),
            "added_count": new_count,
            "duplicate_count": duplicate_count,
            "invalid_count": invalid_count,
            "removed_count": summary["removed"],
            "proxy_ids": list(known_proxy_ids),
        }
        await self.snapshots.insert_one(snapshot_doc)

        # Update source
        await self.sources.update_one(
            {"source_id": source_id},
            {
                "$set": {
                    "last_checked_at": utcnow(),
                    "last_success_at": utcnow(),
                    "last_content_hash": content_hash,
                    "last_item_count": len(normalized_proxies),
                    "backoff_count": 0,
                }
            }
        )

        summary["new"] = new_count
        summary["duplicates"] = duplicate_count
        summary["already_known"] = duplicate_count
        return summary

    async def add_manual_source(self, url: str, source_type: str = SourceType.MANUAL_URL,
                                country: Optional[str] = None, name: Optional[str] = None) -> str:
        source_id = hashlib.sha256(f"{source_type}:{url}:{country or ''}".encode()).hexdigest()[:16]
        doc = {
            "source_id": source_id,
            "name": name or url[:50],
            "url": url,
            "source_type": source_type,
            "enabled": True,
            "country": country,
            "protocol": None,
            "last_checked_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "last_content_hash": None,
            "last_item_count": 0,
            "fetch_interval": config.SOURCE_REFRESH_SECONDS,
            "priority": 200,  # higher priority for manual
            "backoff_count": 0,
            "quality_score": 0.0,
            "created_at": utcnow(),
        }
        await self.sources.update_one({"source_id": source_id}, {"$setOnInsert": doc}, upsert=True)
        return source_id

    async def add_manual_proxies_text(self, text: str, name: str = "Manual Text") -> Dict[str, Any]:
        """Process manual text input."""
        return await self._process_manual_proxies(text, source_type=SourceType.MANUAL_TEXT, name=name)

    async def add_manual_proxies_file(self, content: str, filename: str) -> Dict[str, Any]:
        """Process uploaded file content."""
        return await self._process_manual_proxies(content, source_type=SourceType.MANUAL_FILE, name=filename)

    async def _process_manual_proxies(self, content: str, source_type: str, name: str) -> Dict[str, Any]:
        # For manual data, create a pseudo-source and process.
        # We'll parse directly and insert with a generated source_id.
        normalized_proxies, invalid_count = parse_proxies_from_url_content(content)
        source_id = f"manual_{int(time.time())}_{hashlib.md5(content.encode()).hexdigest()[:8]}"
        # Insert a manual source record
        await self.sources.update_one(
            {"source_id": source_id},
            {"$set": {
                "name": name,
                "url": "manual://",
                "source_type": source_type,
                "enabled": False,  # manual source not auto-refreshed
                "priority": 300,
                "created_at": utcnow(),
                "last_checked_at": utcnow(),
                "last_success_at": utcnow(),
                "last_item_count": len(normalized_proxies),
                "last_content_hash": hashlib.sha256(content.encode()).hexdigest(),
                "backoff_count": 0,
            }},
            upsert=True
        )

        repo = ProxyRepository(self.db)
        new_count = 0
        duplicate_count = 0
        for norm in normalized_proxies:
            proxy_id, is_new = await repo.upsert_new_proxy(
                norm,
                source_id=source_id,
                source_country=None,
                source_protocol=norm["scheme"]
            )
            if is_new:
                new_count += 1
            else:
                duplicate_count += 1

        return {
            "received": len(normalized_proxies) + invalid_count,
            "valid": len(normalized_proxies),
            "invalid": invalid_count,
            "new": new_count,
            "duplicates": duplicate_count,
            "source_id": source_id,
        }

    async def get_source_by_id(self, source_id: str) -> Optional[Dict]:
        return await self.sources.find_one({"source_id": source_id})

    async def list_sources(self, enabled_only: bool = False) -> List[Dict]:
        query = {"enabled": True} if enabled_only else {}
        cursor = self.sources.find(query).sort("priority", -1)
        return await cursor.to_list(length=100)

    async def update_source_enabled(self, source_id: str, enabled: bool):
        await self.sources.update_one({"source_id": source_id}, {"$set": {"enabled": enabled}})

    async def remove_source(self, source_id: str):
        await self.sources.delete_one({"source_id": source_id})
        # Also delete snapshots? Maybe keep history but mark disabled.
        # We'll just disable instead.
        await self.sources.update_one({"source_id": source_id}, {"$set": {"enabled": False}})


# ======================================================================
# PROXY TESTER
# ======================================================================
class ProxyTester:
    """Handles generic connectivity, country detection, and YouTube validation."""
    def __init__(self, db, session: aiohttp.ClientSession):
        self.db = db
        self.session = session
        self.repo = ProxyRepository(db)

    async def generic_connectivity_test(self, proxy: Dict) -> Tuple[bool, Optional[int]]:
        """Test if proxy can connect to a generic site. Returns (success, latency_ms)."""
        proxy_url = proxy_to_url(proxy, include_credentials=True)
        test_url = "http://httpbin.org/ip"
        start = time.monotonic()
        try:
            async with self.session.get(
                test_url,
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=config.HTTP_CONNECT_TIMEOUT)
            ) as resp:
                if resp.status == 200:
                    latency = int((time.monotonic() - start) * 1000)
                    return True, latency
                else:
                    return False, None
        except asyncio.TimeoutError:
            return False, None
        except aiohttp.ClientError:
            return False, None
        except Exception:
            return False, None

    async def get_exit_country(self, proxy: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Get exit country of proxy using ip-api.com through proxy."""
        proxy_url = proxy_to_url(proxy, include_credentials=True)
        try:
            async with self.session.get(
                "http://ip-api.com/json/?fields=status,countryCode,country",
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "success":
                        return data.get("countryCode"), data.get("country")
        except Exception:
            pass
        return None, None

    async def test_youtube(self, proxy: Dict, test_url: Optional[str] = None) -> Dict[str, Any]:
        """Run yt-dlp extraction test through proxy using subprocess."""
        proxy_url = proxy_to_url(proxy, include_credentials=True)
        url = test_url or config.YOUTUBE_TEST_URL

        # Build yt-dlp command
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--proxy", proxy_url,
            "--dump-json",
            "--no-warnings",
            "--skip-download",
            "--no-playlist",
            "--socket-timeout", str(config.YOUTUBE_TEST_TIMEOUT),
            "--retries", "1",
            "--force-ipv4",
            url
        ]
        # Maybe include Deno/EJS if needed but can be handled by yt-dlp itself.
        env = os.environ.copy()
        # Ensure we don't hang on JS challenges
        env["YTDLP_NO_JSRUNTIME"] = "1"  # we'll handle JS separately

        start = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=config.YOUTUBE_TEST_TIMEOUT)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return {
                    "result": YoutubeResult.TIMEOUT,
                    "error": "yt-dlp timeout",
                    "duration": time.monotonic() - start,
                }
            duration = time.monotonic() - start
            stdout_str = stdout.decode('utf-8', errors='replace')
            stderr_str = stderr.decode('utf-8', errors='replace')

            if proc.returncode == 0 and stdout_str.strip():
                try:
                    info = json.loads(stdout_str)
                    formats = info.get("formats", [])
                    if formats:
                        return {
                            "result": YoutubeResult.SUCCESS,
                            "duration": duration,
                            "formats_found": len(formats),
                            "yt_dlp_version": info.get("yt_dlp_version"),
                            "country_code": info.get("country") or info.get("uploader_country"),
                        }
                    else:
                        return {
                            "result": YoutubeResult.YOUTUBE_RELOAD_REJECTION,
                            "error": "No formats found",
                            "duration": duration,
                        }
                except json.JSONDecodeError:
                    return {
                        "result": YoutubeResult.YT_DLP_ERROR,
                        "error": "Invalid JSON from yt-dlp",
                        "duration": duration,
                    }

            # Non-zero exit
            combined = stderr_str + stdout_str
            error_lower = combined.lower()
            if "sign in to confirm you're not a bot" in error_lower:
                return {"result": YoutubeResult.YOUTUBE_BOT_REJECTION, "error": "YouTube bot check", "duration": duration}
            elif "the page needs to be reloaded" in error_lower:
                return {"result": YoutubeResult.YOUTUBE_RELOAD_REJECTION, "error": "YouTube reload required", "duration": duration}
            elif "proxy authentication" in error_lower or "proxy auth" in error_lower:
                return {"result": YoutubeResult.PROXY_AUTH_FAILURE, "error": "Proxy auth failed", "duration": duration}
            elif "georestricted" in error_lower or "geo-restricted" in error_lower:
                return {"result": YoutubeResult.GEO_RESTRICTION, "error": "Geo restricted", "duration": duration}
            elif "jsruntime" in error_lower or "deno" in error_lower or "ejs" in error_lower:
                return {"result": YoutubeResult.JS_RUNTIME_ERROR, "error": "JS runtime error", "duration": duration}
            elif "connection" in error_lower or "timed out" in error_lower or "timeout" in error_lower:
                return {"result": YoutubeResult.CONNECTION_FAILURE, "error": combined[:200], "duration": duration}
            else:
                return {"result": YoutubeResult.UNKNOWN, "error": combined[:200], "duration": duration}
        except Exception as e:
            return {
                "result": YoutubeResult.UNKNOWN,
                "error": str(e),
                "duration": time.monotonic() - start,
            }

    async def full_test_proxy(self, proxy: Dict, test_url: Optional[str] = None,
                             skip_generic: bool = False, skip_country: bool = False) -> Dict[str, Any]:
        """
        Perform staged testing on a proxy:
        1. generic connectivity
        2. country detection (if generic passes)
        3. YouTube validation
        Returns result dict.
        """
        proxy_id = proxy["_id"]
        result = {
            "proxy_id": proxy_id,
            "generic_ok": None,
            "latency_ms": None,
            "country_code": None,
            "country_name": None,
            "youtube_result": None,
            "youtube_error": None,
            "youtube_duration": None,
            "youtube_formats": None,
            "yt_dlp_version": None,
            "final_state": None,
            "generic_health": None,
            "youtube_health": None,
        }

        # Stage B: generic connectivity
        if not skip_generic:
            gen_ok, latency = await self.generic_connectivity_test(proxy)
            result["generic_ok"] = gen_ok
            result["latency_ms"] = latency
            if not gen_ok:
                result["final_state"] = ProxyState.CONNECTION_FAILED
                result["generic_health"] = False
                result["youtube_health"] = False
                return result
            result["generic_health"] = True

        # Stage C: country detection (optional but recommended)
        if not skip_country:
            cc, cn = await self.get_exit_country(proxy)
            if cc:
                result["country_code"] = cc
                result["country_name"] = cn
                result["country_source"] = "EXIT_IP"

        # Stage D: YouTube validation
        yt_result = await self.test_youtube(proxy, test_url=test_url)
        result["youtube_result"] = yt_result.get("result")
        result["youtube_error"] = yt_result.get("error")
        result["youtube_duration"] = yt_result.get("duration")
        result["youtube_formats"] = yt_result.get("formats_found")
        result["yt_dlp_version"] = yt_result.get("yt_dlp_version")

        if yt_result.get("result") == YoutubeResult.SUCCESS:
            result["final_state"] = ProxyState.YOUTUBE_WORKING
            result["youtube_health"] = True
        elif yt_result.get("result") in [YoutubeResult.TIMEOUT, YoutubeResult.CONNECTION_FAILURE]:
            result["final_state"] = yt_result["result"]  # TIMEOUT or CONNECTION_FAILED
            result["youtube_health"] = False
        elif yt_result.get("result") in [YoutubeResult.YOUTUBE_BOT_REJECTION,
                                         YoutubeResult.YOUTUBE_RELOAD_REJECTION]:
            result["final_state"] = ProxyState.YOUTUBE_REJECTED
            result["youtube_health"] = False
        elif yt_result.get("result") in [YoutubeResult.GEO_RESTRICTION,
                                         YoutubeResult.AUTH_REQUIRED]:
            result["final_state"] = ProxyState.YOUTUBE_REJECTED
            result["youtube_health"] = False
        elif yt_result.get("result") in [YoutubeResult.JS_RUNTIME_ERROR,
                                         YoutubeResult.YT_DLP_ERROR,
                                         YoutubeResult.PROXY_AUTH_FAILURE]:
            # Environment or auth issues; we should not necessarily mark proxy dead.
            # We'll mark as YT_DLP_ERROR for later re-test.
            result["final_state"] = ProxyState.YT_DLP_ERROR if yt_result.get("result") != YoutubeResult.PROXY_AUTH_FAILURE else ProxyState.CONNECTION_FAILED
            result["youtube_health"] = False
        else:
            result["final_state"] = ProxyState.CONNECTION_FAILED
            result["youtube_health"] = False

        return result

    async def test_specific_proxy(self, proxy_id: str) -> Dict[str, Any]:
        proxy = await self.repo.get_proxy_by_id(proxy_id)
        if not proxy:
            return {"error": "Proxy not found"}
        result = await self.full_test_proxy(proxy)
        # Update database
        await self.repo.update_proxy_test_result(
            proxy_id,
            result,
            state=result["final_state"],
            generic_health=result.get("generic_health"),
            youtube_health=result.get("youtube_health"),
            country_code=result.get("country_code"),
            country_name=result.get("country_name"),
            country_source="EXIT_IP" if result.get("country_code") else None,
            latency_ms=result.get("latency_ms"),
            test_duration=result.get("youtube_duration"),
            formats_found=result.get("youtube_formats"),
            yt_dlp_version=result.get("yt_dlp_version"),
            error_category=result.get("youtube_result"),
            error_details=result.get("youtube_error"),
        )
        return result


# ======================================================================
# SCHEDULER
# ======================================================================
class WorkerScheduler:
    """Coordinates periodic tasks and manual task execution."""
    def __init__(self, db, source_manager: ProxySourceManager, tester: ProxyTester,
                 bot: AsyncTeleBot, report_manager, alert_manager):
        self.db = db
        self.source_manager = source_manager
        self.tester = tester
        self.bot = bot
        self.report_manager = report_manager
        self.alert_manager = alert_manager
        self.tasks = {}  # task_id -> asyncio.Task
        self.pending_proxy_queue = asyncio.Queue(maxsize=config.MAX_PENDING_TESTS)
        self.active_test_semaphore = asyncio.Semaphore(config.PROXY_TEST_CONCURRENCY)
        self.scheduler_running = False
        self.main_loop_task = None

    async def start(self):
        self.scheduler_running = True
        self.main_loop_task = asyncio.create_task(self._main_loop())

    async def stop(self):
        self.scheduler_running = False
        if self.main_loop_task:
            self.main_loop_task.cancel()
            try:
                await self.main_loop_task
            except asyncio.CancelledError:
                pass
        # Cancel pending tasks
        for task in self.tasks.values():
            task.cancel()

    async def _main_loop(self):
        """Main scheduler loop."""
        while self.scheduler_running:
            try:
                await self._refresh_sources_if_needed()
                await self._process_proxy_queue()
                await self._revalidate_due_proxies()
                await self._expire_quarantines()
                await self._cleanup()
                await asyncio.sleep(10)  # control loop every 10s
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(10)

    async def _refresh_sources_if_needed(self):
        """Check for sources due for refresh and process them."""
        sources = await self.source_manager.get_enabled_sources()
        now = utcnow()
        for source in sources:
            last_checked = source.get("last_checked_at")
            if last_checked is None or (now - last_checked).total_seconds() >= source.get("fetch_interval", config.SOURCE_REFRESH_SECONDS):
                # Schedule a refresh task
                asyncio.create_task(self.refresh_source(source["source_id"]))
                # To avoid many simultaneous refreshes, limit per loop
                await asyncio.sleep(1)

    async def refresh_source(self, source_id: str):
        """Refresh a single source, process new proxies, enqueue tests."""
        source = await self.source_manager.get_source_by_id(source_id)
        if not source:
            return
        logger.info(f"[SOURCE] refresh started for {source_id}")
        summary = await self.source_manager.process_source(source)
        logger.info(f"[SOURCE] {source_id} result: {summary}")
        await self.report_manager.record_task_result(
            task_type="source_refresh",
            source_id=source_id,
            status=TaskStatus.COMPLETED if summary.get("fetched") else TaskStatus.FAILED,
            total_items=summary.get("total_fetched"),
            new_items=summary.get("new"),
            duplicates=summary.get("duplicates"),
            invalid=summary.get("invalid"),
            error=summary.get("error"),
        )
        # If new proxies found, enqueue them
        if summary.get("new", 0) > 0:
            # We'll fetch the new proxies from DB where source_id matches and state UNTESTED
            new_proxies = await self.db.proxies.find(
                {"source_id": source_id, "state": ProxyState.UNTESTED}
            ).to_list(length=config.MAX_PROXIES_PER_REFRESH)
            for proxy in new_proxies[:config.MAX_PROXIES_PER_REFRESH]:
                try:
                    self.pending_proxy_queue.put_nowait(proxy["_id"])
                except asyncio.QueueFull:
                    logger.warning("Pending proxy queue full, dropping new proxy")
                    break

    async def _process_proxy_queue(self):
        """Process proxies from queue with bounded concurrency."""
        while not self.pending_proxy_queue.empty():
            try:
                proxy_id = self.pending_proxy_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            asyncio.create_task(self._test_proxy_async(proxy_id))
            # Avoid creating too many tasks at once; semaphore inside will handle concurrency
            await asyncio.sleep(0.1)

    async def _test_proxy_async(self, proxy_id: str):
        async with self.active_test_semaphore:
            proxy = await self.db.proxies.find_one({"_id": proxy_id})
            if not proxy or proxy.get("state") == ProxyState.TESTING:
                return
            # Claim lease
            claimed = await self.db.proxies.find_one_and_update(
                {"_id": proxy_id, "state": {"$ne": ProxyState.TESTING}},
                {"$set": {"state": ProxyState.TESTING, "testing_lease_until": utcnow() + timedelta(seconds=120)}},
                return_document=True
            )
            if not claimed:
                return
            logger.info(f"[TEST] proxy_id={proxy_id} start")
            result = await self.tester.full_test_proxy(claimed)
            # Update DB
            await self.tester.repo.update_proxy_test_result(
                proxy_id,
                result,
                state=result["final_state"],
                generic_health=result.get("generic_health"),
                youtube_health=result.get("youtube_health"),
                country_code=result.get("country_code"),
                country_name=result.get("country_name"),
                country_source="EXIT_IP" if result.get("country_code") else None,
                latency_ms=result.get("latency_ms"),
                test_duration=result.get("youtube_duration"),
                formats_found=result.get("youtube_formats"),
                yt_dlp_version=result.get("yt_dlp_version"),
                error_category=result.get("youtube_result"),
                error_details=result.get("youtube_error"),
            )
            logger.info(f"[TEST] proxy_id={proxy_id} result={result['final_state']}")

    async def enqueue_proxy_test(self, proxy_id: str):
        try:
            await self.pending_proxy_queue.put(proxy_id)
        except asyncio.QueueFull:
            logger.warning("Pending queue full")

    async def _revalidate_due_proxies(self):
        """Revalidate successful proxies that are due."""
        proxies_due = await self.tester.repo.get_recently_successful_due_for_revalidation(limit=5)
        for proxy in proxies_due:
            await self.enqueue_proxy_test(proxy["_id"])

    async def _expire_quarantines(self):
        """Move quarantined proxies back to UNTESTED if quarantine expired."""
        now = utcnow()
        result = await self.db.proxies.update_many(
            {"state": ProxyState.QUARANTINED, "quarantine_until": {"$lte": now}},
            {"$set": {"state": ProxyState.UNTESTED, "quarantine_until": None}}
        )
        if result.modified_count > 0:
            logger.info(f"Released {result.modified_count} quarantined proxies")

    async def _cleanup(self):
        """Periodic cleanup of old data."""
        # Clean old source snapshots
        cutoff = utcnow() - timedelta(days=30)
        await self.db.source_snapshots.delete_many({"fetched_at": {"$lt": cutoff}})
        # Clean old task runs
        await self.db.task_runs.delete_many({"started_at": {"$lt": cutoff}})
        # Clean old events
        await self.db.proxy_events.delete_many({"created_at": {"$lt": cutoff}})
        # Retire source_missing proxies
        await self.tester.repo.retire_missing_proxies(older_than_hours=48)

    async def manual_test_proxies(self, filter_query: Dict = None, limit: int = 10):
        """Manually trigger testing of proxies matching filter."""
        query = filter_query or {}
        proxies = await self.db.proxies.find(query).limit(limit).to_list(length=limit)
        for proxy in proxies:
            await self.enqueue_proxy_test(proxy["_id"])
        return len(proxies)

    async def manual_revalidate_working(self, limit: int = 10):
        proxies = await self.db.proxies.find(
            {"state": ProxyState.YOUTUBE_WORKING, "enabled": True}
        ).sort("youtube_last_success_at", 1).limit(limit).to_list(length=limit)
        for proxy in proxies:
            await self.enqueue_proxy_test(proxy["_id"])
        return len(proxies)

    async def cancel_task(self, task_id: str):
        # We can't easily cancel internal tasks, but we can mark task_runs as cancelled
        await self.db.task_runs.update_one(
            {"task_id": task_id},
            {"$set": {"status": TaskStatus.CANCELLED, "finished_at": utcnow()}}
        )


# ======================================================================
# REPORT MANAGER
# ======================================================================
class ReportManager:
    """Handles generation and sending of reports."""
    def __init__(self, db, bot: AsyncTeleBot, config):
        self.db = db
        self.bot = bot
        self.config = config

    async def record_task_result(self, task_type: str, status: str, source_id: str = None,
                                 total_items: int = 0, new_items: int = 0, duplicates: int = 0,
                                 invalid: int = 0, tested: int = 0, successes: int = 0,
                                 failures: int = 0, error: str = None, **kwargs):
        task_id = hashlib.sha256(f"{task_type}:{utcnow().isoformat()}:{os.urandom(4).hex()}".encode()).hexdigest()[:16]
        doc = {
            "task_id": task_id,
            "task_type": task_type,
            "source_id": source_id,
            "status": status,
            "started_at": utcnow(),
            "finished_at": utcnow() if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED] else None,
            "total_items": total_items,
            "new_items": new_items,
            "duplicates": duplicates,
            "invalid": invalid,
            "tested": tested,
            "successes": successes,
            "failures": failures,
            "error": error,
            **kwargs,
        }
        await self.db.task_runs.insert_one(doc)
        return task_id

    async def generate_daily_report(self) -> str:
        """Generate a text summary for daily report."""
        repo = ProxyRepository(self.db)
        stats = await repo.get_stats()
        now = utcnow()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        new_today = await self.db.proxies.count_documents({"created_at": {"$gte": today_start}})
        tested_today = await self.db.proxies.count_documents({"tested_at": {"$gte": today_start}})
        working_now = stats["working"]
        quarantined = stats["quarantined"]
        total = stats["total"]
        countries = stats["countries"]

        report = [
            "📊 DAILY WORKER REPORT",
            f"📅 Date: {now.strftime('%Y-%m-%d')}",
            f"🕐 Time: {now.strftime('%H:%M UTC')}",
            "",
            f"📥 Sources processed: {await self.db.source_snapshots.count_documents({'fetched_at': {'$gte': today_start}})}",
            f"➕ New proxies today: {new_today}",
            f"🧪 Tested today: {tested_today}",
            f"🟢 YouTube working: {working_now}",
            f"⛔ Quarantined: {quarantined}",
            f"🗑️ Retired: {stats['retired']}",
            f"💾 Total known: {total}",
            "",
            "🌍 Countries:",
        ]
        for cc, data in sorted(countries.items(), key=lambda x: x[1]["working"], reverse=True)[:15]:
            flag = self._country_flag(cc)
            report.append(f"{flag} {cc}: total={data['total']}, working={data['working']}")
        report.append("")
        report.append(f"🕒 Worker uptime: {self._format_uptime()}")

        # Add source health
        sources = await self.db.proxy_sources.find({"enabled": True}).to_list(length=20)
        report.append("\n📚 Source Health:")
        for src in sources[:5]:
            status = "🟢" if src.get("last_success_at") and (now - src["last_success_at"]).days < 1 else "🔴"
            report.append(f"{status} {src['name']}: last success {src.get('last_success_at')}")

        return "\n".join(report)

    def _country_flag(self, cc: str) -> str:
        """Simple flag emoji from country code."""
        if not cc:
            return "🏳️"
        return chr(ord(cc[0]) + 127397) + chr(ord(cc[1]) + 127397)

    def _format_uptime(self) -> str:
        if not hasattr(self, "_start_time"):
            self._start_time = utcnow()
        delta = utcnow() - self._start_time
        total_seconds = int(delta.total_seconds())
        hours, rem = divmod(total_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours}h {minutes}m {seconds}s"

    async def send_daily_report(self):
        chat_id = self.config.REPORT_CHAT_ID or str(self.config.OWNER_ID)
        report = await self.generate_daily_report()
        try:
            await self.bot.send_message(chat_id, report)
        except Exception as e:
            logger.error(f"Failed to send daily report: {e}")

    async def generate_proxy_file(self, country_code: Optional[str] = None) -> str:
        """Generate a CSV file of working proxies."""
        repo = ProxyRepository(self.db)
        proxies = await repo.find_working_proxies(country_code, limit=500)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["proxy", "country", "score", "latency_ms", "last_success"])
        for p in proxies:
            proxy_str = proxy_to_url(p, include_credentials=False)
            writer.writerow([
                proxy_str,
                p.get("country_code", ""),
                p.get("youtube_score", 0),
                p.get("latency_ms", ""),
                p.get("youtube_last_success_at", "").isoformat() if p.get("youtube_last_success_at") else "",
            ])
        return output.getvalue()

    async def send_proxy_file(self, chat_id: str, country_code: Optional[str] = None):
        content = await self.generate_proxy_file(country_code)
        filename = f"proxies_{country_code or 'all'}_{utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        import io as io_mod
        file_bytes = content.encode('utf-8')
        try:
            await self.bot.send_document(chat_id, io_mod.BytesIO(file_bytes), visible_file_name=filename)
        except Exception as e:
            logger.error(f"Failed to send proxy file: {e}")


# ======================================================================
# ALERT MANAGER
# ======================================================================
class AlertManager:
    """Sends alerts with cooldown."""
    def __init__(self, bot: AsyncTeleBot, owner_id: int):
        self.bot = bot
        self.owner_id = owner_id
        self.last_alert_at = {}  # key -> timestamp

    async def send_alert(self, key: str, message: str, cooldown_seconds: int = 3600):
        now = time.time()
        last = self.last_alert_at.get(key, 0)
        if now - last < cooldown_seconds:
            return
        self.last_alert_at[key] = now
        try:
            await self.bot.send_message(self.owner_id, f"🚨 {message}")
        except Exception as e:
            logger.error(f"Alert failed: {e}")

    async def check_critical_conditions(self, db):
        # Example: check if no working proxies
        working_count = await db.proxies.count_documents({
            "state": ProxyState.YOUTUBE_WORKING,
            "enabled": True,
            "quarantine_until": None,
        })
        if working_count == 0:
            await self.send_alert("no_working", "No YouTube-working proxies available!", cooldown_seconds=1800)
        # Check MongoDB connectivity
        try:
            await db.command("ping")
        except Exception as e:
            await self.send_alert("mongo_down", f"MongoDB unreachable: {e}", cooldown_seconds=300)


# ======================================================================
# TELEGRAM UI
# ======================================================================
class TelegramAdminUI:
    """Owner-only Telegram interface."""
    def __init__(self, bot: AsyncTeleBot, db, config, scheduler: WorkerScheduler,
                 source_manager: ProxySourceManager, tester: ProxyTester,
                 report_manager: ReportManager, alert_manager: AlertManager):
        self.bot = bot
        self.db = db
        self.config = config
        self.scheduler = scheduler
        self.source_manager = source_manager
        self.tester = tester
        self.report_manager = report_manager
        self.alert_manager = alert_manager
        self.owner_id = config.OWNER_ID
        self.pending_input = {}  # user_id -> state dict
        self.progress_messages = {}  # task_id -> message_id

    def is_authorized(self, user_id: int) -> bool:
        return user_id == self.owner_id

    async def setup_handlers(self):
        @self.bot.message_handler(commands=['start', 'menu'])
        async def cmd_start(message):
            if not self.is_authorized(message.from_user.id):
                return
            await self.show_main_dashboard(message.chat.id)

        @self.bot.callback_query_handler(func=lambda call: True)
        async def callback_query(call):
            if not self.is_authorized(call.from_user.id):
                await self.bot.answer_callback_query(call.id, "Unauthorized", show_alert=True)
                return
            await self.handle_callback(call)

        @self.bot.message_handler(content_types=['document'])
        async def handle_document(message):
            if not self.is_authorized(message.from_user.id):
                return
            # Check file size
            if message.document.file_size > 5 * 1024 * 1024:  # 5MB limit
                await self.bot.reply_to(message, "File too large (max 5MB)")
                return
            file_info = await self.bot.get_file(message.document.file_id)
            downloaded_file = await self.bot.download_file(file_info.file_path)
            content = downloaded_file.decode('utf-8', errors='replace')
            summary = await self.source_manager.add_manual_proxies_file(content, message.document.file_name)
            await self.bot.reply_to(message, self.format_manual_import_report(summary))
            # Offer to test new proxies
            markup = tgtypes.InlineKeyboardMarkup()
            markup.add(tgtypes.InlineKeyboardButton("🧪 Test New Proxies", callback_data=f"test_new_{summary['source_id']}"))
            await self.bot.reply_to(message, "Import complete. Do you want to test the new proxies?", reply_markup=markup)

        @self.bot.message_handler(func=lambda message: True)
        async def handle_text(message):
            if not self.is_authorized(message.from_user.id):
                return
            # Check if waiting for input
            if message.from_user.id in self.pending_input:
                await self.handle_pending_input(message)
                return
            # Default help
            await self.show_main_dashboard(message.chat.id)

    async def show_main_dashboard(self, chat_id: int):
        stats = await ProxyRepository(self.db).get_stats()
        working = stats["working"]
        total = stats["total"]
        testing = stats["testing"]
        queue_size = self.scheduler.pending_proxy_queue.qsize()

        text = (
            "🧠 PROXY WORKER\n\n"
            f"🟢 Worker: Online\n"
            f"🟢 MongoDB: Connected\n"
            f"🟢 Scheduler: Running\n\n"
            f"🌐 YouTube Pool: {working}\n"
            f"💾 Total Known: {total}\n"
            f"🧪 Testing: {testing}\n"
            f"⏳ Queue: {queue_size}\n"
        )

        markup = tgtypes.InlineKeyboardMarkup(row_width=2)
        markup.add(
            tgtypes.InlineKeyboardButton("➕ Add URL", callback_data="add_url"),
            tgtypes.InlineKeyboardButton("📝 Add Text", callback_data="add_text"),
            tgtypes.InlineKeyboardButton("📁 Upload File", callback_data="upload_file"),
            tgtypes.InlineKeyboardButton("📊 System Stats", callback_data="stats"),
            tgtypes.InlineKeyboardButton("🌍 Countries", callback_data="countries"),
            tgtypes.InlineKeyboardButton("🧪 Testing", callback_data="testing_menu"),
            tgtypes.InlineKeyboardButton("📚 Sources", callback_data="sources"),
            tgtypes.InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
            tgtypes.InlineKeyboardButton("❤️ Health", callback_data="health"),
            tgtypes.InlineKeyboardButton("📈 Reports", callback_data="reports"),
            tgtypes.InlineKeyboardButton("🔄 Refresh Now", callback_data="refresh_now"),
            tgtypes.InlineKeyboardButton("🗑️ Cleanup", callback_data="cleanup"),
        )
        await self.bot.send_message(chat_id, text, reply_markup=markup)

    async def handle_callback(self, call):
        data = call.data
        chat_id = call.message.chat.id
        user_id = call.from_user.id

        # Answer callback to stop loading
        await self.bot.answer_callback_query(call.id)

        if data == "add_url":
            self.pending_input[user_id] = {"action": "add_url"}
            await self.bot.send_message(chat_id, "Please send the source URL:")
        elif data == "add_text":
            self.pending_input[user_id] = {"action": "add_text"}
            await self.bot.send_message(chat_id, "Paste proxy list (one per line):")
        elif data == "upload_file":
            await self.bot.send_message(chat_id, "Send a .txt, .csv, or .json file containing proxies.")
        elif data == "stats":
            await self.show_stats(chat_id)
        elif data == "countries":
            await self.show_countries(chat_id)
        elif data == "testing_menu":
            await self.show_testing_menu(chat_id)
        elif data == "sources":
            await self.show_sources(chat_id)
        elif data == "settings":
            await self.show_settings(chat_id)
        elif data == "health":
            await self.show_health(chat_id)
        elif data == "reports":
            await self.show_reports(chat_id)
        elif data == "refresh_now":
            await self.cmd_refresh_now(chat_id)
        elif data == "cleanup":
            await self.cmd_cleanup(chat_id)
        elif data.startswith("test_new_"):
            source_id = data[len("test_new_"):]
            await self.test_new_from_source(chat_id, source_id)
        elif data.startswith("country_"):
            cc = data.split("_")[1]
            await self.show_country_detail(chat_id, cc)
        elif data.startswith("source_toggle_"):
            source_id = data[len("source_toggle_"):]
            await self.toggle_source(chat_id, source_id)
        elif data.startswith("source_refresh_"):
            source_id = data[len("source_refresh_"):]
            await self.refresh_single_source(chat_id, source_id)
        elif data.startswith("test_specific_"):
            proxy_id = data[len("test_specific_"):]
            await self.test_specific_proxy(chat_id, proxy_id)
        elif data.startswith("export_"):
            cc = data.split("_")[1] if len(data.split("_")) > 1 else None
            await self.export_proxies(chat_id, cc)
        elif data == "settings_concurrency":
            self.pending_input[user_id] = {"action": "set_concurrency"}
            await self.bot.send_message(chat_id, "Send new test concurrency (1-50):")
        elif data == "settings_testurl":
            self.pending_input[user_id] = {"action": "set_testurl"}
            await self.bot.send_message(chat_id, "Send new YouTube test URL:")
        elif data == "settings_quarantine":
            self.pending_input[user_id] = {"action": "set_quarantine"}
            await self.bot.send_message(chat_id, "Send quarantine hours (1-72):")
        elif data == "settings_refresh":
            self.pending_input[user_id] = {"action": "set_refresh_interval"}
            await self.bot.send_message(chat_id, "Send source refresh interval in seconds (60-3600):")
        elif data == "daily_report_now":
            await self.report_manager.send_daily_report()
            await self.bot.send_message(chat_id, "Daily report sent.")
        elif data == "export_all":
            await self.export_proxies(chat_id, None)
        elif data == "back_main":
            await self.show_main_dashboard(chat_id)
        else:
            await self.bot.send_message(chat_id, "Unknown action")

    async def handle_pending_input(self, message):
        user_id = message.from_user.id
        action = self.pending_input.get(user_id, {}).get("action")
        if not action:
            return
        del self.pending_input[user_id]

        if action == "add_url":
            url = message.text.strip()
            # Basic URL validation
            if not url.startswith(("http://", "https://")):
                await self.bot.reply_to(message, "Invalid URL. Must start with http:// or https://")
                return
            # Validate by fetching a small portion?
            # For now just add source
            source_id = await self.source_manager.add_manual_source(url, source_type=SourceType.MANUAL_URL)
            await self.bot.reply_to(message, f"Source added with ID: {source_id}\nYou can now refresh it.")
            # Optionally refresh immediately
            await self.source_manager.process_source(await self.source_manager.get_source_by_id(source_id))
        elif action == "add_text":
            summary = await self.source_manager.add_manual_proxies_text(message.text)
            await self.bot.reply_to(message, self.format_manual_import_report(summary))
            markup = tgtypes.InlineKeyboardMarkup()
            markup.add(tgtypes.InlineKeyboardButton("🧪 Test New Proxies", callback_data=f"test_new_{summary['source_id']}"))
            await self.bot.reply_to(message, "Do you want to test the new proxies?", reply_markup=markup)
        elif action == "set_concurrency":
            try:
                val = int(message.text)
                if 1 <= val <= 50:
                    self.config.PROXY_TEST_CONCURRENCY = val
                    self.scheduler.active_test_semaphore = asyncio.Semaphore(val)
                    await self.bot.reply_to(message, f"Concurrency set to {val}")
                else:
                    await self.bot.reply_to(message, "Value must be between 1 and 50")
            except ValueError:
                await self.bot.reply_to(message, "Invalid number")
        elif action == "set_testurl":
            url = message.text.strip()
            if url.startswith("https://www.youtube.com/"):
                self.config.YOUTUBE_TEST_URL = url
                await self.db.worker_settings.update_one(
                    {"key": "youtube_test_url"},
                    {"$set": {"value": url, "updated_at": utcnow()}},
                    upsert=True
                )
                await self.bot.reply_to(message, "YouTube test URL updated.")
            else:
                await self.bot.reply_to(message, "Invalid YouTube URL")
        elif action == "set_quarantine":
            try:
                val = int(message.text)
                if 1 <= val <= 72:
                    self.config.PROXY_QUARANTINE_HOURS = val
                    await self.bot.reply_to(message, f"Quarantine duration set to {val} hours")
                else:
                    await self.bot.reply_to(message, "Value must be 1-72")
            except ValueError:
                await self.bot.reply_to(message, "Invalid number")
        elif action == "set_refresh_interval":
            try:
                val = int(message.text)
                if 60 <= val <= 3600:
                    self.config.SOURCE_REFRESH_SECONDS = val
                    await self.bot.reply_to(message, f"Refresh interval set to {val} seconds")
                else:
                    await self.bot.reply_to(message, "Value must be 60-3600")
            except ValueError:
                await self.bot.reply_to(message, "Invalid number")

    def format_manual_import_report(self, summary: Dict[str, Any]) -> str:
        return (
            "📋 MANUAL LIST REPORT\n\n"
            f"Received: {summary.get('received', 0)}\n"
            f"✅ Valid: {summary.get('valid', 0)}\n"
            f"♻️ Duplicates: {summary.get('duplicates', 0)}\n"
            f"❌ Invalid: {summary.get('invalid', 0)}\n"
            f"➕ New: {summary.get('new', 0)}\n"
        )

    async def show_stats(self, chat_id):
        stats = await ProxyRepository(self.db).get_stats()
        repo = ProxyRepository(self.db)
        total = stats["total"]
        working = stats["working"]
        quarantined = stats["quarantined"]
        untested = stats["untested"]
        testing = stats["testing"]
        retired = stats["retired"]
        disabled = stats["disabled"]
        queue_size = self.scheduler.pending_proxy_queue.qsize()

        text = (
            "📊 SYSTEM STATS\n\n"
            f"💾 Total known proxies: {total}\n"
            f"🟢 YouTube working: {working}\n"
            f"⛔ Quarantined: {quarantined}\n"
            f"❓ Untested: {untested}\n"
            f"🧪 Testing: {testing}\n"
            f"🗑️ Retired: {retired}\n"
            f"🚫 Disabled: {disabled}\n"
            f"⏳ Pending queue: {queue_size}\n"
            f"🕒 Uptime: {self.report_manager._format_uptime()}\n"
        )
        await self.bot.send_message(chat_id, text)

    async def show_countries(self, chat_id):
        stats = await ProxyRepository(self.db).get_stats()
        countries = stats["countries"]
        text = "🌍 PROXY COUNTRIES\n\n"
        for cc, data in sorted(countries.items(), key=lambda x: x[1]["working"], reverse=True):
            flag = self.report_manager._country_flag(cc)
            text += f"{flag} {cc}:\n🟢 Working: {data['working']}\n💾 Total: {data['total']}\n\n"
        if not countries:
            text += "No country data available yet."
        await self.bot.send_message(chat_id, text)

    async def show_country_detail(self, chat_id, cc):
        proxies = await self.db.proxies.find({"country_code": cc, "state": ProxyState.YOUTUBE_WORKING}).to_list(length=20)
        text = f"🌍 {cc} Working Proxies\n\n"
        for p in proxies[:20]:
            text += f"• {proxy_to_url(p, include_credentials=False)}\n"
        await self.bot.send_message(chat_id, text)

    async def show_testing_menu(self, chat_id):
        markup = tgtypes.InlineKeyboardMarkup(row_width=1)
        markup.add(
            tgtypes.InlineKeyboardButton("Test New Only", callback_data="test_new_menu"),
            tgtypes.InlineKeyboardButton("Revalidate Working", callback_data="revalidate_working"),
            tgtypes.InlineKeyboardButton("Test Specific Proxy", callback_data="test_specific_prompt"),
            tgtypes.InlineKeyboardButton("Back", callback_data="back_main"),
        )
        await self.bot.send_message(chat_id, "🧪 Testing Options", reply_markup=markup)

    async def show_sources(self, chat_id):
        sources = await self.source_manager.list_sources()
        text = "📚 Sources\n\n"
        for src in sources:
            enabled = "🟢" if src.get("enabled") else "🔴"
            last = src.get("last_success_at")
            last_str = last.strftime("%Y-%m-%d %H:%M") if last else "Never"
            text += f"{enabled} {src['name']} (ID: {src['source_id']})\nLast success: {last_str}\nItems: {src.get('last_item_count', 0)}\n\n"
        await self.bot.send_message(chat_id, text)

    async def show_settings(self, chat_id):
        text = (
            "⚙️ SETTINGS\n\n"
            f"Test concurrency: {self.config.PROXY_TEST_CONCURRENCY}\n"
            f"YouTube timeout: {self.config.YOUTUBE_TEST_TIMEOUT}s\n"
            f"Source refresh interval: {self.config.SOURCE_REFRESH_SECONDS}s\n"
            f"Quarantine hours: {self.config.PROXY_QUARANTINE_HOURS}\n"
            f"YouTube test URL: {self.config.YOUTUBE_TEST_URL}\n"
        )
        markup = tgtypes.InlineKeyboardMarkup(row_width=1)
        markup.add(
            tgtypes.InlineKeyboardButton("Set Concurrency", callback_data="settings_concurrency"),
            tgtypes.InlineKeyboardButton("Set Test URL", callback_data="settings_testurl"),
            tgtypes.InlineKeyboardButton("Set Quarantine Hours", callback_data="settings_quarantine"),
            tgtypes.InlineKeyboardButton("Set Refresh Interval", callback_data="settings_refresh"),
            tgtypes.InlineKeyboardButton("Back", callback_data="back_main"),
        )
        await self.bot.send_message(chat_id, text, reply_markup=markup)

    async def show_health(self, chat_id):
        # Basic health
        text = (
            "❤️ HEALTH\n\n"
            f"Worker: 🟢 Online\n"
            f"MongoDB: 🟢 Connected\n"
            f"Scheduler: 🟢 Running\n"
            f"Active tests: {self.scheduler.active_test_semaphore._value}\n"
            f"Queue size: {self.scheduler.pending_proxy_queue.qsize()}\n"
            f"Uptime: {self.report_manager._format_uptime()}\n"
        )
        await self.bot.send_message(chat_id, text)

    async def show_reports(self, chat_id):
        markup = tgtypes.InlineKeyboardMarkup(row_width=1)
        markup.add(
            tgtypes.InlineKeyboardButton("Send Daily Report Now", callback_data="daily_report_now"),
            tgtypes.InlineKeyboardButton("Export All Working Proxies", callback_data="export_all"),
            tgtypes.InlineKeyboardButton("Export US Proxies", callback_data="export_US"),
            tgtypes.InlineKeyboardButton("Export GB Proxies", callback_data="export_GB"),
            tgtypes.InlineKeyboardButton("Back", callback_data="back_main"),
        )
        await self.bot.send_message(chat_id, "📈 Reports", reply_markup=markup)

    async def cmd_refresh_now(self, chat_id):
        sources = await self.source_manager.get_enabled_sources()
        await self.bot.send_message(chat_id, f"Refreshing {len(sources)} sources...")
        for source in sources:
            await self.scheduler.refresh_source(source["source_id"])
        await self.bot.send_message(chat_id, "Refresh triggered. Check logs.")

    async def cmd_cleanup(self, chat_id):
        await self.scheduler._cleanup()
        await self.bot.send_message(chat_id, "Cleanup completed.")

    async def test_new_from_source(self, chat_id, source_id):
        # Find untested proxies from that source
        proxies = await self.db.proxies.find(
            {"source_id": source_id, "state": ProxyState.UNTESTED}
        ).limit(config.MAX_PROXIES_PER_REFRESH).to_list(length=config.MAX_PROXIES_PER_REFRESH)
        count = 0
        for proxy in proxies:
            await self.scheduler.enqueue_proxy_test(proxy["_id"])
            count += 1
        await self.bot.send_message(chat_id, f"Queued {count} proxies for testing.")

    async def toggle_source(self, chat_id, source_id):
        source = await self.source_manager.get_source_by_id(source_id)
        if source:
            new_enabled = not source.get("enabled", True)
            await self.source_manager.update_source_enabled(source_id, new_enabled)
            await self.bot.send_message(chat_id, f"Source {'enabled' if new_enabled else 'disabled'}.")
        else:
            await self.bot.send_message(chat_id, "Source not found.")

    async def refresh_single_source(self, chat_id, source_id):
        await self.scheduler.refresh_source(source_id)
        await self.bot.send_message(chat_id, f"Source {source_id} refreshed.")

    async def test_specific_proxy(self, chat_id, proxy_id):
        result = await self.tester.test_specific_proxy(proxy_id)
        text = (
            f"Proxy: {proxy_id}\n"
            f"Result: {result.get('final_state')}\n"
            f"Generic health: {result.get('generic_health')}\n"
            f"YouTube health: {result.get('youtube_health')}\n"
            f"Latency: {result.get('latency_ms')} ms\n"
            f"Country: {result.get('country_code')}\n"
            f"YouTube error: {result.get('youtube_error')}\n"
        )
        await self.bot.send_message(chat_id, text)

    async def export_proxies(self, chat_id, country_code=None):
        await self.report_manager.send_proxy_file(str(chat_id), country_code)
        await self.bot.send_message(chat_id, "Exported proxies file.")


# ======================================================================
# HEALTH SERVER
# ======================================================================
class HealthServer:
    """aiohttp HTTP server for health/ready endpoints and optional webhook."""
    def __init__(self, config, db, scheduler):
        self.config = config
        self.db = db
        self.scheduler = scheduler
        self.app = aiohttp.web.Application()
        self.runner = None
        self.setup_routes()

    def setup_routes(self):
        self.app.router.add_get("/health", self.health_handler)
        self.app.router.add_get("/ready", self.ready_handler)

    async def health_handler(self, request):
        mongo_ok = await self.check_mongo()
        scheduler_running = self.scheduler.scheduler_running
        stats = await ProxyRepository(self.db).get_stats()
        data = {
            "worker": "healthy" if mongo_ok and scheduler_running else "degraded",
            "mongodb": mongo_ok,
            "scheduler": scheduler_running,
            "last_source_refresh": None,  # could compute
            "active_tests": self.scheduler.active_test_semaphore._value,
            "queue_size": self.scheduler.pending_proxy_queue.qsize(),
            "youtube_working": stats["working"],
            "uptime": self.scheduler.report_manager._format_uptime() if hasattr(self.scheduler, 'report_manager') else "unknown",
        }
        return aiohttp.web.json_response(data)

    async def ready_handler(self, request):
        mongo_ok = await self.check_mongo()
        if mongo_ok:
            return aiohttp.web.json_response({"status": "ready"})
        return aiohttp.web.json_response({"status": "not_ready"}, status=503)

    async def check_mongo(self) -> bool:
        try:
            await self.db.command("ping")
            return True
        except Exception:
            return False

    async def start(self):
        self.runner = aiohttp.web.AppRunner(self.app)
        await self.runner.setup()
        site = aiohttp.web.TCPSite(self.runner, host=self.config.HTTP_HOST, port=self.config.PORT)
        await site.start()
        logger.info(f"Health server running on {self.config.HTTP_HOST}:{self.config.PORT}")

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()


# ======================================================================
# MAIN APPLICATION
# ======================================================================
class ProxyWorkerBot:
    def __init__(self):
        self.config = config
        self.db = None
        self.bot = None
        self.session = None
        self.source_manager = None
        self.proxy_repo = None
        self.tester = None
        self.scheduler = None
        self.report_manager = None
        self.alert_manager = None
        self.telegram_ui = None
        self.health_server = None

    async def initialize(self):
        # Validate config
        if not self.config.validate():
            logger.critical("Invalid configuration")
            sys.exit(1)

        # Connect MongoDB
        self.config.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(self.config.MONGO_URI)
        self.db = self.config.mongo_client[self.config.MONGO_DB_NAME]
        self.config.db = self.db
        await ensure_indexes()
        logger.info("Connected to MongoDB")

        # Create aiohttp session
        self.session = aiohttp.ClientSession()

        # Initialize components
        self.proxy_repo = ProxyRepository(self.db)
        self.source_manager = ProxySourceManager(self.db, self.session)
        self.tester = ProxyTester(self.db, self.session)
        self.report_manager = ReportManager(self.db, self.bot, self.config)
        self.alert_manager = AlertManager(self.bot, self.config.OWNER_ID)

        # Initialize default sources
        await self.source_manager.initialize_default_sources()

        # Initialize scheduler
        self.scheduler = WorkerScheduler(
            self.db,
            self.source_manager,
            self.tester,
            self.bot,
            self.report_manager,
            self.alert_manager
        )

        # Initialize Telegram bot
        self.bot = AsyncTeleBot(self.config.BOT_TOKEN)
        self.config.bot = self.bot

        # Set report manager bot (it was None before)
        self.report_manager.bot = self.bot
        self.alert_manager.bot = self.bot

        # Set up Telegram UI
        self.telegram_ui = TelegramAdminUI(
            self.bot,
            self.db,
            self.config,
            self.scheduler,
            self.source_manager,
            self.tester,
            self.report_manager,
            self.alert_manager
        )
        await self.telegram_ui.setup_handlers()

        # Set up health server
        self.health_server = HealthServer(self.config, self.db, self.scheduler)

        # Set scheduler's bot reference
        self.scheduler.bot = self.bot

        # Start health server
        await self.health_server.start()

        # Start scheduler
        await self.scheduler.start()

        # Send startup notification
        try:
            await self.bot.send_message(
                self.config.OWNER_ID,
                "🟢 Proxy Worker Bot started."
            )
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")

        # Schedule daily report task
        asyncio.create_task(self._daily_report_loop())

        logger.info("Worker initialization complete")

    async def _daily_report_loop(self):
        while True:
            await asyncio.sleep(60 * 60)  # check every hour
            now = utcnow()
            if now.hour == self.config.DAILY_REPORT_HOUR:
                await self.report_manager.send_daily_report()
                # sleep to avoid sending multiple times in same hour
                await asyncio.sleep(3600)

    async def run(self):
        """Run the bot main loop."""
        await self.initialize()
        # Start Telegram polling
        await self.bot.polling(non_stop=True)

    async def shutdown(self, sig=None):
        """Graceful shutdown."""
        logger.info("Shutting down...")
        if self.scheduler:
            await self.scheduler.stop()
        if self.health_server:
            await self.health_server.stop()
        if self.session:
            await self.session.close()
        if self.config.mongo_client:
            self.config.mongo_client.close()
        logger.info("Shutdown complete")
        sys.exit(0)


# ======================================================================
# ENTRY POINT
# ======================================================================
async def main():
    worker = ProxyWorkerBot()
    try:
        await worker.run()
    except (KeyboardInterrupt, SystemExit):
        await worker.shutdown()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        await worker.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
