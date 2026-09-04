#!/usr/bin/env python3
"""
Dedicated Proxy Worker Bot - Multi-Platform Implementation (v3).
Comprehensive upgrade integrating:
- Platforms: YouTube + Instagram + TikTok
- Per-platform MongoDB collections (proxies, proxies_instagram, proxies_tiktok)
- Staged revalidation state machine (WORKING -> QUARANTINED -> DISABLED after 48h)
- Per-platform Telegram log channels for verified & recovered proxy alerts
- Manual single-proxy & multi-proxy priority checking with queue pause/resume
- Paste/file ingestion (.txt, .json, .csv) with duplicate detection
- Auth missing detection (requires_auth_missing tracking)
- Bandwidth-conscious test pacing, leases, and TCP pre-check
- Multi-source auto-discovery (GitHub sibling file crawler)
- Reorganized per-platform Telegram Admin UI panels
- 10 Advanced Features: Latency ranking, Geo-diversity guard, Validator plugins,
  Failure-reason taxonomy, Source health scoring, Adaptive quarantine step-down,
  Per-platform daily digest, HTTP 429 backoff, Test-target rotation, Self-healing resolver.
"""
from __future__ import annotations
import asyncio
import csv
import hashlib
import io
import ipaddress
import json
import logging
import os
import random
import re
import shutil
import signal
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, unquote, urlparse
import aiohttp
from aiohttp import web
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, ReturnDocument
from pymongo.errors import OperationFailure
try:
    from pyrogram import Client, filters
    from pyrogram.errors import FloodWait
    from pyrogram.types import (
        CallbackQuery,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        Message,
    )
except ImportError:
    Client = None
    filters = None
    FloodWait = Exception
    InlineKeyboardButton = None
    InlineKeyboardMarkup = None
    Message = Any
    CallbackQuery = Any
try:
    from aiohttp_socks import ProxyConnector
except ImportError:
    ProxyConnector = None
# ============================================================================
# CONFIGURATION & ENVIRONMENT
# ============================================================================
def env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}
def env_int(name: str, default: int, minimum: int = 0, maximum: Optional[int] = None) -> int:
    try:
        val = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        val = default
    val = max(minimum, val)
    if maximum is not None:
        val = min(val, maximum)
    return val
def env_list(name: str, default: Tuple[str, ...]) -> Tuple[str, ...]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return tuple(x.strip() for x in raw.split(",") if x.strip())
class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
    OWNER_ID = env_int("OWNER_ID", 0, 1)
    MONGO_URI = os.getenv("MONGO_URI", "").strip()
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "telegram_downloader").strip()
    # Per-platform MongoDB collections
    PROXY_COLLECTION_YT = os.getenv("MONGO_COLLECTION", "proxies").strip()
    PROXY_COLLECTION_IG = os.getenv("MONGO_COLLECTION_IG", "proxies_instagram").strip()
    PROXY_COLLECTION_TT = os.getenv("MONGO_COLLECTION_TT", "proxies_tiktok").strip()
    PORT = env_int("PORT", 8080, 1, 65535)
    # Per-platform Telegram Log Channels
    YOUTUBE_LOG_CHANNEL_ID = env_int("YOUTUBE_LOG_CHANNEL_ID", OWNER_ID)
    INSTAGRAM_LOG_CHANNEL_ID = env_int("INSTAGRAM_LOG_CHANNEL_ID", OWNER_ID)
    TIKTOK_LOG_CHANNEL_ID = env_int("TIKTOK_LOG_CHANNEL_ID", OWNER_ID)
    ADMIN_CHAT_ID = env_int("REPORT_CHAT_ID", OWNER_ID, 1)
    # Concurrency & Schedulers
    SOURCE_REFRESH_SECONDS = env_int("SOURCE_REFRESH_SECONDS", 300, 30)
    TEST_CONCURRENCY = env_int("PROXY_TEST_CONCURRENCY", 10, 1, 100)
    MAX_PENDING_TESTS = env_int("MAX_PENDING_TESTS", 2000, 1, 20000)
    MAX_TEST_PER_REFRESH = env_int("MAX_PROXIES_PER_REFRESH", 300, 1, 5000)
    PER_PLATFORM_TEST_BUDGET = env_int("PER_PLATFORM_TEST_BUDGET", 150, 10, 2000)
    DISCOVERY_INTERVAL_SECONDS = env_int("DISCOVERY_INTERVAL_SECONDS", 1800, 300)
    # Timeouts
    CONNECT_CHECK_TIMEOUT = env_int("CONNECT_CHECK_TIMEOUT", 6, 1, 30)
    GENERIC_TIMEOUT = env_int("HTTP_CONNECT_TIMEOUT", 12, 3, 120)
    GEO_TIMEOUT = env_int("GEO_TIMEOUT", 10, 3, 60)
    YOUTUBE_TIMEOUT = env_int("YOUTUBE_TEST_TIMEOUT", 35, 10, 180)
    INSTAGRAM_TIMEOUT = env_int("INSTAGRAM_TEST_TIMEOUT", 15, 5, 60)
    TIKTOK_TIMEOUT = env_int("TIKTOK_TEST_TIMEOUT", 15, 5, 60)
    # State Machine Intervals
    WORKING_CHECK_INTERVAL = env_int("WORKING_CHECK_INTERVAL", 3600, 300)        # 1 hour
    QUARANTINE_CHECK_INTERVAL = env_int("QUARANTINE_CHECK_INTERVAL", 18000, 600)  # 5 hours
    QUARANTINE_HARD_CUTOFF = env_int("QUARANTINE_HARD_CUTOFF", 172800, 3600)     # 48 hours
    ORPHAN_RETIRE_AFTER_SECONDS = env_int("ORPHAN_RETIRE_AFTER_SECONDS", 7 * 86400, 3600)
    # Validation Flags & Targets (Target Rotation Pools)
    YOUTUBE_VALIDATION_ENABLED = env_bool("YOUTUBE_VALIDATION_ENABLED", True)
    INSTAGRAM_VALIDATION_ENABLED = env_bool("INSTAGRAM_VALIDATION_ENABLED", True)
    TIKTOK_VALIDATION_ENABLED = env_bool("TIKTOK_VALIDATION_ENABLED", True)
    YOUTUBE_TEST_URLS = env_list(
        "YOUTUBE_TEST_URLS",
        (
            "https://www.youtube.com/watch?v=jNQXAC9IVRw",
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "https://www.youtube.com/",
        ),
    )
    INSTAGRAM_TEST_URLS = env_list(
        "INSTAGRAM_TEST_URLS",
        (
            "https://www.instagram.com/",
            "https://www.instagram.com/explore/",
        ),
    )
    TIKTOK_TEST_URLS = env_list(
        "TIKTOK_TEST_URLS",
        (
            "https://www.tiktok.com/",
            "https://www.tiktok.com/explore",
        ),
    )
    YTDLP_BINARY = os.getenv("YTDLP_BINARY", "yt-dlp").strip()
    YTDLP_REMOTE_COMPONENTS = os.getenv("YTDLP_REMOTE_COMPONENTS", "").strip()
    ENABLE_GEO_LOOKUP = env_bool("ENABLE_GEO_LOOKUP", True)
    GEO_LOOKUP_URL = os.getenv("GEO_LOOKUP_URL", "https://ipwho.is/{ip}").strip()
    MAX_SOURCE_BYTES = env_int("MAX_SOURCE_BYTES", 50 * 1024 * 1024, 1024)
    MAX_DISCOVERED_PER_SOURCE = env_int("MAX_DISCOVERED_PER_SOURCE", 10000, 1, 100000)
    SOURCE_FAILURE_ALERT_THRESHOLD = env_int("SOURCE_FAILURE_ALERT_THRESHOLD", 3, 1, 20)
    MAX_RETRIES = env_int("NETWORK_RETRIES", 1, 0, 3)
    SOURCE_RESOLVE_CACHE_SECONDS = env_int("SOURCE_RESOLVE_CACHE_SECONDS", 6 * 3600, 300)
    PREFERRED_SOURCE_FORMATS = env_list("PREFERRED_SOURCE_FORMATS", ("json", "txt", "csv"))
    USER_AGENT = os.getenv(
        "HTTP_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    ).strip()
    REPORT_ENABLED = env_bool("REPORT_ENABLED", True)
    DAILY_REPORT_HOUR = env_int("DAILY_REPORT_HOUR", 9, 0, 23)
    DAILY_REPORT_MINUTE = env_int("DAILY_REPORT_MINUTE", 0, 0, 59)
    DEBUG = env_bool("DEBUG", False)
    @classmethod
    def validate(cls) -> None:
        missing = []
        if not cls.BOT_TOKEN:
            missing.append("BOT_TOKEN")
        if not cls.OWNER_ID:
            missing.append("OWNER_ID")
        if not cls.MONGO_URI:
            missing.append("MONGO_URI")
        if missing:
            raise RuntimeError("Missing required environment variables: " + ", ".join(missing))
        if not cls.YTDLP_BINARY:
            raise RuntimeError("YTDLP_BINARY cannot be empty.")
        if Client is not None:
            api_id = env_int("API_ID", 0)
            api_hash = os.getenv("API_HASH", "").strip()
            if not api_id or not api_hash:
                raise RuntimeError("API_ID and API_HASH are required when Pyrogram is installed.")
# ============================================================================
# LOGGING & SANITIZATION
# ============================================================================
class SecretFilter(logging.Filter):
    _patterns = (
        re.compile(r"(mongodb(?:\+srv)?://)([^/\s]+)@", re.I),
        re.compile(r"((?:https?|socks4|socks5)://)([^/\s:@]+):([^@\s]+)@", re.I),
    )
    def filter(self, record: logging.LogRecord) -> bool:
        text = str(record.msg)
        for pattern in self._patterns:
            if pattern.groups == 2:
                text = pattern.sub(r"\1***@", text)
            else:
                text = pattern.sub(r"\1***:***@", text)
        record.msg = text
        return True
logging.basicConfig(
    level=logging.DEBUG if Config.DEBUG else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("proxy-worker-v3")
logger.addFilter(SecretFilter())
UTC = timezone.utc
def now_utc() -> datetime:
    return datetime.now(UTC)
def parse_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return None
def short_error(value: Any, limit: int = 400) -> str:
    text = str(value or "").replace("\x00", " ").strip()
    return text[:limit]
def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default
def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default
def mask_proxy_string(proxy_url: str) -> str:
    try:
        p = urlparse(proxy_url)
        host = p.hostname or ""
        port = p.port or ""
        scheme = p.scheme or "http"
        return f"{scheme}://{host}:{port}"
    except Exception:
        return "<proxy>"
# ============================================================================
# STATES & FAILURE TAXONOMY
# ============================================================================
class PlatformState:
    WORKING = "WORKING"
    QUARANTINED = "QUARANTINED"
    DISABLED = "DISABLED"
class FailureCategory:
    CONNECTION_TIMEOUT = "tcp_timeout"
    CONNECTION_REFUSED = "connection_refused"
    DNS_FAILURE = "dns_failure"
    PROXY_PROTOCOL_FAILURE = "proxy_protocol_error"
    PROXY_AUTH_FAILURE = "proxy_auth_failure"
    AUTH_MISSING = "auth_missing_or_invalid"
    TARGET_UNAVAILABLE = "target_unavailable"
    HTTP_403 = "http_403"
    HTTP_429 = "http_429"
    RATE_LIMITED = "rate_limited"
    TLS_ERROR = "tls_error"
    EXTRACTION_FAILURE = "extraction_failure"
    ENVIRONMENT_ERROR = "environment_error"
    SUCCESS = "success"
    UNKNOWN = "unknown"
    NON_ROUTE_SPECIFIC = frozenset(
        {ENVIRONMENT_ERROR, TARGET_UNAVAILABLE, RATE_LIMITED, HTTP_429}
    )
ALL_PLATFORMS = ("youtube", "instagram", "tiktok")
# ============================================================================
# PROXY DATA MODEL & MULTI-FORMAT PARSING
# ============================================================================
@dataclass(frozen=True)
class ProxyEntry:
    scheme: str
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    source_id: Optional[str] = None
    source_country: Optional[str] = None
    requires_auth_missing: bool = False
    @property
    def canonical(self) -> str:
        auth = ""
        if self.username is not None:
            auth = (
                quote(self.username, safe="")
                + ":"
                + quote(self.password or "", safe="")
                + "@"
            )
        host = self.host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"{self.scheme.lower()}://{auth}{host}:{self.port}"
    @property
    def proxy_id(self) -> str:
        return hashlib.sha256(self.canonical.encode("utf-8")).hexdigest()
SUPPORTED_SCHEMES = {"http", "https", "socks4", "socks5"}
PROXY_RE = re.compile(
    r"^(?:(?P<scheme>https?|socks4|socks5)://)?"
    r"(?:(?P<user>[^:@/\s]+)(?::(?P<password>[^@/\s]*))?@)?"
    r"(?P<host>\[[0-9a-fA-F:]+\]|[^:/\s]+):"
    r"(?P<port>\d{1,5})/?$",
    re.I,
)
def canonical_host(host: str) -> str:
    host = host.strip().lower().rstrip(".")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    return host
def parse_proxy_string(value: str, default_scheme: str = "http") -> Optional[ProxyEntry]:
    raw = str(value or "").strip().strip("`'\" ,;")
    if not raw:
        return None
    raw = re.sub(r"^(?:proxy|server|address)\s*[:=]\s*", "", raw, flags=re.I)
    match = PROXY_RE.match(raw)
    if not match:
        return None
    scheme = (match.group("scheme") or default_scheme).lower()
    if scheme not in SUPPORTED_SCHEMES:
        return None
    host = canonical_host(match.group("host"))
    try:
        port = int(match.group("port"))
    except ValueError:
        return None
    if not (1 <= port <= 65535) or not host or len(host) > 253:
        return None
    if any(ch.isspace() for ch in host):
        return None
    user = match.group("user")
    pwd = match.group("password")
    # Feature 6: Detect missing credentials when auth is implied
    requires_auth_missing = bool(user is not None and not pwd)
    return ProxyEntry(
        scheme=scheme,
        host=host,
        port=port,
        username=user,
        password=pwd,
        requires_auth_missing=requires_auth_missing,
    )
@dataclass
class ParsedCandidate:
    raw: str
    scheme_hint: Optional[str] = None
    country: Optional[str] = None
    anonymity: Optional[str] = None
def parse_txt_payload(text: str) -> List[ParsedCandidate]:
    lines = re.split(r"[\r\n]+", text)
    out = []
    for line in lines:
        line = line.strip().strip(",;")
        if not line or line.startswith("#"):
            continue
        out.append(ParsedCandidate(raw=line))
    return out
def parse_csv_payload(text: str) -> List[ParsedCandidate]:
    out: List[ParsedCandidate] = []
    try:
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
    except Exception:
        return out
    if not rows:
        return out
    header = [c.strip().lower() for c in rows[0]]
    has_header = any(h in header for h in ("ip", "host", "port", "proxy"))
    data_rows = rows[1:] if has_header else rows
    def col(row: List[str], name: str) -> Optional[str]:
        if not has_header or name not in header:
            return None
        idx = header.index(name)
        return row[idx].strip() if idx < len(row) else None
    for row in data_rows:
        if not row:
            continue
        if has_header:
            ip = col(row, "ip") or col(row, "host")
            port = col(row, "port")
            proto = col(row, "protocol") or col(row, "scheme") or col(row, "type")
            country = col(row, "country") or col(row, "country_code") or col(row, "cc")
            anon = col(row, "anonymity")
            if ip and port:
                candidate = f"{proto + '://' if proto else ''}{ip}:{port}"
                out.append(ParsedCandidate(raw=candidate, scheme_hint=proto, country=country, anonymity=anon))
        else:
            joined = ":".join(c.strip() for c in row if c.strip())
            if joined:
                out.append(ParsedCandidate(raw=row[0].strip()))
    return out
def parse_json_payload(text: str) -> List[ParsedCandidate]:
    out: List[ParsedCandidate] = []
    try:
        data = json.loads(text)
    except Exception:
        return out
    items: List[Any]
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("proxies") or data.get("data") or data.get("items") or []
        if not isinstance(items, list):
            items = []
    else:
        items = []
    for item in items:
        if isinstance(item, str):
            out.append(ParsedCandidate(raw=item))
            continue
        if not isinstance(item, dict):
            continue
        host = item.get("ip") or item.get("host") or item.get("address")
        port = item.get("port")
        proto = (item.get("protocol") or item.get("scheme") or item.get("type") or "").lower() or None
        anon = item.get("anonymity") or item.get("anonymityLevel")
        country = item.get("country") or item.get("country_code") or item.get("geo")
        geoloc = item.get("geolocation")
        if not country and isinstance(geoloc, dict):
            country = geoloc.get("country") or geoloc.get("country_code")
        if isinstance(item.get("proxy"), str) and item["proxy"].strip():
            out.append(ParsedCandidate(raw=item["proxy"], scheme_hint=proto, country=country, anonymity=anon))
        elif host and port:
            candidate = f"{proto + '://' if proto else ''}{host}:{port}"
            out.append(ParsedCandidate(raw=candidate, scheme_hint=proto, country=country, anonymity=anon))
    return out
def detect_format(content_type: str, url: str) -> str:
    ct = (content_type or "").lower()
    path = urlparse(url).path.lower()
    if "json" in ct or path.endswith(".json"):
        return "json"
    if "csv" in ct or path.endswith(".csv"):
        return "csv"
    return "txt"
def parse_source_payload(text: str, content_type: str, url: str = "") -> List[ParsedCandidate]:
    fmt = detect_format(content_type, url)
    if fmt == "json":
        parsed = parse_json_payload(text)
        if parsed:
            return parsed
        return parse_txt_payload(text)
    if fmt == "csv":
        parsed = parse_csv_payload(text)
        if parsed:
            return parsed
        return parse_txt_payload(text)
    return parse_txt_payload(text)
# ============================================================================
# DATABASE LAYER (Per-Platform Collections & Retention)
# ============================================================================
class Database:
    def __init__(self) -> None:
        self.client: Optional[AsyncMongoClient] = None
        self.db = None
        self.cols: Dict[str, Any] = {}
        self.sources = None
        self.tasks = None
        self.snapshots = None
        self.events = None
        self.daily = None
        self.worker_config = None
    def get_col(self, platform: str):
        return self.cols.get(platform, self.cols["youtube"])
    async def connect(self) -> None:
        self.client = AsyncMongoClient(
            Config.MONGO_URI,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=8000,
            socketTimeoutMS=20000,
            retryWrites=True,
        )
        await self.client.admin.command("ping")
        self.db = self.client[Config.MONGO_DB_NAME]
        # Sibling per-platform collections
        self.cols = {
            "youtube": self.db[Config.PROXY_COLLECTION_YT],
            "instagram": self.db[Config.PROXY_COLLECTION_IG],
            "tiktok": self.db[Config.PROXY_COLLECTION_TT],
        }
        self.sources = self.db["proxy_sources"]
        self.tasks = self.db["proxy_tasks"]
        self.snapshots = self.db["proxy_source_snapshots"]
        self.events = self.db["proxy_events"]
        self.daily = self.db["proxy_daily_summary"]
        self.worker_config = self.db["worker_config"]
        await self.ensure_indexes()
        logger.info("[DB] Connected to MongoDB with 3 per-platform collections.")
    async def ensure_indexes(self) -> None:
        for platform, col in self.cols.items():
            try:
                await col.create_index([("proxy_id", ASCENDING)], unique=True, sparse=True)
            except OperationFailure as e:
                if e.code == 86:
                    await col.drop_index("proxy_id_1")
                    await col.create_index([("proxy_id", ASCENDING)], unique=True, sparse=True)
                else:
                    raise
            await col.create_index([("enabled", ASCENDING), (f"platform_status.{platform}.state", ASCENDING)])
            await col.create_index([(f"platform_status.{platform}.next_check_at", ASCENDING)])
            await col.create_index([("ever_working", ASCENDING)])
            await col.create_index([("lease_until", ASCENDING)])
            await col.create_index([("latency_ms", ASCENDING)])
            await col.create_index([("verified_country", ASCENDING)])
            await col.create_index([("source_ids", ASCENDING)])
        await self.sources.create_index([("source_id", ASCENDING)], unique=True)
        await self.tasks.create_index([("task_id", ASCENDING)], unique=True)
        await self.events.create_index([("proxy_id", ASCENDING), ("created_at", DESCENDING)])
        await self.daily.create_index([("date", ASCENDING)], unique=True)
    async def ping(self) -> bool:
        try:
            if not self.client:
                return False
            await self.client.admin.command("ping")
            return True
        except Exception:
            return False
    async def close(self) -> None:
        if self.client:
            await self.client.close()
            self.client = None
    async def get_config(self, key: str, default: Any = None) -> Any:
        doc = await self.worker_config.find_one({"_id": key})
        return doc.get("value", default) if doc else default
    async def set_config(self, key: str, value: Any) -> None:
        await self.worker_config.update_one(
            {"_id": key},
            {"$set": {"value": value, "updated_at": now_utc()}},
            upsert=True,
        )
    # --- Sources management ---
    async def get_sources(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        query = {"enabled": True} if enabled_only else {}
        return await self.sources.find(query).sort("priority", DESCENDING).to_list(length=1000)
    async def get_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        return await self.sources.find_one({"source_id": source_id})
    async def upsert_source(self, source: Dict[str, Any], only_if_missing: bool = False) -> None:
        source_id = source["source_id"]
        source_copy = dict(source)
        source_copy.pop("_id", None)
        base_defaults = {
            "created_at": now_utc(),
            "failure_count": 0,
            "last_checked_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "last_content_hash": None,
            "last_item_count": 0,
            "resolved_url": None,
            "resolved_format": None,
            "resolved_at": None,
            "known_proxy_ids": [],
            "yield_working_count": 0,
            "yield_total_discovered": 0,
            "discovered": False,
        }
        if only_if_missing:
            await self.sources.update_one(
                {"source_id": source_id},
                {"$setOnInsert": {**base_defaults, **source_copy}},
                upsert=True,
            )
            return
        source_copy.setdefault("updated_at", now_utc())
        await self.sources.update_one(
            {"source_id": source_id},
            {"$set": source_copy, "$setOnInsert": base_defaults},
            upsert=True,
        )
    async def record_source_state(
        self,
        source_id: str,
        *,
        content_hash: Optional[str] = None,
        item_count: Optional[int] = None,
        resolved_url: Optional[str] = None,
        resolved_format: Optional[str] = None,
        known_proxy_ids: Optional[List[str]] = None,
        success: bool = False,
        error: Optional[str] = None,
    ) -> None:
        update: Dict[str, Any] = {"last_checked_at": now_utc(), "updated_at": now_utc()}
        if content_hash is not None:
            update["last_content_hash"] = content_hash
        if item_count is not None:
            update["last_item_count"] = item_count
            update["$inc"] = {"yield_total_discovered": item_count}
        if resolved_url is not None:
            update["resolved_url"] = resolved_url
            update["resolved_at"] = now_utc()
        if resolved_format is not None:
            update["resolved_format"] = resolved_format
        if known_proxy_ids is not None:
            update["known_proxy_ids"] = known_proxy_ids
        if success:
            update["last_success_at"] = now_utc()
            update["last_failure_at"] = None
            update["failure_count"] = 0
            await self.sources.update_one({"source_id": source_id}, {"$set": update})
        else:
            update["last_failure_at"] = now_utc()
            if error:
                update["last_error"] = short_error(error)
            inc = update.pop("$inc", {})
            inc["failure_count"] = 1
            await self.sources.update_one({"source_id": source_id}, {"$set": update, "$inc": inc})
    async def increment_source_yield(self, source_id: Optional[str]) -> None:
        if not source_id:
            return
        await self.sources.update_one({"source_id": source_id}, {"$inc": {"yield_working_count": 1}})
    async def update_source_health_score(self, source_id: Optional[str]) -> None:
        if not source_id:
            return
        source = await self.get_source(source_id)
        if not source:
            return
        total = max(1, safe_int(source.get("yield_total_discovered", 0)))
        working = safe_int(source.get("yield_working_count", 0))
        failures = safe_int(source.get("failure_count", 0))
        success_ratio = min(1.0, working / total)
        penalty = min(0.5, failures / 20.0)
        score = round(max(0.0, min(100.0, success_ratio * 100.0 - penalty * 100.0)), 1)
        await self.sources.update_one(
            {"source_id": source_id},
            {"$set": {"health_score": score, "health_updated_at": now_utc()}},
        )
    # --- Proxies ingestion & state persistence across 3 collections ---
    async def upsert_proxy_to_platforms(
        self, entry: ProxyEntry, country: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        now = now_utc()
        is_new_overall = False
        sample_doc: Dict[str, Any] = {}
        source_id = entry.source_id
        source_country = country or entry.source_country
        for platform, col in self.cols.items():
            existing = await col.find_one({"proxy_id": entry.proxy_id})
            if existing:
                source_ids = list(existing.get("source_ids") or [])
                if source_id and source_id not in source_ids:
                    source_ids.append(source_id)
                source_presence = dict(existing.get("source_presence") or {})
                if source_id:
                    source_presence[source_id] = True
                update: Dict[str, Any] = {
                    "last_seen_at": now,
                    "source_present": True if source_presence else existing.get("source_present", True),
                    "source_missing_since": None,
                    "source_ids": source_ids,
                    "source_presence": source_presence,
                    "enabled": existing.get("enabled", True),
                    "retired": False,
                }
                if source_country and not existing.get("source_country"):
                    update["source_country"] = source_country
                if entry.requires_auth_missing:
                    update["requires_auth_missing"] = True
                await col.update_one({"proxy_id": entry.proxy_id}, {"$set": update})
                sample_doc = {**existing, **update}
            else:
                is_new_overall = True
                source_presence = {source_id: True} if source_id else {}
                doc = {
                    "proxy_id": entry.proxy_id,
                    "proxy_url": entry.canonical,
                    "scheme": entry.scheme,
                    "host": entry.host,
                    "port": entry.port,
                    "username": entry.username,
                    "password": entry.password,
                    "source_ids": [source_id] if source_id else [],
                    "source_country": source_country,
                    "source_presence": source_presence,
                    "source_present": True if source_presence else False,
                    "source_missing_since": None,
                    "requires_auth_missing": entry.requires_auth_missing,
                    "enabled": True,
                    "retired": False,
                    "ever_working": False,
                    "verified_country": None,
                    "country_name": None,
                    "latency_ms": None,
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "last_tested_at": None,
                    "lease_until": None,
                    "platform_status": {
                        platform: {
                            "state": PlatformState.QUARANTINED,
                            "working": False,
                            "last_checked_at": None,
                            "next_check_at": now,
                            "quarantined_since": now,
                            "consecutive_fail_windows": 0,
                            "flap_recovery_count": 0,
                            "last_error": None,
                            "last_error_category": None,
                            "last_notified_state": None,
                        }
                    },
                }
                await col.insert_one(doc)
                sample_doc = doc
        return is_new_overall, sample_doc
    async def claim_proxy(self, platform: str, lease_seconds: int = 180) -> Optional[Dict[str, Any]]:
        now = now_utc()
        col = self.get_col(platform)
        # Find next eligible proxy for this platform
        query = {
            "enabled": True,
            "retired": False,
            f"platform_status.{platform}.state": {"$ne": PlatformState.DISABLED},
            f"platform_status.{platform}.next_check_at": {"$lte": now},
            "$or": [{"lease_until": None}, {"lease_until": {"$lte": now}}],
        }
        return await col.find_one_and_update(
            query,
            {"$set": {"lease_until": now + timedelta(seconds=lease_seconds)}},
            sort=[(f"platform_status.{platform}.next_check_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )
    async def release_lease(self, platform: str, proxy_id: str) -> None:
        await self.get_col(platform).update_one({"proxy_id": proxy_id}, {"$set": {"lease_until": None}})
    async def release_expired_leases(self) -> int:
        now = now_utc()
        released = 0
        for platform, col in self.cols.items():
            stale = await col.find(
                {"lease_until": {"$ne": None, "$lte": now}},
                {"proxy_id": 1},
            ).to_list(length=10000)
            ids = [d["proxy_id"] for d in stale if d.get("proxy_id")]
            if not ids:
                continue
            await col.update_many(
                {"proxy_id": {"$in": ids}},
                {
                    "$set": {
                        "lease_until": None,
                        f"platform_status.{platform}.next_check_at": now,
                    }
                },
            )
            released += len(ids)
        return released
    # --- Staged Revalidation State Machine    # --- Staged Revalidation State Machine (Requirement #1 & #6) ---
    async def record_platform_result(
        self, platform: str, proxy_id: str, result: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        now = now_utc()
        col = self.get_col(platform)
        doc = await col.find_one({"proxy_id": proxy_id})
        if not doc:
            return {}, {}
        p_stat = dict(doc.get("platform_status", {}).get(platform, {}))
        current_state = p_stat.get("state", PlatformState.QUARANTINED)
        consecutive_fails = safe_int(p_stat.get("consecutive_fail_windows", 0))
        q_since = parse_dt(p_stat.get("quarantined_since"))
        flap_count = safe_int(p_stat.get("flap_recovery_count", 0))
        success = bool(result.get("ok"))
        category = result.get("category", FailureCategory.UNKNOWN)
        error_msg = result.get("error")
        meta_update: Dict[str, Any] = {"lease_until": None, "last_tested_at": now}
        if result.get("latency_ms") is not None:
            meta_update["latency_ms"] = result["latency_ms"]
        if result.get("country_code"):
            meta_update["verified_country"] = str(result["country_code"]).upper()
            meta_update["country_name"] = result.get("country_name")
        if category == FailureCategory.AUTH_MISSING:
            meta_update["requires_auth_missing"] = True
        p_update: Dict[str, Any] = {
            "last_checked_at": now,
            "last_error": short_error(error_msg),
            "last_error_category": category,
        }
        transition_meta = {
            "was_working": current_state == PlatformState.WORKING,
            "now_working": False,
            "recovered": False,
            "permanently_disabled": False,
            "downtime_hours": 0.0,
        }
        if success:
            meta_update["ever_working"] = True
            p_update["state"] = PlatformState.WORKING
            p_update["working"] = True
            p_update["quarantined_since"] = None
            p_update["consecutive_fail_windows"] = 0
            p_update["next_check_at"] = now + timedelta(seconds=Config.WORKING_CHECK_INTERVAL)
            transition_meta["now_working"] = True
            if current_state == PlatformState.QUARANTINED:
                transition_meta["recovered"] = True
                p_update["flap_recovery_count"] = flap_count + 1
                if q_since:
                    transition_meta["downtime_hours"] = round(max(0.0, (now - q_since).total_seconds() / 3600.0), 1)
            if current_state != PlatformState.WORKING:
                for sid in doc.get("source_ids") or []:
                    await self.increment_source_yield(sid)
                    await self.update_source_health_score(sid)
        else:
            p_update["working"] = False
            if category in FailureCategory.NON_ROUTE_SPECIFIC:
                p_update["state"] = current_state
                p_update["working"] = current_state == PlatformState.WORKING
                p_update["next_check_at"] = now + timedelta(minutes=15)
            elif current_state == PlatformState.WORKING:
                p_update["state"] = PlatformState.QUARANTINED
                p_update["quarantined_since"] = now
                p_update["consecutive_fail_windows"] = 1
                base_wait = Config.QUARANTINE_CHECK_INTERVAL
                if flap_count > 0:
                    base_wait = max(1800, int(base_wait / (1 + flap_count * 0.5)))
                p_update["next_check_at"] = now + timedelta(seconds=base_wait)
            elif current_state == PlatformState.QUARANTINED:
                p_update["consecutive_fail_windows"] = consecutive_fails + 1
                effective_q_since = q_since or now
                elapsed = max(0.0, (now - effective_q_since).total_seconds())
                if elapsed >= Config.QUARANTINE_HARD_CUTOFF:
                    p_update["state"] = PlatformState.DISABLED
                    p_update["next_check_at"] = None
                    p_update["working"] = False
                    transition_meta["permanently_disabled"] = True
                else:
                    base_wait = Config.QUARANTINE_CHECK_INTERVAL
                    if flap_count > 0:
                        base_wait = max(1800, int(base_wait / (1 + flap_count * 0.5)))
                    p_update["next_check_at"] = now + timedelta(seconds=base_wait)
            else:
                p_update["state"] = PlatformState.DISABLED
                p_update["next_check_at"] = None
        meta_update[f"platform_status.{platform}"] = {**p_stat, **p_update}
        await col.update_one({"proxy_id": proxy_id}, {"$set": meta_update})
        merged = {**doc, **meta_update}
        if p_update.get("state") != current_state:
            await self.events.insert_one(
                {
                    "proxy_id": proxy_id,
                    "platform": platform,
                    "old_state": current_state,
                    "new_state": p_update.get("state"),
                    "created_at": now,
                    "error_category": category,
                }
            )
        return merged, transition_meta
    async def get_platform_stats(self, platform: str) -> Dict[str, int]:
        col = self.get_col(platform)
        total = await col.count_documents({})
        working = await col.count_documents({f"platform_status.{platform}.state": PlatformState.WORKING, "enabled": True})
        quarantined = await col.count_documents({f"platform_status.{platform}.state": PlatformState.QUARANTINED, "enabled": True})
        disabled = await col.count_documents({f"platform_status.{platform}.state": PlatformState.DISABLED})
        ever_working = await col.count_documents({"ever_working": True})
        return {
            "total": total,
            "working": working,
            "quarantined": quarantined,
            "disabled": disabled,
            "ever_working": ever_working,
        }
    async def retire_orphans(self) -> int:
        cutoff = now_utc() - timedelta(seconds=Config.ORPHAN_RETIRE_AFTER_SECONDS)
        retired = 0
        for col in self.cols.values():
            res = await col.update_many(
                {
                    "ever_working": False,
                    "source_present": False,
                    "source_missing_since": {"$lte": cutoff},
                    "retired": {"$ne": True},
                },
                {"$set": {"retired": True, "enabled": False}},
            )
            retired += res.modified_count
        return retired
    async def mark_missing_from_sources(self, source_id: str, proxy_ids: List[str]) -> None:
        if not source_id or not proxy_ids:
            return
        now = now_utc()
        for col in self.cols.values():
            docs = await col.find(
                {"proxy_id": {"$in": proxy_ids}},
                {"proxy_id": 1, "source_ids": 1, "source_presence": 1},
            ).to_list(length=10000)
            for doc in docs:
                presence = dict(doc.get("source_presence") or {})
                if not presence:
                    source_ids = [str(x) for x in (doc.get("source_ids") or []) if x]
                    if source_ids:
                        other_source_docs = await self.sources.find(
                            {
                                "source_id": {"$in": source_ids},
                                "known_proxy_ids": doc["proxy_id"],
                            },
                            {"source_id": 1},
                        ).to_list(length=1000)
                        present_ids = {str(item["source_id"]) for item in other_source_docs if item.get("source_id")}
                        presence = {sid: sid in present_ids for sid in source_ids}
                presence[source_id] = False
                any_present = any(bool(v) for v in presence.values())
                await col.update_one(
                    {"proxy_id": doc["proxy_id"]},
                    {
                        "$set": {
                            "source_presence": presence,
                            "source_present": any_present,
                            "source_missing_since": None if any_present else now,
                        }
                    },
                )
# ============================================================================
# VALIDATORS & PLUGIN ARCHITECTURE# ============================================================================
# VALIDATORS & PLUGIN ARCHITECTURE (Features 10.3, 10.4, 10.8, 10.9)
# ============================================================================
class BaseValidator:
    def __init__(self, platform: str, test_urls: Tuple[str, ...], timeout_seconds: int):
        self.platform = platform
        self.test_urls = test_urls
        self.timeout_seconds = timeout_seconds
        self.rate_limited_until: Optional[datetime] = None
    def pick_target_url(self) -> str:
        # Feature 10.9: Target rotation
        return random.choice(self.test_urls)
    def is_rate_limited(self) -> bool:
        if self.rate_limited_until and now_utc() < self.rate_limited_until:
            return True
        self.rate_limited_until = None
        return False
    def trigger_rate_limit_backoff(self, minutes: int = 15) -> None:
        # Feature 10.8: Rate-limit-aware validator backoff
        self.rate_limited_until = now_utc() + timedelta(minutes=minutes)
        logger.warning("[%s] 429 encountered. Backing off platform validator for %sm", self.platform, minutes)
    async def test(self, entry: ProxyEntry) -> Dict[str, Any]:
        raise NotImplementedError
class YouTubeValidator(BaseValidator):
    def __init__(self):
        super().__init__("youtube", Config.YOUTUBE_TEST_URLS, Config.YOUTUBE_TIMEOUT)
        self._ytdlp_version: Optional[str] = None
    async def get_version(self) -> Optional[str]:
        if self._ytdlp_version:
            return self._ytdlp_version
        try:
            proc = await asyncio.create_subprocess_exec(
                Config.YTDLP_BINARY,
                "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            if proc.returncode == 0:
                self._ytdlp_version = stdout.decode().strip()
        except Exception:
            pass
        return self._ytdlp_version
    def classify_error(self, stderr: str, stdout: str, auth_missing: bool) -> Tuple[str, str]:
        text = (stderr + "\n" + stdout).lower()
        if "sign in to confirm you're not a bot" in text or "not a bot" in text:
            return FailureCategory.RATE_LIMITED, "Bot detection / sign in required"
        if "429" in text or "too many requests" in text:
            return FailureCategory.HTTP_429, "HTTP 429 rate limited"
        if "proxy" in text and ("authentication" in text or "407" in text):
            return (
                FailureCategory.AUTH_MISSING if auth_missing else FailureCategory.PROXY_AUTH_FAILURE,
                "Proxy authentication rejected",
            )
        if "timed out" in text or "timeout" in text:
            return FailureCategory.CONNECTION_TIMEOUT, "yt-dlp extraction timeout"
        if "name or service not known" in text or "getaddrinfo" in text:
            return FailureCategory.DNS_FAILURE, "DNS resolution failed through proxy"
        if "connection refused" in text or "connection reset" in text:
            return FailureCategory.CONNECTION_REFUSED, "Proxy connection refused"
        if "certificate" in text or "tls" in text or "ssl" in text:
            return FailureCategory.TLS_ERROR, "TLS handshake failed"
        if "video unavailable" in text:
            return FailureCategory.TARGET_UNAVAILABLE, "Video unavailable in this region"
        return FailureCategory.EXTRACTION_FAILURE, short_error(stderr or stdout, 200)
    async def test(self, entry: ProxyEntry) -> Dict[str, Any]:
        if self.is_rate_limited():
            return {"ok": False, "category": FailureCategory.RATE_LIMITED, "error": "Platform validator backed off"}
        target_url = self.pick_target_url()
        started = time.monotonic()
        proxy_url = entry.canonical
        cmd = [
            Config.YTDLP_BINARY,
            "--dump-single-json",
            "--skip-download",
            "--no-playlist",
            "--no-warnings",
            "--quiet",
            "--socket-timeout",
            str(self.timeout_seconds),
            "--retries",
            "0",
            "--proxy",
            proxy_url,
            "--user-agent",
            Config.USER_AGENT,
        ]
        deno = shutil.which("deno")
        if deno:
            cmd += ["--js-runtimes", f"deno:{deno}"]
        if Config.YTDLP_REMOTE_COMPONENTS:
            cmd += ["--remote-components", Config.YTDLP_REMOTE_COMPONENTS]
        cmd += ["--", target_url]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self.timeout_seconds + 5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
                raise
            duration = time.monotonic() - started
            out = stdout.decode("utf-8", errors="replace")
            err = stderr.decode("utf-8", errors="replace")
            if proc.returncode == 0:
                try:
                    data = json.loads(out)
                    if data.get("formats") and data.get("title"):
                        return {
                            "ok": True,
                            "category": FailureCategory.SUCCESS,
                            "latency_ms": round(duration * 1000.0, 1),
                            "title": data.get("title", "")[:100],
                            "version": await self.get_version(),
                        }
                except Exception:
                    pass
            cat, msg = self.classify_error(err, out, entry.requires_auth_missing)
            if cat in (FailureCategory.HTTP_429, FailureCategory.RATE_LIMITED):
                self.trigger_rate_limit_backoff()
            return {"ok": False, "category": cat, "error": msg, "latency_ms": round(duration * 1000.0, 1)}
        except asyncio.TimeoutError:
            return {
                "ok": False,
                "category": FailureCategory.CONNECTION_TIMEOUT,
                "error": "yt-dlp process timed out",
                "latency_ms": round((time.monotonic() - started) * 1000.0, 1),
            }
        except Exception as exc:
            return {"ok": False, "category": FailureCategory.ENVIRONMENT_ERROR, "error": short_error(exc)}
class GenericHTTPValidator(BaseValidator):
    def __init__(self, platform: str, test_urls: Tuple[str, ...], timeout_seconds: int):
        super().__init__(platform, test_urls, timeout_seconds)
    async def test(self, entry: ProxyEntry) -> Dict[str, Any]:
        if self.is_rate_limited():
            return {"ok": False, "category": FailureCategory.RATE_LIMITED, "error": "Platform validator backed off"}
        target_url = self.pick_target_url()
        started = time.monotonic()
        proxy_url = entry.canonical
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        if entry.scheme.startswith("socks") and ProxyConnector is None:
            return {
                "ok": False,
                "category": FailureCategory.ENVIRONMENT_ERROR,
                "error": "aiohttp-socks is required for SOCKS validation",
            }
        try:
            if entry.scheme.startswith("socks"):
                connector = ProxyConnector.from_url(proxy_url)
                session_ctx = aiohttp.ClientSession(
                    connector=connector, timeout=timeout, headers={"User-Agent": Config.USER_AGENT}
                )
            else:
                session_ctx = aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": Config.USER_AGENT})
            async with session_ctx as session:
                kwargs = {} if entry.scheme.startswith("socks") else {"proxy": proxy_url}
                async with session.get(target_url, allow_redirects=True, **kwargs) as resp:
                    duration = time.monotonic() - started
                    if resp.status == 429:
                        self.trigger_rate_limit_backoff()
                        return {
                            "ok": False,
                            "category": FailureCategory.HTTP_429,
                            "error": "HTTP 429 Too Many Requests",
                            "latency_ms": round(duration * 1000.0, 1),
                        }
                    if resp.status == 403:
                        return {
                            "ok": False,
                            "category": FailureCategory.HTTP_403,
                            "error": "HTTP 403 Forbidden",
                            "latency_ms": round(duration * 1000.0, 1),
                        }
                    if resp.status == 407:
                        return {
                            "ok": False,
                            "category": FailureCategory.AUTH_MISSING,
                            "error": "Proxy authentication required",
                            "latency_ms": round(duration * 1000.0, 1),
                        }
                    if resp.status < 400:
                        return {
                            "ok": True,
                            "category": FailureCategory.SUCCESS,
                            "latency_ms": round(duration * 1000.0, 1),
                        }
                    return {
                        "ok": False,
                        "category": FailureCategory.TARGET_UNAVAILABLE,
                        "error": f"HTTP {resp.status}",
                        "latency_ms": round(duration * 1000.0, 1),
                    }
        except asyncio.TimeoutError:
            return {
                "ok": False,
                "category": FailureCategory.CONNECTION_TIMEOUT,
                "error": "HTTP connect timeout",
                "latency_ms": round((time.monotonic() - started) * 1000.0, 1),
            }
        except Exception as exc:
            text = str(exc).lower()
            if "407" in text or "auth" in text:
                cat = FailureCategory.AUTH_MISSING if entry.requires_auth_missing else FailureCategory.PROXY_AUTH_FAILURE
            elif "ssl" in text or "cert" in text:
                cat = FailureCategory.TLS_ERROR
            elif "refused" in text:
                cat = FailureCategory.CONNECTION_REFUSED
            elif "getaddrinfo" in text:
                cat = FailureCategory.DNS_FAILURE
            else:
                cat = FailureCategory.PROXY_PROTOCOL_FAILURE
            return {
                "ok": False,
                "category": cat,
                "error": short_error(exc),
                "latency_ms": round((time.monotonic() - started) * 1000.0, 1),
            }
class ValidationEngine:
    def __init__(self) -> None:
        self.validators: Dict[str, BaseValidator] = {
            "youtube": YouTubeValidator(),
            "instagram": GenericHTTPValidator("instagram", Config.INSTAGRAM_TEST_URLS, Config.INSTAGRAM_TIMEOUT),
            "tiktok": GenericHTTPValidator("tiktok", Config.TIKTOK_TEST_URLS, Config.TIKTOK_TIMEOUT),
        }
    @staticmethod
    async def tcp_connect_check(entry: ProxyEntry) -> bool:
        """Cheapest pre-check: pure TCP syn/ack, zero external payload bandwidth."""
        try:
            fut = asyncio.open_connection(entry.host, entry.port)
            reader, writer = await asyncio.wait_for(fut, timeout=Config.CONNECT_CHECK_TIMEOUT)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            return False
    async def resolve_ip_and_country(self, entry: ProxyEntry) -> Dict[str, Any]:
        """Generic connectivity + exit IP resolution."""
        if not Config.ENABLE_GEO_LOOKUP:
            return {}
        proxy_url = entry.canonical
        timeout = aiohttp.ClientTimeout(total=Config.GENERIC_TIMEOUT)
        try:
            if entry.scheme.startswith("socks") and ProxyConnector is not None:
                connector = ProxyConnector.from_url(proxy_url)
                session = aiohttp.ClientSession(connector=connector, timeout=timeout)
                async with session:
                    async with session.get("https://api.ipify.org?format=json") as r:
                        if r.status == 200:
                            data = await r.json(content_type=None)
                            ip = str(data.get("ip", "")).strip()
            else:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get("https://api.ipify.org?format=json", proxy=proxy_url) as r:
                        if r.status == 200:
                            data = await r.json(content_type=None)
                            ip = str(data.get("ip", "")).strip()
            if not ip:
                return {}
            geo_url = Config.GEO_LOOKUP_URL.replace("{ip}", quote(ip, safe=""))
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=Config.GEO_TIMEOUT)) as s:
                async with s.get(geo_url) as gr:
                    if gr.status == 200:
                        gdata = await gr.json(content_type=None)
                        return {
                            "exit_ip": ip,
                            "country_code": (gdata.get("country_code") or gdata.get("country") or "").upper() or None,
                            "country_name": gdata.get("country") or gdata.get("country_name"),
                        }
        except Exception:
            pass
        return {}
    async def validate(self, proxy_doc: Dict[str, Any], platform: str) -> Dict[str, Any]:
        entry = ProxyEntry(
            scheme=proxy_doc.get("scheme", "http"),
            host=proxy_doc["host"],
            port=safe_int(proxy_doc["port"]),
            username=proxy_doc.get("username"),
            password=proxy_doc.get("password"),
            source_country=proxy_doc.get("source_country"),
            requires_auth_missing=proxy_doc.get("requires_auth_missing", False),
        )
        if not await self.tcp_connect_check(entry):
            return {
                "ok": False,
                "category": FailureCategory.CONNECTION_TIMEOUT,
                "error": "TCP connect to proxy port timed out / refused",
            }
        geo_data: Dict[str, Any] = {}
        if not (proxy_doc.get("verified_country") or proxy_doc.get("source_country")):
            geo_data = await self.resolve_ip_and_country(entry)
        validator = self.validators.get(platform)
        if not validator:
            return {"ok": False, "category": FailureCategory.ENVIRONMENT_ERROR, "error": f"No validator for {platform}"}
        res = await validator.test(entry)
        if proxy_doc.get("source_country") and not res.get("country_code"):
            res["country_code"] = str(proxy_doc["source_country"]).upper()
        return {**geo_data, **res}
# ============================================================================
# SOURCE MANAGER & AUTO-DISCOVERY# ============================================================================
# SOURCE MANAGER & AUTO-DISCOVERY (Features 8, 10.5, 10.10)
# ============================================================================
class ProxySourceManager:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.session: Optional[aiohttp.ClientSession] = None
    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=Config.GENERIC_TIMEOUT * 2)
        self.session = aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": Config.USER_AGENT})
    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None
    @staticmethod
    def _is_github_repo_url(url: str) -> bool:
        return urlparse(url).netloc.lower() == "github.com"
    async def _list_github_directory(self, owner: str, repo: str, branch: str, path: str) -> List[Dict[str, Any]]:
        api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{quote(path)}?ref={quote(branch)}"
        async with self.session.get(api_url, headers={"Accept": "application/vnd.github+json"}) as response:
            if response.status != 200:
                raise RuntimeError(f"GitHub directory listing failed: HTTP {response.status}")
            data = await response.json(content_type=None)
        if not isinstance(data, list):
            raise RuntimeError("GitHub contents API did not return a directory list.")
        return data
    def _pick_preferred_file(self, entries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        by_ext: Dict[str, Dict[str, Any]] = {}
        for entry in entries:
            if entry.get("type") != "file":
                continue
            name = str(entry.get("name", "")).lower()
            for ext in ("json", "txt", "csv"):
                if name.endswith(f".{ext}"):
                    by_ext.setdefault(ext, entry)
        for ext in Config.PREFERRED_SOURCE_FORMATS:
            if ext in by_ext:
                return by_ext[ext]
        for entry in entries:
            if entry.get("type") == "file":
                return entry
        return None
    async def resolve_source_url(self, source: Dict[str, Any], force_re_resolve: bool = False) -> Tuple[str, str]:
        url = str(source["url"]).strip()
        resolved_url = source.get("resolved_url")
        resolved_at = parse_dt(source.get("resolved_at"))
        if not force_re_resolve and resolved_url and resolved_at:
            age = (now_utc() - resolved_at).total_seconds()
            if age < Config.SOURCE_RESOLVE_CACHE_SECONDS:
                return resolved_url, source.get("resolved_format") or detect_format("", resolved_url)
        if not self._is_github_repo_url(url):
            return url, detect_format("", url)
        parsed = urlparse(url)
        parts = [unquote(x) for x in parsed.path.split("/") if x]
        if len(parts) < 2:
            return url, detect_format("", url)
        owner, repo = parts[0], parts[1]
        ref_type = parts[2] if len(parts) >= 3 else ""
        if ref_type == "blob" and len(parts) >= 5:
            branch = parts[3]
            file_path = "/".join(parts[4:])
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}"
            fmt = detect_format("", raw_url)
            await self.db.record_source_state(source["source_id"], resolved_url=raw_url, resolved_format=fmt)
            return raw_url, fmt
        if ref_type == "tree":
            branch = parts[3] if len(parts) >= 4 else "main"
            path = "/".join(parts[4:]) if len(parts) >= 5 else ""
            entries = await self._list_github_directory(owner, repo, branch, path)
            chosen = self._pick_preferred_file(entries)
            if not chosen or not chosen.get("download_url"):
                raise RuntimeError("No usable proxy file found in GitHub tree.")
            raw_url = str(chosen["download_url"])
            fmt = detect_format("", raw_url)
            await self.db.record_source_state(source["source_id"], resolved_url=raw_url, resolved_format=fmt)
            return raw_url, fmt
        return url, detect_format("", url)
    async def fetch(self, fetch_url: str) -> Tuple[str, str, int]:
        max_bytes = Config.MAX_SOURCE_BYTES
        last_exc: Optional[Exception] = None
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=Config.GENERIC_TIMEOUT * 3)
                async with self.session.get(fetch_url, timeout=timeout, allow_redirects=True) as resp:
                    if resp.status >= 400:
                        raise RuntimeError(f"HTTP {resp.status}")
                    ct = resp.headers.get("Content-Type", "")
                    body = bytearray()
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        body.extend(chunk)
                        if len(body) > max_bytes:
                            raise RuntimeError("Source payload exceeds MAX_SOURCE_BYTES.")
                    raw = bytes(body)
                    return raw.decode("utf-8", errors="replace"), ct, len(raw)
            except Exception as exc:
                last_exc = exc
                if attempt < Config.MAX_RETRIES:
                    await asyncio.sleep(2**attempt)
        raise last_exc or RuntimeError("Fetch error.")
    async def import_source(self, source: Dict[str, Any]) -> Dict[str, Any]:
        source_id = source["source_id"]
        try:
            fetch_url, fmt = await self.resolve_source_url(source)
            text, content_type, byte_count = await self.fetch(fetch_url)
        except Exception as first_exc:
            logger.warning(
                "[SOURCE] Initial fetch failed for %s, trying fresh directory resolution: %s",
                source_id,
                short_error(first_exc),
            )
            try:
                fetch_url, fmt = await self.resolve_source_url(source, force_re_resolve=True)
                text, content_type, byte_count = await self.fetch(fetch_url)
            except Exception as second_exc:
                await self.db.record_source_state(source_id, success=False, error=short_error(second_exc))
                await self.db.update_source_health_score(source_id)
                raise
        content_hash = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
        if source.get("last_content_hash") == content_hash:
            await self.db.record_source_state(source_id, content_hash=content_hash, success=True)
            return {"source_id": source_id, "unchanged": True, "added": 0, "total": source.get("last_item_count", 0)}
        candidates = parse_source_payload(text, content_type, fetch_url)
        if len(candidates) > Config.MAX_DISCOVERED_PER_SOURCE:
            candidates = candidates[: Config.MAX_DISCOVERED_PER_SOURCE]
        seen_ids: Set[str] = set()
        new_count = 0
        known_ids: List[str] = []
        for c in candidates:
            entry = parse_proxy_string(c.raw, default_scheme=c.scheme_hint or "http")
            if not entry:
                continue
            entry = ProxyEntry(
                scheme=entry.scheme,
                host=entry.host,
                port=entry.port,
                username=entry.username,
                password=entry.password,
                source_id=source_id,
                source_country=c.country or source.get("country"),
                requires_auth_missing=entry.requires_auth_missing,
            )
            if entry.proxy_id in seen_ids:
                continue
            seen_ids.add(entry.proxy_id)
            known_ids.append(entry.proxy_id)
            is_new, _ = await self.db.upsert_proxy_to_platforms(entry, country=entry.source_country)
            if is_new:
                new_count += 1
        # Track missing proxies from this specific source
        prev_ids = set(source.get("known_proxy_ids") or [])
        missing_ids = list(prev_ids - seen_ids)
        if missing_ids:
            await self.db.mark_missing_from_sources(source_id, missing_ids)
        await self.db.record_source_state(
            source_id,
            content_hash=content_hash,
            item_count=len(known_ids),
            known_proxy_ids=known_ids,
            success=True,
        )
        await self.db.update_source_health_score(source_id)
        return {"source_id": source_id, "unchanged": False, "added": new_count, "total": len(known_ids)}
    async def run_discovery_pass(self) -> int:
        sources = await self.db.get_sources(enabled_only=True)
        discovered_count = 0
        for src_doc in sources:
            url = str(src_doc.get("url", "")).strip()
            if not self._is_github_repo_url(url):
                continue
            parsed = urlparse(url)
            parts = [unquote(x) for x in parsed.path.split("/") if x]
            if len(parts) < 2:
                continue
            owner, repo = parts[0], parts[1]
            ref_type = parts[2] if len(parts) >= 3 else ""
            if ref_type == "tree":
                branch = parts[3] if len(parts) >= 4 else "main"
                path = "/".join(parts[4:]) if len(parts) >= 5 else ""
            elif ref_type == "blob":
                branch = parts[3] if len(parts) >= 4 else "main"
                path = "/".join(parts[4:-1]) if len(parts) >= 6 else ""
            else:
                branch = "main"
                path = ""
            try:
                entries = await self._list_github_directory(owner, repo, branch, path)
            except Exception as exc:
                logger.warning("[DISCOVERY] %s: %s", src_doc.get("name", "source"), short_error(exc))
                continue
            for item in entries:
                if item.get("type") != "file":
                    continue
                fname = str(item.get("name", "")).lower()
                if not any(fname.endswith(f".{ext}") for ext in ("txt", "json", "csv")):
                    continue
                if not any(k in fname for k in ("proxy", "proxies", "http", "socks", "list")):
                    continue
                raw_download = item.get("download_url")
                if not raw_download:
                    continue
                cand_id = hashlib.sha1(str(raw_download).encode()).hexdigest()[:16]
                if await self.db.get_source(cand_id):
                    continue
                await self.db.upsert_source(
                    {
                        "source_id": cand_id,
                        "name": f"Auto: {repo}/{item.get('name')}",
                        "url": str(raw_download),
                        "enabled": True,
                        "discovered": True,
                        "priority": 50,
                        "fetch_interval": Config.SOURCE_REFRESH_SECONDS * 2,
                    },
                    only_if_missing=True,
                )
                discovered_count += 1
        return discovered_count
# ============================================================================
# SCHEDULER & DISPATCHER# ============================================================================
# SCHEDULER & DISPATCHER (Features 1, 4, 7)
# ============================================================================
class WorkerScheduler:
    def __init__(
        self,
        db: Database,
        sources: ProxySourceManager,
        engine: ValidationEngine,
        notify_func: Callable[[str, str], Any],
    ) -> None:
        self.db = db
        self.sources = sources
        self.engine = engine
        self.notify_func = notify_func
        self.running = False
        self.stop_event = asyncio.Event()
        self.pause_event = asyncio.Event()
        self.pause_event.set()
        self.claim_lock = asyncio.Lock()
        self.manual_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(Config.TEST_CONCURRENCY)
        self.active_tests = 0
        self.platform_tasks: List[asyncio.Task] = []
        self.periodic_task: Optional[asyncio.Task] = None
        self.discovery_task: Optional[asyncio.Task] = None
    async def start(self) -> None:
        self.running = True
        self.stop_event.clear()
        self.pause_event.set()
        self.platform_tasks.clear()
        self.periodic_task = None
        self.discovery_task = None
        # Dedicated background dispatcher per platform
        for plat in ALL_PLATFORMS:
            t = asyncio.create_task(self.platform_dispatch_loop(plat), name=f"dispatcher-{plat}")
            self.platform_tasks.append(t)
        self.periodic_task = asyncio.create_task(self.periodic_scheduler_loop(), name="scheduler-periodic")
        self.discovery_task = asyncio.create_task(self.discovery_scheduler_loop(), name="scheduler-discovery")
        logger.info("[SCHEDULER] All platform dispatchers and scheduler loops initialized.")
    async def stop(self) -> None:
        self.running = False
        self.stop_event.set()
        self.pause_event.set()
        for t in self.platform_tasks:
            t.cancel()
        if self.periodic_task:
            self.periodic_task.cancel()
        if self.discovery_task:
            self.discovery_task.cancel()
        tasks = list(self.platform_tasks)
        if self.periodic_task:
            tasks.append(self.periodic_task)
        if self.discovery_task:
            tasks.append(self.discovery_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.platform_tasks.clear()
        self.periodic_task = None
        self.discovery_task = None
        logger.info("[SCHEDULER] All tasks successfully stopped.")
    async def platform_dispatch_loop(self, platform: str) -> None:
        while not self.stop_event.is_set():
            await self.pause_event.wait()
            enabled = await self.db.get_config(
                f"{platform}_validation_enabled",
                getattr(Config, f"{platform.upper()}_VALIDATION_ENABLED", True),
            )
            if not enabled:
                await asyncio.sleep(2)
                continue
            async with self.claim_lock:
                await self.pause_event.wait()
                if self.stop_event.is_set() or not self.pause_event.is_set():
                    continue
                doc = await self.db.claim_proxy(platform)
            if not doc:
                await asyncio.sleep(2)
                continue
            async with self.semaphore:
                self.active_tests += 1
                try:
                    res = await self.engine.validate(doc, platform)
                    updated, meta = await self.db.record_platform_result(platform, doc["proxy_id"], res)
                    if updated:
                        await self._handle_platform_notification(platform, updated, meta)
                except Exception as exc:
                    logger.exception("[DISPATCH] Test task failed for %s on %s", doc.get("proxy_id", "")[:8], platform)
                    await self.db.release_lease(platform, doc["proxy_id"])
                finally:
                    self.active_tests -= 1
    async def _handle_platform_notification(self, platform: str, doc: Dict[str, Any], meta: Dict[str, Any]) -> None:
        if not meta.get("now_working"):
            return
        country = doc.get("verified_country") or doc.get("source_country") or "UNKNOWN"
        source_name = doc.get("source_ids", ["manual"])[0] if doc.get("source_ids") else "manual"
        t_str = now_utc().strftime("%Y-%m-%d %H:%M UTC")
        status_line = "first-time verified"
        if meta.get("recovered"):
            status_line = f"recovered after quarantine ({meta.get('downtime_hours', 0.0)}h downtime)"
        lines = [
            f"✅ WORKING — {platform.title()}",
            "```",
            f"proxy: {doc.get('proxy_url')}",
            f"country: {country}",
            f"source: {source_name}",
            f"status: {status_line}",
            f"checked_at: {t_str}",
            "```",
        ]
        await self.notify_func(platform, "\n".join(lines))
    async def manual_priority_check(self, proxies_raw: List[str]) -> str:
        unique_raw = list(dict.fromkeys(str(x).strip() for x in proxies_raw if str(x).strip()))
        if not unique_raw:
            return "No valid proxies supplied."
        async with self.manual_lock:
            async with self.claim_lock:
                self.pause_event.clear()
            while self.active_tests > 0 and not self.stop_event.is_set():
                await asyncio.sleep(0.1)
            results_summary: List[str] = []
            try:
                for raw in unique_raw[:10]:
                    entry = parse_proxy_string(raw)
                    if not entry:
                        results_summary.append(f"❌ `{raw}` — Invalid proxy string format")
                        continue
                    await self.db.upsert_proxy_to_platforms(entry, country=entry.source_country)
                    async def test_platform(platform: str):
                        enabled = await self.db.get_config(
                            f"{platform}_validation_enabled",
                            getattr(Config, f"{platform.upper()}_VALIDATION_ENABLED", True),
                        )
                        if not enabled:
                            return platform, {"ok": False, "error": "Platform validation disabled"}
                        async with self.semaphore:
                            self.active_tests += 1
                            try:
                                doc = await self.db.get_col(platform).find_one({"proxy_id": entry.proxy_id})
                                if not doc:
                                    return platform, {"ok": False, "error": "Proxy record not found"}
                                res = await self.engine.validate(doc, platform)
                                updated, meta = await self.db.record_platform_result(platform, entry.proxy_id, res)
                                if updated:
                                    await self._handle_platform_notification(platform, updated, meta)
                                return platform, res
                            finally:
                                self.active_tests -= 1
                    plat_results = await asyncio.gather(
                        *(test_platform(p) for p in ALL_PLATFORMS),
                        return_exceptions=True,
                    )
                    working_on: List[str] = []
                    failures: List[str] = []
                    for result in plat_results:
                        if isinstance(result, tuple):
                            pname, res = result
                            if res.get("ok"):
                                working_on.append(pname.title())
                            else:
                                failures.append(f"{pname.title()}: {res.get('error', 'failed')}")
                    proxy_masked = mask_proxy_string(entry.canonical)
                    if working_on:
                        msg = f"✅ `{proxy_masked}` — Working on: {', '.join(working_on)}"
                        if failures:
                            msg += f" (Failed: {'; '.join(failures)})"
                    else:
                        msg = f"❌ `{proxy_masked}` — Not working on any platform:\n  " + (("\n  ".join(failures)) or "No platform result.")
                    results_summary.append(msg)
            finally:
                async with self.claim_lock:
                    self.pause_event.set()
            return "\n\n".join(results_summary) or "No valid proxies parsed."
    # --- Periodic maintenance & source refresh loops ---
    async def periodic_scheduler_loop(self) -> None:
        last_digest_date: Optional[str] = None
        first_run = True
        while not self.stop_event.is_set():
            try:
                if first_run:
                    first_run = False
                else:
                    await asyncio.sleep(Config.SOURCE_REFRESH_SECONDS)
                await self.pause_event.wait()
                now = now_utc()
                sources = await self.db.get_sources(enabled_only=True)
                for src_doc in sources:
                    await self.pause_event.wait()
                    interval = safe_int(src_doc.get("fetch_interval"), Config.SOURCE_REFRESH_SECONDS)
                    last_checked = parse_dt(src_doc.get("last_checked_at"))
                    if last_checked and (now - last_checked).total_seconds() < interval:
                        continue
                    try:
                        res = await self.sources.import_source(src_doc)
                        if not res.get("unchanged"):
                            logger.info(
                                "[SOURCE] Ingested %s (New: %s, Total: %s)",
                                src_doc.get("name", src_doc.get("source_id")),
                                res.get("added", 0),
                                res.get("total", 0),
                            )
                    except Exception as exc:
                        logger.error("[SOURCE] Ingestion error on %s: %s", src_doc.get("name"), short_error(exc))
                    await self.db.update_source_health_score(src_doc.get("source_id"))
                await self.db.release_expired_leases()
                await self.db.retire_orphans()
                now = now_utc()
                digest_key = now.strftime("%Y-%m-%d")
                scheduled_minutes = Config.DAILY_REPORT_HOUR * 60 + Config.DAILY_REPORT_MINUTE
                current_minutes = now.hour * 60 + now.minute
                if (
                    Config.REPORT_ENABLED
                    and current_minutes >= scheduled_minutes
                    and last_digest_date != digest_key
                ):
                    for platform in ALL_PLATFORMS:
                        await self.notify_func(platform, await self.build_platform_digest(platform))
                    last_digest_date = digest_key
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[SCHEDULER] Error in periodic scheduler loop")
                await asyncio.sleep(5)
    async def build_platform_digest(self, platform: str) -> str:
        stats = await self.db.get_platform_stats(platform)
        return "\n".join(
            [
                f"📊 DAILY {platform.upper()} PROXY DIGEST",
                f"Date: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}",
                "",
                f"🟢 Working: {stats['working']}",
                f"🟠 Quarantined: {stats['quarantined']}",
                f"🔴 Disabled: {stats['disabled']}",
                f"🌐 Total: {stats['total']}",
                f"⭐ Ever Working: {stats['ever_working']}",
            ]
        )
    async def discovery_scheduler_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(Config.DISCOVERY_INTERVAL_SECONDS)
                await self.pause_event.wait()
                added = await self.sources.run_discovery_pass()
                if added > 0:
                    logger.info("[DISCOVERY] Auto-discovered %s new proxy sources from GitHub trees.", added)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[DISCOVERY] Error in auto-discovery loop")
# ============================================================================
# REPORTING ENGINE (Feature 10.1, 10.2, 10.7)
# ============================================================================
class ReportEngine:
    def __init__(self, db: Database) -> None:
        self.db = db
    async def export_working(self, platform: str) -> bytes:
        col = self.db.get_col(platform)
        cursor = col.find(
            {f"platform_status.{platform}.state": PlatformState.WORKING, "enabled": True}
        ).sort("latency_ms", ASCENDING)
        docs = await cursor.to_list(length=10000)
        if not docs:
            return b"# No active working proxies for this platform\n"
        by_country: Dict[str, List[str]] = defaultdict(list)
        for doc in docs:
            country = str(doc.get("verified_country") or doc.get("source_country") or "UNKNOWN").upper()
            proxy_url = str(doc.get("proxy_url") or "")
            if proxy_url:
                by_country[country].append(proxy_url)
        interleaved: List[str] = []
        max_len = max((len(values) for values in by_country.values()), default=0)
        for index in range(max_len):
            for values in by_country.values():
                if index < len(values):
                    interleaved.append(values[index])
        return "\n".join(interleaved).encode("utf-8")
    async def generate_daily_digest(self) -> str:
        lines = ["📊 DAILY PROXY WORKER DIGEST", f"Date: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}", ""]
        for platform in ALL_PLATFORMS:
            stats = await self.db.get_platform_stats(platform)
            lines.extend(
                [
                    f"• **{platform.title()}**:",
                    f"   🟢 Working: {stats['working']}",
                    f"   🟠 Quarantined: {stats['quarantined']}",
                    f"   🔴 Disabled: {stats['disabled']}",
                    f"   🌐 Total: {stats['total']}",
                ]
            )
        return "\n".join(lines)
    async def source_health(self) -> List[Dict[str, Any]]:
        sources = await self.db.get_sources()
        return sorted(sources, key=lambda item: safe_float(item.get("health_score"), 0.0), reverse=True)
# ============================================================================
# TELEGRAM ADMIN UI (Feature 3, 4, 5, 9)
# ============================================================================
class TelegramAdminUI:
    def __init__(self, db: Database, scheduler: WorkerScheduler, reports: ReportEngine) -> None:
        self.db = db
        self.scheduler = scheduler
        self.reports = reports
        self.bot: Optional[Client] = None
        self._awaiting_source = False
        self.log_channels = {
            "youtube": Config.YOUTUBE_LOG_CHANNEL_ID,
            "instagram": Config.INSTAGRAM_LOG_CHANNEL_ID,
            "tiktok": Config.TIKTOK_LOG_CHANNEL_ID,
        }
    async def notify_platform(self, platform: str, text: str) -> None:
        """Sends verified/recovery alert to the designated platform log channel."""
        if not self.bot:
            return
        target_channel = self.log_channels.get(platform, Config.ADMIN_CHAT_ID)
        try:
            await self.bot.send_message(target_channel, text)
        except FloodWait as exc:
            await asyncio.sleep(getattr(exc, "value", 5))
            try:
                await self.bot.send_message(target_channel, text)
            except Exception:
                logger.exception("[TG] Retry send failed for %s", platform)
        except Exception:
            logger.exception("[TG] Failed to dispatch alert to %s log channel", platform)
    def is_authorized(self, user_id: int) -> bool:
        return user_id == Config.OWNER_ID
    # --- Dashboards & Keyboards (Requirement #9) ---
    @staticmethod
    def main_dashboard_markup() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📺 YouTube", callback_data="panel_youtube"),
                    InlineKeyboardButton("📸 Instagram", callback_data="panel_instagram"),
                    InlineKeyboardButton("🎵 TikTok", callback_data="panel_tiktok"),
                ],
                [
                    InlineKeyboardButton("📊 Daily Digest", callback_data="btn_digest"),
                    InlineKeyboardButton("📁 Sources", callback_data="btn_sources"),
                ],
                [
                    InlineKeyboardButton("➕ Add Source", callback_data="btn_add_source"),
                    InlineKeyboardButton("⚡ Manual Priority Check", callback_data="btn_manual_prompt"),
                ],
                [InlineKeyboardButton("🩺 Source Health", callback_data="btn_source_health")],
            ]
        )
    @staticmethod
    def platform_subpanel_markup(platform: str, enabled: bool) -> InlineKeyboardMarkup:
        toggle_text = "⏸ Disable Platform" if enabled else "▶️ Enable Platform"
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📥 Export Working (Fastest First)", callback_data=f"exp_{platform}")],
                [
                    InlineKeyboardButton(toggle_text, callback_data=f"toggle_{platform}"),
                    InlineKeyboardButton("♻️ Refresh Pool", callback_data=f"ref_{platform}"),
                ],
                [InlineKeyboardButton("🔙 Back to Main Dashboard", callback_data="panel_main")],
            ]
        )
    async def setup(self) -> None:
        if Client is None:
            logger.warning("[TG] pyrogram not available; Telegram Admin UI disabled.")
            return
        self.bot = Client(
            "proxy_worker_v3",
            bot_token=Config.BOT_TOKEN,
            api_id=env_int("API_ID", 0, 1),
            api_hash=os.getenv("API_HASH", "").strip(),
            in_memory=True,
        )
        @self.bot.on_message(filters.command("start") & filters.private)
        async def _cmd_start(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            await message.reply_text(
                "🤖 **Proxy Worker Bot v3 (Multi-Platform)**\nSelect a platform panel below:",
                reply_markup=self.main_dashboard_markup(),
            )
        @self.bot.on_message(filters.command("addsource") & filters.private)
        async def _cmd_addsource(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                await message.reply_text(
                    "Usage: `/addsource <url>` or `/addsource <name> | <url>`"
                )
                return
            spec = parts[1].strip()
            if "|" in spec:
                name, url = [item.strip() for item in spec.split("|", 1)]
            else:
                name, url = "Manual Source", spec
            if not url.startswith(("http://", "https://")):
                await message.reply_text("❌ Source URL must start with http:// or https://")
                return
            source_id = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
            await self.db.upsert_source(
                {
                    "source_id": source_id,
                    "name": name or "Manual Source",
                    "url": url,
                    "enabled": True,
                    "discovered": False,
                    "priority": 100,
                    "fetch_interval": Config.SOURCE_REFRESH_SECONDS,
                },
                only_if_missing=True,
            )
            await message.reply_text(f"✅ Source added: `{name or 'Manual Source'}`")
        @self.bot.on_message(filters.text & filters.private)
        async def _on_text(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            if getattr(self, "_awaiting_source", False):
                self._awaiting_source = False
                spec = str(message.text or "").strip()
                if "|" in spec:
                    name, url = [item.strip() for item in spec.split("|", 1)]
                else:
                    name, url = "Manual Source", spec
                if not url.startswith(("http://", "https://")):
                    await message.reply_text("❌ Source URL must start with http:// or https://")
                    return
                source_id = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
                await self.db.upsert_source(
                    {
                        "source_id": source_id,
                        "name": name or "Manual Source",
                        "url": url,
                        "enabled": True,
                        "discovered": False,
                        "priority": 100,
                        "fetch_interval": Config.SOURCE_REFRESH_SECONDS,
                    },
                    only_if_missing=True,
                )
                await message.reply_text(f"✅ Source added: `{name or 'Manual Source'}`")
        @self.bot.on_message(filters.command("addproxy") & filters.private)
        async def _cmd_addproxy(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                await message.reply_text("Usage: `/addproxy <proxy_url>` or paste multiple lines.")
                return
            proxies = [p.strip() for p in parts[1].split() if p.strip()]
            wait_msg = await message.reply_text("⚡ Pausing queue and executing manual priority check across platforms...")
            res = await self.scheduler.manual_priority_check(proxies)
            await wait_msg.edit_text(res)
        @self.bot.on_message(filters.command("digest") & filters.private)
        async def _cmd_digest(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            digest = await self.reports.generate_daily_digest()
            await message.reply_text(digest)
        @self.bot.on_message(filters.document & filters.private)
        async def _on_document_upload(_, message: Message):
            """Requirement #5: File ingestion (.txt, .json, .csv) up to 10 files."""
            if not self.is_authorized(message.from_user.id):
                return
            fname = message.document.file_name.lower()
            if not any(fname.endswith(f".{ext}") for ext in ("txt", "json", "csv")):
                await message.reply_text("❌ Only .txt, .json, or .csv files are supported.")
                return
            wait_msg = await message.reply_text("📥 Downloading & ingesting proxy list...")
            fpath = await message.download()
            try:
                with open(fpath, "r", encoding="utf-8", errors="replace") as fh:
                    raw_text = fh.read()
                candidates = parse_source_payload(raw_text, "", fname)
                proxies = [c.raw for c in candidates]
                res = await self.scheduler.manual_priority_check(proxies)
                await wait_msg.edit_text(f"📁 Ingestion Summary for `{message.document.file_name}`:\n\n{res[:3800]}")
            finally:
                if os.path.exists(fpath):
                    os.remove(fpath)
        @self.bot.on_callback_query()
        async def _on_callback(_, query: CallbackQuery):
            if not self.is_authorized(query.from_user.id):
                await query.answer("Unauthorized", show_alert=True)
                return
            data = query.data
            if data == "panel_main":
                await query.message.edit_text(
                    "🤖 **Proxy Worker Bot v3 (Multi-Platform)**\nSelect a platform panel below:",
                    reply_markup=self.main_dashboard_markup(),
                )
            elif data.startswith("panel_"):
                p = data.split("_")[1]
                stats = await self.db.get_platform_stats(p)
                enabled = await self.db.get_config(f"{p}_validation_enabled", True)
                status_str = "🟢 Active" if enabled else "⏸ Paused"
                text = (
                    f"**[{p.title()} Panel]** — Status: {status_str}\n\n"
                    f"🟢 Working: `{stats['working']}`\n"
                    f"🟠 Quarantined: `{stats['quarantined']}`\n"
                    f"🔴 Disabled: `{stats['disabled']}`\n"
                    f"🌐 Total Registered: `{stats['total']}`\n"
                    f"⭐ Ever Validated Working: `{stats['ever_working']}`"
                )
                await query.message.edit_text(text, reply_markup=self.platform_subpanel_markup(p, enabled))
            elif data.startswith("exp_"):
                p = data.split("_")[1]
                await query.answer("Generating export...")
                export_bytes = await self.reports.export_working(p)
                t_path = f"/tmp/{p}_working_{int(time.time())}.txt"
                with open(t_path, "wb") as fh:
                    fh.write(export_bytes)
                await query.message.reply_document(
                    t_path,
                    caption=f"Verified {p.title()} Working Proxies (Latency-Sorted & Geo-Distributed)",
                )
                if os.path.exists(t_path):
                    os.remove(t_path)
            elif data.startswith("toggle_"):
                p = data.split("_")[1]
                cur = await self.db.get_config(f"{p}_validation_enabled", True)
                await self.db.set_config(f"{p}_validation_enabled", not cur)
                await query.answer(f"{p.title()} validation toggled.")
                # Refresh panel view
                stats = await self.db.get_platform_stats(p)
                enabled = not cur
                status_str = "🟢 Active" if enabled else "⏸ Paused"
                text = (
                    f"**[{p.title()} Panel]** — Status: {status_str}\n\n"
                    f"🟢 Working: `{stats['working']}`\n"
                    f"🟠 Quarantined: `{stats['quarantined']}`\n"
                    f"🔴 Disabled: `{stats['disabled']}`\n"
                    f"🌐 Total Registered: `{stats['total']}`"
                )
                await query.message.edit_text(text, reply_markup=self.platform_subpanel_markup(p, enabled))
            elif data.startswith("ref_"):
                p = data.split("_")[1]
                await query.answer("Queueing pool revalidation...")
                # Sets next_check_at to now for all working proxies of this platform
                col = self.db.get_col(p)
                await col.update_many(
                    {f"platform_status.{p}.state": PlatformState.WORKING},
                    {"$set": {f"platform_status.{p}.next_check_at": now_utc()}},
                )
                await query.message.reply_text(f"♻️ Immediate revalidation scheduled for all active {p.title()} proxies.")
            elif data == "btn_add_source":
                self._awaiting_source = True
                await query.message.reply_text(
                    "Send the source as `<name> | <url>` or just `<url>`. "
                    "You can also use `/addsource <url>`."
                )
                await query.answer("Waiting for source details...")
                return
            elif data == "btn_digest":
                digest = await self.reports.generate_daily_digest()
                await query.message.edit_text(digest, reply_markup=self.main_dashboard_markup())
            elif data == "btn_sources":
                sources = await self.db.get_sources()
                lines = [f"📁 Configured Sources ({len(sources)}):"]
                for s in sources[:20]:
                    status = "🟢" if s.get("enabled") else "⏸"
                    disc = " [Auto]" if s.get("discovered") else ""
                    score = safe_float(s.get("health_score"), 0.0)
                    lines.append(
                        f"{status} {s.get('name')}{disc} "
                        f"(Yield: {s.get('yield_working_count', 0)}, Health: {score:.1f})"
                    )
                await query.message.edit_text("\n".join(lines), reply_markup=self.main_dashboard_markup())
            elif data == "btn_manual_prompt":
                await query.message.reply_text(
                    "Send `/addproxy <url>` or paste a block of proxies directly into chat."
                )
            elif data == "btn_source_health":
                sources = await self.reports.source_health()
                lines = ["🩺 SOURCE HEALTH"]
                for s in sources[:20]:
                    lines.append(
                        f"{'🟢' if s.get('enabled') else '⏸'} "
                        f"{s.get('name', s.get('source_id'))}: "
                        f"{safe_float(s.get('health_score'), 0.0):.1f}"
                    )
                await query.message.edit_text("\n".join(lines), reply_markup=self.main_dashboard_markup())
            await query.answer()
    async def start(self) -> None:
        if self.bot:
            await self.bot.start()
            logger.info("[TG] Telegram Admin UI started successfully.")
    async def stop(self) -> None:
        if self.bot:
            try:
                await self.bot.stop()
            except Exception:
                pass
# ============================================================================
# HEALTH & READINESS SERVER
# ============================================================================
class HealthServer:
    def __init__(self, db: Database, scheduler: WorkerScheduler) -> None:
        self.db = db
        self.scheduler = scheduler
        self.app = web.Application()
        self.app.add_routes(
            [
                web.get("/", self.handle_root),
                web.get("/health", self.handle_health),
                web.get("/ready", self.handle_ready),
            ]
        )
        self.runner: Optional[web.AppRunner] = None
    async def handle_root(self, _) -> web.Response:
        return web.json_response({"service": "proxy-worker-bot-v3", "status": "running"})
    async def handle_health(self, _) -> web.Response:
        db_ok = await self.db.ping()
        status = 200 if db_ok else 503
        stats = {}
        for p in ALL_PLATFORMS:
            stats[p] = await self.db.get_platform_stats(p)
        return web.json_response(
            {
                "status": "ok" if db_ok else "degraded",
                "mongo": db_ok,
                "active_tests": self.scheduler.active_tests,
                "platform_stats": stats,
            },
            status=status,
        )
    async def handle_ready(self, _) -> web.Response:
        ok = await self.db.ping() and self.scheduler.running
        return web.json_response({"ready": ok}, status=200 if ok else 503)
    async def start(self) -> None:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", Config.PORT)
        await site.start()
        logger.info("[HEALTH] HTTP health server running on port %s", Config.PORT)
    async def stop(self) -> None:
        if self.runner:
            await self.runner.cleanup()
# ============================================================================
# APPLICATION LIFECYCLE
# ============================================================================
class Application:
    def __init__(self) -> None:
        self.db = Database()
        self.sources = ProxySourceManager(self.db)
        self.engine = ValidationEngine()
        self.reports = ReportEngine(self.db)
        self.admin_ui = TelegramAdminUI(self.db, None, self.reports)
        self.scheduler = WorkerScheduler(self.db, self.sources, self.engine, self.admin_ui.notify_platform)
        self.admin_ui.scheduler = self.scheduler
        self.health_server = HealthServer(self.db, self.scheduler)
    async def start(self) -> None:
        Config.validate()
        await self.db.connect()
        await self.sources.start()
        await self.admin_ui.setup()
        await self.admin_ui.start()
        await self.scheduler.start()
        await self.health_server.start()
        # Startup Announcement
        start_msg = (
            "🚀 **Proxy Worker Bot v3 Online**\n"
            "• Platforms: YouTube, Instagram, TikTok\n"
            "• Collections: 3 Isolated collections\n"
            "• State Machine: Staged Revalidation Active"
        )
        await self.admin_ui.notify_platform("youtube", start_msg)
        logger.info("[APP] Initialization fully complete.")
    async def stop(self) -> None:
        logger.info("[APP] Shutting down...")
        await self.health_server.stop()
        await self.scheduler.stop()
        await self.admin_ui.stop()
        await self.sources.close()
        await self.db.close()
        logger.info("[APP] Terminated cleanly.")
    async def run(self) -> None:
        await self.start()
        stop_signal = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_signal.set)
            except NotImplementedError:
                pass
        await stop_signal.wait()
        await self.stop()
# ============================================================================
# MAIN ENTRYPOINT
# ============================================================================
async def main() -> None:
    app = Application()
    try:
        await app.run()
    except (KeyboardInterrupt, SystemExit):
        await app.stop()
    except Exception:
        logger.critical("Fatal application error", exc_info=True)
        await app.stop()
        sys.exit(1)
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
