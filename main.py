#!/usr/bin/env python3
"""
Dedicated Proxy Worker Bot - single-file implementation (v2).

This application is a proxy-intelligence/service worker. It does NOT download
YouTube media for end users and does NOT post content anywhere. Its output is
verified proxy connectivity intelligence persisted in MongoDB for consumption
by a separate Main Bot.

Design foundation: Code A (ChatGPT version) architecture and safety
characteristics (coarse-grained scheduler, bounded queue, bounded test
concurrency, byte-capped source fetches, content-hash change detection,
lease-based duplicate-work prevention, YouTube-target health guard).

Selectively integrated from Code B:
    - Source-removal awareness (adapted: only affects proxies that were
      NEVER validated as working; a proxy that has ever been WORKING is
      permanent and is never removed just because a source stopped listing
      it).
    - Explicit state-constant classes for readability.

New in this revision (per operator requirements):
    - Generic GitHub repo/tree source resolution (not provider-specific),
      with a cached resolved-file lookup to avoid re-listing the directory
      on every refresh.
    - Multi-format source parsing (TXT / JSON / CSV) kept separate from
      proxy validation logic.
    - Country metadata is reused from source data when available, avoiding
      an extra geolocation request per proxy.
    - A cheap TCP "pre-check" runs before any HTTP request is made, so a
      dead proxy costs zero external bandwidth.
    - Working proxies are permanent records: source removal, worker
      restarts, and worker crashes never delete them. Only an explicit
      failure policy (consecutive-failure threshold) can move a permanent
      record to PERMANENTLY_FAILED, and the record itself is retained.
    - Optional, disabled-by-default Instagram connectivity validator,
      fully separate from the YouTube validator.
    - Notifications are sent only on meaningful state changes (new working,
      recovered, permanently failed) to avoid spamming the same result.

Core flow:
    configured source(s) -> resolve -> fetch (hash + size capped)
        -> parse (format-specific) -> normalize -> deduplicate
        -> persist (NORMALIZED) -> bounded queue -> lease/claim
        -> cheap TCP check -> country (source metadata reused if present)
        -> YouTube validation (skip-download) -> optional Instagram check
        -> failure classification -> backoff / quarantine / permanent state
        -> permanent MongoDB working pool -> Telegram notification -> Main Bot

Python:
    3.12+

Required environment:
    BOT_TOKEN
    OWNER_ID
    MONGO_URI

Recommended packages:
    aiohttp
    aiohttp-socks
    pyrogram
    tgcrypto
    pymongo
    yt-dlp

Run:
    python main.py
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
import re
import shutil
import signal
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, unquote, urlparse

import aiohttp
from aiohttp import web
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.errors import OperationFailure

try:
    from pyrogram import Client, filters
    from pyrogram.errors import FloodWait
    from pyrogram.types import (
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        Message,
        CallbackQuery,
    )
except Exception:  # pragma: no cover - optional at import time for tooling
    Client = None
    filters = None
    FloodWait = Exception
    InlineKeyboardButton = None
    InlineKeyboardMarkup = None
    Message = Any
    CallbackQuery = Any

try:
    from aiohttp_socks import ProxyConnector
except Exception:  # pragma: no cover
    ProxyConnector = None


# ============================================================================
# CONFIGURATION
# ============================================================================

def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int = 0, maximum: Optional[int] = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    value = max(minimum, value)
    if maximum is not None:
        value = min(value, maximum)
    return value


def env_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return tuple(x.strip() for x in raw.split(",") if x.strip())


class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
    OWNER_ID = env_int("OWNER_ID", 0, 1)
    MONGO_URI = os.getenv("MONGO_URI", "").strip()
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "telegram_downloader").strip()

    # IMPORTANT: default stays "proxies" for existing Main Bot compatibility.
    PROXY_COLLECTION = os.getenv("MONGO_COLLECTION", "proxies").strip()

    PORT = env_int("PORT", 8080, 1, 65535)

    # --- Scheduler cadence (Code A's coarse-grained model, kept on purpose) ---
    SOURCE_REFRESH_SECONDS = env_int("SOURCE_REFRESH_SECONDS", 300, 30)
    TEST_CONCURRENCY = env_int("PROXY_TEST_CONCURRENCY", 5, 1, 50)
    MAX_PENDING_TESTS = env_int("MAX_PENDING_TESTS", 1000, 1, 10000)
    MAX_TEST_PER_REFRESH = env_int("MAX_PROXIES_PER_REFRESH", 300, 1, 5000)

    # --- Timeouts ---
    CONNECT_CHECK_TIMEOUT = env_int("CONNECT_CHECK_TIMEOUT", 6, 1, 30)
    GENERIC_TIMEOUT = env_int("HTTP_CONNECT_TIMEOUT", 12, 3, 120)
    GEO_TIMEOUT = env_int("GEO_TIMEOUT", 10, 3, 60)
    YOUTUBE_TIMEOUT = env_int("YOUTUBE_TEST_TIMEOUT", 45, 10, 180)
    INSTAGRAM_TIMEOUT = env_int("INSTAGRAM_TEST_TIMEOUT", 15, 5, 60)

    # --- Revalidation / failure policy ---
    # Operator requirement: previously-approved proxies are revalidated
    # roughly every 2-3 days by default, not hard-coded to one value.
    REVALIDATION_INTERVAL = env_int("REVALIDATION_INTERVAL", 3 * 86400, 300, 14 * 86400)
    QUARANTINE_BASE_SECONDS = env_int("PROXY_QUARANTINE_SECONDS", 21600, 300, 7 * 86400)
    MAX_QUARANTINE_SECONDS = env_int("MAX_QUARANTINE_SECONDS", 7 * 86400, 3600, 30 * 86400)
    MAX_CONSECUTIVE_FAILURES = env_int("MAX_CONSECUTIVE_FAILURES", 5, 1, 50)

    # Only affects proxies that were NEVER validated as working. Working
    # proxies are permanent and are never auto-retired by this setting.
    ORPHAN_RETIRE_AFTER_SECONDS = env_int("ORPHAN_RETIRE_AFTER_SECONDS", 7 * 86400, 3600, 60 * 86400)

    DAILY_REPORT_HOUR = env_int("DAILY_REPORT_HOUR", 9, 0, 23)
    DAILY_REPORT_MINUTE = env_int("DAILY_REPORT_MINUTE", 0, 0, 59)

    YOUTUBE_TEST_URL = os.getenv(
        "YOUTUBE_TEST_URL",
        "https://www.youtube.com/watch?v=jNQXAC9IVRw",
    ).strip()

    YTDLP_BINARY = os.getenv("YTDLP_BINARY", "yt-dlp").strip()
    TEST_ENGINE_VERSION = os.getenv("TEST_ENGINE_VERSION", "2").strip()
    YTDLP_REMOTE_COMPONENTS = os.getenv("YTDLP_REMOTE_COMPONENTS", "").strip()

    ENABLE_GEO_LOOKUP = env_bool("ENABLE_GEO_LOOKUP", True)
    GEO_LOOKUP_URL = os.getenv("GEO_LOOKUP_URL", "https://ipwho.is/{ip}").strip()

    # --- Optional secondary validator (disabled by default) ---
    INSTAGRAM_VALIDATION_ENABLED = env_bool("INSTAGRAM_VALIDATION_ENABLED", False)
    INSTAGRAM_TEST_URL = os.getenv("INSTAGRAM_TEST_URL", "https://www.instagram.com/").strip()

    REPORT_ENABLED = env_bool("REPORT_ENABLED", True)
    DEBUG = env_bool("DEBUG", False)
    DRY_RUN = env_bool("DRY_RUN", False)

    MAX_SOURCE_BYTES = env_int("MAX_SOURCE_BYTES", 50 * 1024 * 1024, 1024, 200 * 1024 * 1024)
    MAX_DISCOVERED_PER_SOURCE = env_int("MAX_DISCOVERED_PER_SOURCE", 10000, 1, 100000)

    SOURCE_FAILURE_ALERT_THRESHOLD = env_int("SOURCE_FAILURE_ALERT_THRESHOLD", 3, 1, 20)

    MAX_RETRIES = env_int("NETWORK_RETRIES", 1, 0, 3)
    MAX_YTDLP_FORMATS = env_int("YOUTUBE_MAX_FORMAT_CHECKS", 50, 1, 1000)
    ADMIN_CHAT_ID = env_int("REPORT_CHAT_ID", OWNER_ID, 1)

    # How long a resolved GitHub directory listing (tree -> actual raw file)
    # stays cached before being re-resolved. Directory contents rarely
    # change, so this avoids an extra GitHub API call on every refresh.
    SOURCE_RESOLVE_CACHE_SECONDS = env_int("SOURCE_RESOLVE_CACHE_SECONDS", 6 * 3600, 300, 7 * 86400)

    # Preference order when a source resolves to a directory containing
    # multiple formats (e.g. data.txt / data.json / data.csv). JSON is
    # preferred first because it commonly carries country/protocol metadata
    # that would otherwise require an extra geolocation request per proxy.
    PREFERRED_SOURCE_FORMATS = env_list("PREFERRED_SOURCE_FORMATS", ("json", "txt", "csv"))

    USER_AGENT = os.getenv(
        "HTTP_USER_AGENT",
        "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0 Mobile Safari/537.36",
    ).strip()

    # No sources are enabled automatically. The operator adds and controls
    # the configured source list explicitly (Telegram admin UI or this
    # list). This intentionally replaces the old behaviour of auto-seeding
    # third-party proxy-list mirrors on first run.
    DEFAULT_SOURCES: tuple[dict[str, Any], ...] = ()

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

        parsed = urlparse(cls.YOUTUBE_TEST_URL)
        if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
            "youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"
        }:
            raise RuntimeError("YOUTUBE_TEST_URL must be a valid YouTube URL.")

        if not cls.YTDLP_BINARY:
            raise RuntimeError("YTDLP_BINARY cannot be empty.")


# ============================================================================
# LOGGING / HELPERS
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
logger = logging.getLogger("proxy-worker")
logger.addFilter(SecretFilter())

UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def parse_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return None


def short_error(value: Any, limit: int = 700) -> str:
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
# STATE CONSTANTS
# ============================================================================

class ProxyState:
    """Validation lifecycle state. Distinct from `enabled`/`retired` flags
    and from `ever_working`, which is a permanent, never-unset marker."""
    NORMALIZED = "NORMALIZED"          # parsed + deduplicated, not yet queued
    QUEUED = "QUEUED"                  # waiting in the bounded test queue
    TESTING = "TESTING"                # currently leased/under test
    YOUTUBE_WORKING = "YOUTUBE_WORKING"
    CONNECTION_FAILED = "CONNECTION_FAILED"
    TIMEOUT = "TIMEOUT"
    YOUTUBE_REJECTED = "YOUTUBE_REJECTED"
    ENVIRONMENT_ERROR = "ENVIRONMENT_ERROR"
    QUARANTINED = "QUARANTINED"
    PERMANENTLY_FAILED = "PERMANENTLY_FAILED"   # record retained, just unusable
    RETIRED = "RETIRED"                # orphan cleanup only; never applied to ever_working proxies


class FailureCategory:
    """Failure classification used to decide retry/quarantine/permanent policy."""
    CONNECTION_TIMEOUT = "CONNECTION_TIMEOUT"
    CONNECTION_REFUSED = "CONNECTION_REFUSED"
    DNS_FAILURE = "DNS_FAILURE"
    PROXY_PROTOCOL_FAILURE = "PROXY_PROTOCOL_FAILURE"
    PROXY_AUTH_FAILURE = "PROXY_AUTH_FAILURE"
    TARGET_UNAVAILABLE = "TARGET_UNAVAILABLE"
    TEMPORARY_SERVER_FAILURE = "TEMPORARY_SERVER_FAILURE"
    EXTRACTION_FAILURE = "EXTRACTION_FAILURE"
    INVALID_PROXY = "INVALID_PROXY"
    ENVIRONMENT_ERROR = "ENVIRONMENT_ERROR"
    RATE_LIMITED = "RATE_LIMITED"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"

    # Categories that should NOT count against a proxy's reputation because
    # they reflect a problem with the *test environment* or the *target*,
    # not with the proxy route itself.
    NON_ROUTE_SPECIFIC = frozenset({ENVIRONMENT_ERROR, TARGET_UNAVAILABLE, RATE_LIMITED})


# ============================================================================
# PROXY MODEL / PARSING
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
        # Canonical identity is scheme+host+port+credentials. This means
        # "ip:port" (implicit http) and "http://ip:port" collapse to the
        # same identity, but a distinct explicit scheme (e.g. socks5://
        # same ip:port) is treated as a different route, since it behaves
        # differently on the wire.
        return hashlib.sha256(self.canonical.encode("utf-8")).hexdigest()


SUPPORTED_SCHEMES = {"http", "https", "socks4", "socks5"}
PROXY_RE = re.compile(
    r"^(?:(?P<scheme>https?|socks4|socks5)://)?"
    r"(?:(?P<user>[^:@/\s]+):(?P<password>[^@/\s]*)@)?"
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

    return ProxyEntry(
        scheme=scheme,
        host=host,
        port=port,
        username=match.group("user"),
        password=match.group("password"),
    )


# --- format-specific parsers: kept separate from validation logic ---------

def extract_proxy_candidates(text: str) -> list[str]:
    lines = re.split(r"[\r\n]+", text)
    out = []
    for line in lines:
        line = line.strip().strip(",;")
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


@dataclass
class ParsedCandidate:
    raw: str
    scheme_hint: Optional[str] = None
    country: Optional[str] = None
    anonymity: Optional[str] = None


def parse_txt_payload(text: str) -> list[ParsedCandidate]:
    return [ParsedCandidate(raw=line) for line in extract_proxy_candidates(text)]


def parse_csv_payload(text: str) -> list[ParsedCandidate]:
    out: list[ParsedCandidate] = []
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

    def col(row: list[str], name: str) -> Optional[str]:
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


def parse_json_payload(text: str) -> list[ParsedCandidate]:
    out: list[ParsedCandidate] = []
    try:
        data = json.loads(text)
    except Exception:
        return out

    items: list[Any]
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Common shapes: {"proxies": [...]}, {"data": [...]}
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

        # Country may be a flat field or nested under a geolocation object
        # (e.g. Proxifly's {"geolocation": {"country": "US", ...}}).
        country = item.get("country") or item.get("country_code") or item.get("geo")
        geoloc = item.get("geolocation")
        if not country and isinstance(geoloc, dict):
            country = geoloc.get("country") or geoloc.get("country_code")

        # Prefer the source's own fully-formed proxy string when present -
        # it already encodes the correct scheme and avoids reconstruction.
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


def parse_source_payload(text: str, content_type: str, url: str = "") -> list[ParsedCandidate]:
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
# DATABASE
# ============================================================================

class Database:
    def __init__(self) -> None:
        self.client: Optional[AsyncMongoClient] = None
        self.db = None
        self.proxies = None
        self.sources = None
        self.tasks = None
        self.snapshots = None
        self.events = None
        self.feedback = None
        self.daily = None
        self.worker_config = None

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

        self.proxies = self.db[Config.PROXY_COLLECTION]
        self.sources = self.db["proxy_sources"]
        self.tasks = self.db["proxy_tasks"]
        self.snapshots = self.db["proxy_source_snapshots"]
        self.events = self.db["proxy_events"]
        self.feedback = self.db["proxy_feedback"]
        self.daily = self.db["proxy_daily_summary"]
        self.worker_config = self.db["worker_config"]

        await self.ensure_indexes()
        logger.info("[DB] MongoDB connected (idempotent init, no destructive operations)")

    async def ensure_indexes(self) -> None:
        # Catch index conflict errors from older bot versions and recreate them safely
        try:
            await self.proxies.create_index([("proxy_id", ASCENDING)], unique=True, sparse=True)
        except OperationFailure as e:
            if e.code == 86:  # IndexKeySpecsConflict
                logger.warning("[DB] Index conflict for proxy_id. Dropping old index and recreating...")
                await self.proxies.drop_index("proxy_id_1")
                await self.proxies.create_index([("proxy_id", ASCENDING)], unique=True, sparse=True)
            else:
                raise

        await self.proxies.create_index(
            [
                ("enabled", ASCENDING),
                ("state", ASCENDING),
                ("quarantine_until", ASCENDING),
                ("youtube_score", DESCENDING),
            ]
        )
        await self.proxies.create_index([("ever_working", ASCENDING)])
        await self.proxies.create_index([("verified_country", ASCENDING)])
        await self.proxies.create_index([("scheme", ASCENDING)])
        await self.proxies.create_index([("last_tested_at", ASCENDING)])
        await self.proxies.create_index([("next_validation_at", ASCENDING)])
        await self.proxies.create_index([("lease_until", ASCENDING)])
        await self.proxies.create_index([("source_ids", ASCENDING)])

        await self.sources.create_index([("source_id", ASCENDING)], unique=True)
        await self.tasks.create_index([("task_id", ASCENDING)], unique=True)
        await self.tasks.create_index([("status", ASCENDING), ("created_at", DESCENDING)])
        await self.snapshots.create_index([("source_id", ASCENDING), ("fetched_at", DESCENDING)])
        await self.events.create_index([("proxy_id", ASCENDING), ("created_at", DESCENDING)])
        await self.feedback.create_index([("proxy_id", ASCENDING), ("created_at", DESCENDING)])
        await self.daily.create_index([("date", ASCENDING)], unique=True)

        # TTL-style bounded history: events/snapshots older than 30 days are
        # pruned by the scheduler's cleanup pass (see WorkerScheduler.cleanup_history),
        # not deleted here. Indexes above just make that pass efficient.

    async def ping(self) -> bool:
        try:
            if self.client is None:
                return False
            await self.client.admin.command("ping")
            return True
        except Exception:
            return False

    async def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None
            logger.info("[DB] MongoDB closed")

    # --- worker_config (small key/value store) -----------------------------

    async def get_config(self, key: str, default: Any = None) -> Any:
        doc = await self.worker_config.find_one({"_id": key})
        return doc.get("value", default) if doc else default

    async def set_config(self, key: str, value: Any) -> None:
        await self.worker_config.update_one(
            {"_id": key},
            {"$set": {"value": value, "updated_at": now_utc()}},
            upsert=True,
        )

    async def initialize_defaults(self) -> None:
        # No third-party sources are auto-seeded. The operator is fully in
        # control of the source list (Config.DEFAULT_SOURCES is empty by
        # design; use /add_source or upsert_source to configure one).
        for src in Config.DEFAULT_SOURCES:
            await self.upsert_source({**src}, only_if_missing=True)

        defaults = {
            "youtube_test_url": Config.YOUTUBE_TEST_URL,
            "test_engine_version": Config.TEST_ENGINE_VERSION,
            "dry_run": Config.DRY_RUN,
        }
        for key, value in defaults.items():
            if await self.get_config(key) is None:
                await self.set_config(key, value)

    # --- sources -------------------------------------------------------------

    async def get_sources(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = {"enabled": True} if enabled_only else {}
        return await self.sources.find(query).sort("priority", DESCENDING).to_list(length=500)

    async def get_source(self, source_id: str) -> Optional[dict[str, Any]]:
        return await self.sources.find_one({"source_id": source_id})

    async def upsert_source(self, source: dict[str, Any], only_if_missing: bool = False) -> None:
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
            "stale": False,
            "resolved_url": None,
            "resolved_format": None,
            "resolved_at": None,
            "known_proxy_ids": [],
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
            {
                "$set": source_copy,
                "$setOnInsert": base_defaults,
            },
            upsert=True,
        )

    async def remove_source(self, source_id: str) -> bool:
        result = await self.sources.delete_one({"source_id": source_id})
        return result.deleted_count > 0

    async def record_source_state(
        self,
        source_id: str,
        *,
        content_hash: Optional[str] = None,
        item_count: Optional[int] = None,
        resolved_url: Optional[str] = None,
        resolved_format: Optional[str] = None,
        known_proxy_ids: Optional[list[str]] = None,
        success: bool = False,
        error: Optional[str] = None,
    ) -> None:
        update: dict[str, Any] = {"last_checked_at": now_utc(), "updated_at": now_utc()}
        if content_hash is not None:
            update["last_content_hash"] = content_hash
        if item_count is not None:
            update["last_item_count"] = item_count
        if resolved_url is not None:
            update["resolved_url"] = resolved_url
            update["resolved_at"] = now_utc()
        if resolved_format is not None:
            update["resolved_format"] = resolved_format
        if known_proxy_ids is not None:
            # Bounded: this is a set of proxy_id strings (~64 chars each) for
            # ONE source, overwritten in place every refresh - not appended
            # to history, so it cannot grow without bound like a per-fetch
            # snapshot array would.
            update["known_proxy_ids"] = known_proxy_ids

        if success:
            update["last_success_at"] = now_utc()
            update["last_failure_at"] = None
            update["failure_count"] = 0
            update["stale"] = False
            await self.sources.update_one({"source_id": source_id}, {"$set": update})
        else:
            update["last_failure_at"] = now_utc()
            if error:
                update["last_error"] = short_error(error)
            await self.sources.update_one(
                {"source_id": source_id},
                {"$set": update, "$inc": {"failure_count": 1}},
            )

    async def save_snapshot(self, doc: dict[str, Any]) -> None:
        # Snapshot documents intentionally do NOT store the full list of
        # proxy IDs (that lives on the source document, overwritten each
        # cycle - see record_source_state). Snapshots here are lightweight
        # counters only, so this collection stays small and bounded.
        await self.snapshots.insert_one(doc)

    # --- tasks / events (operational history, bounded by cleanup) ----------

    async def create_task(self, task_type: str, source_id: Optional[str] = None) -> str:
        task_id = hashlib.sha1(f"{task_type}:{source_id}:{time.time_ns()}".encode()).hexdigest()[:16]
        await self.tasks.insert_one(
            {
                "task_id": task_id,
                "task_type": task_type,
                "source_id": source_id,
                "status": "PENDING",
                "created_at": now_utc(),
                "started_at": None,
                "finished_at": None,
            }
        )
        return task_id

    async def start_task(self, task_id: str) -> None:
        await self.tasks.update_one({"task_id": task_id}, {"$set": {"status": "RUNNING", "started_at": now_utc()}})

    async def finish_task(self, task_id: str, status: str, **fields: Any) -> None:
        update = {"status": status, "finished_at": now_utc(), **fields}
        await self.tasks.update_one({"task_id": task_id}, {"$set": update})

    async def update_task(self, task_id: str, **fields: Any) -> None:
        await self.tasks.update_one({"task_id": task_id}, {"$set": fields})

    async def create_event(self, proxy_id: str, event_type: str, **data: Any) -> None:
        await self.events.insert_one(
            {"proxy_id": proxy_id, "event_type": event_type, "created_at": now_utc(), **data}
        )

    # --- proxies ---------------------------------------------------------------

    async def get_proxy(self, proxy_id: str) -> Optional[dict[str, Any]]:
        return await self.proxies.find_one({"proxy_id": proxy_id})

    async def upsert_proxy(self, entry: ProxyEntry, country: Optional[str] = None) -> tuple[bool, dict[str, Any]]:
        """Insert-or-update a NORMALIZED proxy record. Never overwrites an
        existing record's validation/working history - only source-linkage
        metadata is refreshed on an existing document."""
        now = now_utc()
        existing = await self.proxies.find_one({"proxy_id": entry.proxy_id})

        if existing:
            update: dict[str, Any] = {
                "last_seen_at": now,
                "source_present": True,
                "enabled": existing.get("enabled", True),
            }
            if entry.source_id and entry.source_id not in (existing.get("source_ids") or []):
                update["source_ids"] = list(set((existing.get("source_ids") or []) + [entry.source_id]))
            if country and not existing.get("source_country"):
                update["source_country"] = country
            await self.proxies.update_one({"proxy_id": entry.proxy_id}, {"$set": update})
            return False, {**existing, **update}

        doc = {
            "proxy_id": entry.proxy_id,
            "scheme": entry.scheme,
            "host": entry.host,
            "port": entry.port,
            "username": entry.username,
            "password": entry.password,
            "source_ids": [entry.source_id] if entry.source_id else [],
            "source_country": country or entry.source_country,
            "source_present": True,
            "state": ProxyState.NORMALIZED,
            "enabled": True,
            "retired": False,
            "ever_working": False,
            "consecutive_failures": 0,
            "youtube_score": 0.0,
            "verified_country": None,
            "first_seen_at": now,
            "last_seen_at": now,
            "last_tested_at": None,
            "next_validation_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "last_error_category": None,
            "last_error": None,
            "quarantine_until": None,
            "lease_until": None,
            "last_notified_state": None,
            "instagram_working": None,
            "instagram_last_tested_at": None,
        }
        await self.proxies.insert_one(doc)
        return True, doc

    async def claim_proxy(self, proxy_id: str, lease_seconds: int = 180) -> Optional[dict[str, Any]]:
        now = now_utc()
        result = await self.proxies.find_one_and_update(
            {
                "proxy_id": proxy_id,
                "$or": [{"lease_until": None}, {"lease_until": {"$lte": now}}],
            },
            {
                "$set": {
                    "lease_until": now + timedelta(seconds=lease_seconds),
                    "state": ProxyState.TESTING,
                }
            },
            return_document=True,
        )
        return result

    async def release_lease(self, proxy_id: str) -> None:
        await self.proxies.update_one({"proxy_id": proxy_id}, {"$set": {"lease_until": None}})

    async def release_expired_leases(self) -> int:
        now = now_utc()
        result = await self.proxies.update_many(
            {"lease_until": {"$ne": None, "$lte": now}},
            {"$set": {"lease_until": None}},
        )
        return result.modified_count

    async def record_test_result(self, proxy_id: str, result: dict[str, Any]) -> dict[str, Any]:
        """Apply a validation result under the permanent-record policy:
        - ever_working is a one-way flag, never unset.
        - a permanent (ever_working) record is NEVER deleted; only its
          `state`/`consecutive_failures`/`quarantine_until` change.
        - PERMANENTLY_FAILED is reachable only after MAX_CONSECUTIVE_FAILURES
          consecutive failed revalidations since the last success.
        """
        now = now_utc()
        doc = await self.proxies.find_one({"proxy_id": proxy_id})
        if not doc:
            return {}

        state = result.get("state", ProxyState.CONNECTION_FAILED)
        update: dict[str, Any] = {
            "lease_until": None,
            "last_tested_at": now,
            "last_error_category": result.get("error_category"),
            "last_error": short_error(result.get("error")) if result.get("error") else None,
            "latency_ms": result.get("latency_ms"),
            "generic_ok": result.get("generic_ok"),
        }

        if result.get("country_code"):
            update["verified_country"] = result["country_code"]
            update["country_name"] = result.get("country_name")

        if result.get("instagram_working") is not None:
            update["instagram_working"] = result.get("instagram_working")
            update["instagram_last_tested_at"] = now

        if state == ProxyState.YOUTUBE_WORKING:
            update.update(
                {
                    "state": ProxyState.YOUTUBE_WORKING,
                    "ever_working": True,
                    "enabled": True,
                    "consecutive_failures": 0,
                    "last_success_at": now,
                    "quarantine_until": None,
                    "next_validation_at": now + timedelta(seconds=Config.REVALIDATION_INTERVAL),
                    "youtube_score": min(100.0, safe_float(doc.get("youtube_score")) * 0.7 + 30.0),
                    "yt_dlp_version": result.get("yt_dlp_version"),
                    "formats_found": result.get("formats_found"),
                }
            )
        else:
            consecutive = safe_int(doc.get("consecutive_failures")) + 1
            update["consecutive_failures"] = consecutive
            update["last_failure_at"] = now
            update["youtube_score"] = max(0.0, safe_float(doc.get("youtube_score")) * 0.7)

            non_route = result.get("error_category") in FailureCategory.NON_ROUTE_SPECIFIC
            if non_route:
                # Don't penalize the proxy for a target/environment problem;
                # just retry soon without incrementing the failure counter.
                update["consecutive_failures"] = doc.get("consecutive_failures", 0)
                update["state"] = doc.get("state") if doc.get("ever_working") else ProxyState.QUARANTINED
                update["quarantine_until"] = now + timedelta(minutes=10)
            elif consecutive >= Config.MAX_CONSECUTIVE_FAILURES:
                update["state"] = ProxyState.PERMANENTLY_FAILED
                update["quarantine_until"] = None
            else:
                backoff = min(
                    Config.MAX_QUARANTINE_SECONDS,
                    Config.QUARANTINE_BASE_SECONDS * (2 ** (consecutive - 1)),
                )
                update["state"] = ProxyState.QUARANTINED
                update["quarantine_until"] = now + timedelta(seconds=backoff)
                update["next_validation_at"] = update["quarantine_until"]

        await self.proxies.update_one({"proxy_id": proxy_id}, {"$set": update})
        merged = {**doc, **update}
        await self.create_event(
            proxy_id,
            "TEST_RESULT",
            state=update.get("state", state),
            error_category=result.get("error_category"),
        )
        return merged

    async def mark_missing_from_sources(self, proxy_ids: list[str]) -> int:
        """A proxy no longer appears in ANY of its configured sources.
        This only flags `source_present=False` - it never changes
        working/state for a proxy that has ever been validated as working.
        """
        if not proxy_ids:
            return 0
        result = await self.proxies.update_many(
            {"proxy_id": {"$in": proxy_ids}},
            {"$set": {"source_present": False, "source_missing_since": now_utc()}},
        )
        return result.modified_count

    async def retire_orphans(self, older_than_seconds: int) -> int:
        """Retire proxies that were NEVER validated as working and have
        been absent from all sources for a long time. Never touches
        ever_working=True records."""
        cutoff = now_utc() - timedelta(seconds=older_than_seconds)
        result = await self.proxies.update_many(
            {
                "ever_working": False,
                "source_present": False,
                "source_missing_since": {"$lte": cutoff},
                "retired": {"$ne": True},
            },
            {"$set": {"retired": True, "state": ProxyState.RETIRED, "enabled": False}},
        )
        return result.modified_count

    async def cleanup_history(self, days: int = 30) -> dict[str, int]:
        """Bounded operational history: prunes only auxiliary logs
        (snapshots/tasks/events). Never touches the proxies collection."""
        cutoff = now_utc() - timedelta(days=days)
        out = {}
        for name, coll, field in (
            ("snapshots", self.snapshots, "fetched_at"),
            ("tasks", self.tasks, "created_at"),
            ("events", self.events, "created_at"),
        ):
            result = await coll.delete_many({field: {"$lt": cutoff}})
            out[name] = result.deleted_count
        return out

    async def get_stats(self) -> dict[str, Any]:
        total = await self.proxies.count_documents({})
        working = await self.proxies.count_documents({"state": ProxyState.YOUTUBE_WORKING, "enabled": True})
        quarantined = await self.proxies.count_documents({"state": ProxyState.QUARANTINED})
        permanently_failed = await self.proxies.count_documents({"state": ProxyState.PERMANENTLY_FAILED})
        retired = await self.proxies.count_documents({"retired": True})
        ever_working = await self.proxies.count_documents({"ever_working": True})
        return {
            "total": total,
            "working": working,
            "quarantined": quarantined,
            "permanently_failed": permanently_failed,
            "retired": retired,
            "ever_working": ever_working,
        }


# ============================================================================
# SOURCE MANAGER - generic GitHub + multi-format resolution
# ============================================================================

class ProxySourceManager:
    """Fetches configured sources and turns them into normalized, deduplicated
    proxy records. Deliberately has no autonomous discovery capability: it
    only ever touches URLs explicitly configured by the operator. Parsing is
    fully separate from validation (ProxyValidator)."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=Config.GENERIC_TIMEOUT * 2)
        headers = {"User-Agent": Config.USER_AGENT, "Accept": "*/*"}
        self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    # --- generic GitHub repo/tree resolution --------------------------------

    @staticmethod
    def _is_github_repo_url(url: str) -> bool:
        return urlparse(url).netloc.lower() == "github.com"

    async def _list_github_directory(self, owner: str, repo: str, branch: str, path: str) -> list[dict[str, Any]]:
        """Lightweight, non-recursive directory listing via the GitHub
        Contents API. Cheaper than the recursive Trees API and returns
        ready-to-use raw download URLs directly."""
        api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{quote(path)}?ref={quote(branch)}"
        async with self.session.get(api_url, headers={"Accept": "application/vnd.github+json"}) as response:
            if response.status != 200:
                raise RuntimeError(f"GitHub directory listing failed: HTTP {response.status}")
            data = await response.json(content_type=None)
        if not isinstance(data, list):
            raise RuntimeError("Unexpected GitHub contents API response (not a directory).")
        return data

    def _pick_preferred_file(self, entries: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        by_ext: dict[str, dict[str, Any]] = {}
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
        # fall back to any file found
        for entry in entries:
            if entry.get("type") == "file":
                return entry
        return None

    async def resolve_source_url(self, source: dict[str, Any]) -> tuple[str, str]:
        """Returns (fetch_url, resolved_format). For a direct raw/blob URL
        this is immediate. For a GitHub tree (directory) URL, this lists
        the directory once and caches the chosen raw file URL on the
        source document for SOURCE_RESOLVE_CACHE_SECONDS, so subsequent
        refreshes hit the raw file directly without re-listing."""
        url = str(source["url"]).strip()

        resolved_url = source.get("resolved_url")
        resolved_at = parse_dt(source.get("resolved_at"))
        if resolved_url and resolved_at:
            age = (now_utc() - resolved_at).total_seconds()
            if age < Config.SOURCE_RESOLVE_CACHE_SECONDS:
                return resolved_url, source.get("resolved_format") or "txt"

        if not self._is_github_repo_url(url):
            return url, detect_format("", url)

        parsed = urlparse(url)
        parts = [unquote(x) for x in parsed.path.split("/") if x]

        # /owner/repo/blob/branch/path -> direct raw file
        if len(parts) >= 5 and parts[2] == "blob":
            owner, repo, _, branch = parts[:4]
            file_path = "/".join(parts[4:])
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}"
            fmt = detect_format("", raw_url)
            await self.db.record_source_state(source["source_id"], resolved_url=raw_url, resolved_format=fmt)
            return raw_url, fmt

        # /owner/repo/tree/branch/path -> directory; pick the best file
        if len(parts) >= 5 and parts[2] == "tree":
            owner, repo, _, branch = parts[:4]
            path = "/".join(parts[4:])
            entries = await self._list_github_directory(owner, repo, branch, path)
            chosen = self._pick_preferred_file(entries)
            if not chosen or not chosen.get("download_url"):
                raise RuntimeError("No usable data file found in configured GitHub directory.")
            raw_url = chosen["download_url"]
            fmt = detect_format("", raw_url)
            await self.db.record_source_state(source["source_id"], resolved_url=raw_url, resolved_format=fmt)
            return raw_url, fmt

        # Plain github.com/owner/repo/... file view without /blob/ - best effort
        return url, detect_format("", url)

    # --- fetch (size-capped, retried, hashed) -------------------------------

    async def fetch(self, fetch_url: str) -> tuple[str, str, int]:
        if not self.session:
            raise RuntimeError("Source manager is not started.")

        max_bytes = Config.MAX_SOURCE_BYTES
        last_exc: Optional[Exception] = None
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=Config.GENERIC_TIMEOUT * 3)
                async with self.session.get(
                    fetch_url,
                    timeout=timeout,
                    allow_redirects=True,
                    headers={
                        "User-Agent": Config.USER_AGENT,
                        "Accept": "application/json,text/plain,text/*,*/*;q=0.8",
                    },
                ) as response:
                    if response.status >= 400:
                        raise RuntimeError(f"HTTP {response.status}")

                    content_type = response.headers.get("Content-Type", "")
                    body = bytearray()
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        body.extend(chunk)
                        if len(body) > max_bytes:
                            raise RuntimeError("Source exceeds MAX_SOURCE_BYTES.")

                    raw = bytes(body)
                    text = raw.decode("utf-8", errors="replace")
                    return text, content_type, len(raw)
            except Exception as exc:
                last_exc = exc
                if attempt < Config.MAX_RETRIES:
                    await asyncio.sleep(2 ** attempt)
        raise last_exc or RuntimeError("Unknown source fetch error.")

    async def preview(self, url: str) -> dict[str, Any]:
        source_id = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
        pseudo_source = {"source_id": source_id, "url": url, "resolved_url": None, "resolved_at": None}
        fetch_url, fmt = await self.resolve_source_url(pseudo_source)
        text, content_type, byte_count = await self.fetch(fetch_url)
        candidates = parse_source_payload(text, content_type, fetch_url)
        return {
            "url": url,
            "fetch_url": fetch_url,
            "format": fmt,
            "bytes": byte_count,
            "estimated_entries": len(candidates),
        }

    async def import_source(self, source: dict[str, Any], task_id: Optional[str] = None) -> dict[str, Any]:
        source_id = source["source_id"]
        fetch_url, fmt = await self.resolve_source_url(source)
        text, content_type, byte_count = await self.fetch(fetch_url)
        content_hash = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()

        if source.get("last_content_hash") == content_hash:
            await self.db.record_source_state(source_id, content_hash=content_hash, item_count=0, success=True)
            if task_id:
                await self.db.finish_task(task_id, "COMPLETED", unchanged=True)
            return {"source_id": source_id, "unchanged": True, "fetched": 0, "valid": 0, "new": 0,
                     "duplicates": 0, "invalid": 0, "already_known": 0, "bytes": byte_count}

        candidates = parse_source_payload(text, content_type, fetch_url)
        if len(candidates) > Config.MAX_DISCOVERED_PER_SOURCE:
            candidates = candidates[: Config.MAX_DISCOVERED_PER_SOURCE]

        seen: set[str] = set()
        invalid = 0
        duplicates = 0
        new_count = 0
        known_count = 0
        known_ids: list[str] = []
        source_country_default = source.get("country")

        for candidate in candidates:
            entry = parse_proxy_string(candidate.raw, default_scheme=candidate.scheme_hint or "http")
            if not entry:
                invalid += 1
                continue
            country = candidate.country or source_country_default
            entry = ProxyEntry(
                scheme=entry.scheme,
                host=entry.host,
                port=entry.port,
                username=entry.username,
                password=entry.password,
                source_id=source_id,
                source_country=country,
            )
            if entry.proxy_id in seen:
                duplicates += 1
                continue
            seen.add(entry.proxy_id)
            known_ids.append(entry.proxy_id)

            is_new, _ = await self.db.upsert_proxy(entry, country=country)
            if is_new:
                new_count += 1
            else:
                known_count += 1

        # Adapted from Code B's removal-reconciliation, but bounded to
        # "was this proxy seen in THIS source's latest fetch" (a single
        # overwritten field on the source doc, not a growing history), and
        # it never deletes or changes the state of an ever_working proxy.
        previous_ids = set(source.get("known_proxy_ids") or [])
        missing_ids = list(previous_ids - set(known_ids))
        if missing_ids:
            await self.db.mark_missing_from_sources(missing_ids)

        await self.db.save_snapshot(
            {
                "source_id": source_id,
                "fetched_at": now_utc(),
                "content_hash": content_hash,
                "count": len(known_ids),
                "added_count": new_count,
                "duplicate_count": duplicates + known_count,
                "invalid_count": invalid,
                "removed_count": len(missing_ids),
            }
        )
        await self.db.record_source_state(
            source_id,
            content_hash=content_hash,
            item_count=len(known_ids),
            known_proxy_ids=known_ids,
            success=True,
        )

        if task_id:
            await self.db.update_task(
                task_id,
                total_items=len(candidates),
                new_items=new_count,
                duplicates=duplicates + known_count,
                invalid=invalid,
                removed=len(missing_ids),
            )

        return {
            "source_id": source_id,
            "unchanged": False,
            "fetched": len(candidates),
            "valid": len(known_ids),
            "new": new_count,
            "duplicates": duplicates + known_count,
            "already_known": known_count,
            "invalid": invalid,
            "removed": len(missing_ids),
            "bytes": byte_count,
            "format": fmt,
        }


# ============================================================================
# VALIDATION - cheap check first, then YouTube, then optional Instagram
# ============================================================================

class ProxyValidator:
    def __init__(self) -> None:
        self._ytdlp_version: Optional[str] = None
        self._testing_now: set[str] = set()  # in-process guard against duplicate concurrent yt-dlp runs

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    # --- stage 1: cheapest possible check, no external HTTP at all ---------

    @staticmethod
    async def tcp_connect_check(entry: ProxyEntry) -> bool:
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

    # --- stage 2: generic connectivity + exit IP (only if country unknown) -

    async def generic_check_and_ip(self, entry: ProxyEntry) -> dict[str, Any]:
        proxy_url = entry.canonical
        started = time.monotonic()

        if entry.scheme.startswith("socks") and ProxyConnector is None:
            return {
                "generic_ok": False,
                "error_category": FailureCategory.ENVIRONMENT_ERROR,
                "error": "aiohttp-socks is required for SOCKS proxy validation.",
            }

        try:
            timeout = aiohttp.ClientTimeout(total=Config.GENERIC_TIMEOUT)
            if entry.scheme.startswith("socks"):
                connector = ProxyConnector.from_url(proxy_url)
                session = aiohttp.ClientSession(connector=connector, timeout=timeout,
                                                 headers={"User-Agent": Config.USER_AGENT})
                try:
                    async with session.get("https://api.ipify.org?format=json") as response:
                        body = await response.json(content_type=None)
                        ok = response.status == 200
                finally:
                    await session.close()
            else:
                async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": Config.USER_AGENT}) as session:
                    async with session.get("https://api.ipify.org?format=json", proxy=proxy_url) as response:
                        body = await response.json(content_type=None)
                        ok = response.status == 200

            latency_ms = (time.monotonic() - started) * 1000.0
            if not ok:
                return {"generic_ok": False, "error_category": FailureCategory.CONNECTION_REFUSED,
                        "error": "Non-200 from generic connectivity check.", "latency_ms": latency_ms}
            return {"generic_ok": True, "exit_ip": str(body.get("ip", "")).strip(), "latency_ms": latency_ms}
        except asyncio.TimeoutError:
            return {"generic_ok": False, "error_category": FailureCategory.CONNECTION_TIMEOUT,
                     "error": "Generic connectivity timeout.", "latency_ms": (time.monotonic() - started) * 1000.0}
        except Exception as exc:
            text = short_error(exc)
            lowered = text.lower()
            if any(k in lowered for k in ("407", "unauthorized", "proxy auth")):
                category = FailureCategory.PROXY_AUTH_FAILURE
            elif "name or service not known" in lowered or "getaddrinfo" in lowered:
                category = FailureCategory.DNS_FAILURE
            elif "refused" in lowered:
                category = FailureCategory.CONNECTION_REFUSED
            else:
                category = FailureCategory.PROXY_PROTOCOL_FAILURE
            return {"generic_ok": False, "error_category": category, "error": text,
                     "latency_ms": (time.monotonic() - started) * 1000.0}

    async def resolve_country(self, ip: str) -> dict[str, Any]:
        if not Config.ENABLE_GEO_LOOKUP or not ip:
            return {}
        try:
            ipaddress.ip_address(ip)
        except ValueError:
            return {}
        url = Config.GEO_LOOKUP_URL.replace("{ip}", quote(ip, safe=""))
        try:
            timeout = aiohttp.ClientTimeout(total=Config.GEO_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": Config.USER_AGENT}) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        return {}
                    data = await response.json(content_type=None)
                    return {
                        "country_code": (data.get("country_code") or data.get("country") or "").upper() or None,
                        "country_name": data.get("country") or data.get("country_name"),
                    }
        except Exception:
            return {}

    # --- stage 3: YouTube (the primary, decisive target) -------------------

    async def get_ytdlp_version(self) -> Optional[str]:
        if self._ytdlp_version:
            return self._ytdlp_version
        try:
            process = await asyncio.create_subprocess_exec(
                Config.YTDLP_BINARY, "--version",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=15)
            if process.returncode == 0:
                self._ytdlp_version = stdout.decode(errors="replace").strip()
        except Exception:
            self._ytdlp_version = None
        return self._ytdlp_version

    @staticmethod
    def classify_youtube_error(stderr: str, stdout: str) -> tuple[str, str, bool]:
        """Returns (failure_category, human_message, route_specific)."""
        text = (stderr + "\n" + stdout).lower()
        if "sign in to confirm you're not a bot" in text or "not a bot" in text:
            return FailureCategory.RATE_LIMITED, "Target rejected the route as automated traffic.", True
        if "the page needs to be reloaded" in text:
            return FailureCategory.TARGET_UNAVAILABLE, "Target requested a reload for this route.", False
        if "video unavailable" in text or "not available" in text:
            return FailureCategory.TARGET_UNAVAILABLE, "Test content unavailable from this route (geo).", True
        if "confirm your age" in text or "age-restricted" in text:
            return FailureCategory.TARGET_UNAVAILABLE, "Target requires an age gate.", False
        if "authentication" in text and "proxy" in text:
            return FailureCategory.PROXY_AUTH_FAILURE, "Proxy authentication failed.", True
        if "javascript" in text or "deno" in text or "ejs" in text:
            return FailureCategory.ENVIRONMENT_ERROR, "Local JS extraction environment failed.", False
        if "timed out" in text or "timeout" in text:
            return FailureCategory.CONNECTION_TIMEOUT, "yt-dlp timed out.", True
        if "name or service not known" in text or "getaddrinfo" in text:
            return FailureCategory.DNS_FAILURE, "DNS resolution failed through this route.", True
        if any(k in text for k in ("proxyerror", "connection reset", "connection refused")):
            return FailureCategory.CONNECTION_REFUSED, "The proxy route failed during extraction.", True
        if "yt-dlp" in text and "error" in text:
            return FailureCategory.EXTRACTION_FAILURE, "yt-dlp reported an extraction error.", False
        return FailureCategory.UNKNOWN, short_error(stderr or stdout), False

    async def test_youtube(self, entry: ProxyEntry, target_url: str) -> dict[str, Any]:
        proxy_url = entry.canonical
        started = time.monotonic()
        version = await self.get_ytdlp_version()

        if not shutil.which(Config.YTDLP_BINARY) and not Path(Config.YTDLP_BINARY).exists():
            return {
                "state": ProxyState.ENVIRONMENT_ERROR,
                "error_category": FailureCategory.ENVIRONMENT_ERROR,
                "error": f"yt-dlp binary not found: {Config.YTDLP_BINARY}",
                "duration": time.monotonic() - started,
            }

        command = [
            Config.YTDLP_BINARY, "--dump-single-json", "--skip-download", "--no-playlist",
            "--no-warnings", "--quiet",
            "--socket-timeout", str(Config.YOUTUBE_TIMEOUT),
            "--retries", "0", "--fragment-retries", "0",
            "--proxy", proxy_url, "--user-agent", Config.USER_AGENT,
        ]
        deno_path = shutil.which("deno")
        if deno_path:
            command += ["--js-runtimes", f"deno:{deno_path}"]
        if Config.YTDLP_REMOTE_COMPONENTS:
            command += ["--remote-components", Config.YTDLP_REMOTE_COMPONENTS]
        command += ["--", target_url]

        try:
            process = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=Config.YOUTUBE_TIMEOUT + 10)
            out = stdout.decode("utf-8", errors="replace")
            err = stderr.decode("utf-8", errors="replace")
            duration = time.monotonic() - started

            if process.returncode == 0:
                try:
                    info = json.loads(out)
                except json.JSONDecodeError:
                    info = {}
                formats = info.get("formats") or []
                title = info.get("title") or ""
                if formats and title:
                    return {
                        "state": ProxyState.YOUTUBE_WORKING,
                        "error_category": FailureCategory.SUCCESS,
                        "duration": duration, "latency_ms": duration * 1000.0,
                        "formats_found": min(len(formats), Config.MAX_YTDLP_FORMATS),
                        "yt_dlp_version": version, "title": title[:250],
                    }
                return {
                    "state": ProxyState.YOUTUBE_REJECTED,
                    "error_category": FailureCategory.EXTRACTION_FAILURE,
                    "error": "yt-dlp returned no usable formats/title.",
                    "duration": duration, "latency_ms": duration * 1000.0,
                }

            category, human, route_specific = self.classify_youtube_error(err, out)
            state = ProxyState.TIMEOUT if category == FailureCategory.CONNECTION_TIMEOUT else (
                ProxyState.ENVIRONMENT_ERROR if category in FailureCategory.NON_ROUTE_SPECIFIC
                else ProxyState.YOUTUBE_REJECTED
            )
            return {
                "state": state, "error_category": category, "error": human,
                "raw_error": short_error(err, 1400), "route_specific": route_specific,
                "duration": duration, "latency_ms": duration * 1000.0, "yt_dlp_version": version,
            }
        except asyncio.TimeoutError:
            duration = time.monotonic() - started
            return {"state": ProxyState.TIMEOUT, "error_category": FailureCategory.CONNECTION_TIMEOUT,
                     "error": "yt-dlp test process timed out.", "duration": duration,
                     "latency_ms": duration * 1000.0, "yt_dlp_version": version}
        except Exception as exc:
            return {"state": ProxyState.ENVIRONMENT_ERROR, "error_category": FailureCategory.ENVIRONMENT_ERROR,
                     "error": short_error(exc), "duration": time.monotonic() - started, "yt_dlp_version": version}

    # --- stage 4: optional, disabled-by-default Instagram connectivity -----

    async def test_instagram(self, entry: ProxyEntry) -> Optional[bool]:
        """Purely a connectivity check: can this route reach a public
        Instagram page over HTTPS? No login, no session, no automation of
        Instagram's product surface - just a reachability probe, matching
        the same shape as the generic connectivity check."""
        if not Config.INSTAGRAM_VALIDATION_ENABLED:
            return None
        proxy_url = entry.canonical
        try:
            timeout = aiohttp.ClientTimeout(total=Config.INSTAGRAM_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": Config.USER_AGENT}) as session:
                async with session.get(Config.INSTAGRAM_TEST_URL, proxy=proxy_url, allow_redirects=True) as response:
                    return response.status < 500
        except Exception:
            return False

    # --- orchestration -------------------------------------------------------

    async def validate(self, proxy: dict[str, Any], test_url: str) -> dict[str, Any]:
        entry = ProxyEntry(
            scheme=proxy.get("scheme", "http"),
            host=proxy["host"], port=safe_int(proxy["port"]),
            username=proxy.get("username"), password=proxy.get("password"),
        )

        # Stage 1: cheapest possible check - pure TCP, no bandwidth at all.
        reachable = await self.tcp_connect_check(entry)
        if not reachable:
            return {"state": ProxyState.CONNECTION_FAILED, "error_category": FailureCategory.CONNECTION_TIMEOUT,
                     "error": "TCP connect to proxy failed.", "generic_ok": False}

        # Stage 2: generic HTTP check + exit IP - skipped for country lookup
        # if the source already told us the country (bandwidth optimization).
        known_country = proxy.get("source_country") or proxy.get("verified_country")
        generic = await self.generic_check_and_ip(entry)
        if not generic.get("generic_ok"):
            return {"state": ProxyState.CONNECTION_FAILED, "generic_ok": False, **generic}

        country_info: dict[str, Any] = {}
        if not known_country:
            country_info = await self.resolve_country(generic.get("exit_ip", ""))
        else:
            country_info = {"country_code": known_country}

        # Stage 3: YouTube - the primary, decisive validation target.
        yt = await self.test_youtube(entry, test_url)

        result: dict[str, Any] = {
            "generic_ok": True,
            "latency_ms": generic.get("latency_ms"),
            **country_info,
            **yt,
        }

        # Stage 4: optional Instagram check, only runs if working on YouTube
        # or explicitly enabled - avoids extra requests when disabled.
        if Config.INSTAGRAM_VALIDATION_ENABLED:
            result["instagram_working"] = await self.test_instagram(entry)

        return result


# ============================================================================
# WORKER STATE / SCHEDULER
# ============================================================================

class WorkerState:
    def __init__(self) -> None:
        self.started_at = now_utc()
        self.stop_event = asyncio.Event()
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=Config.MAX_PENDING_TESTS)
        self.pending_ids: set[str] = set()
        self.pending_lock = asyncio.Lock()
        self.tasks: set[asyncio.Task] = set()
        self.active_tests = 0
        self.completed_tests = 0
        self.successful_tests = 0
        self.failed_tests = 0
        self.last_source_refresh: Optional[datetime] = None
        self.last_working_pool_count = 0
        self.critical_since: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.last_test_target_health = True
        self.test_target_checked_at: Optional[datetime] = None
        self.scheduler_running = False
        self.report_last_sent: Optional[datetime] = None

    def uptime_seconds(self) -> int:
        return int((now_utc() - self.started_at).total_seconds())

    def pool_health(self) -> str:
        if self.last_working_pool_count <= 0:
            return "CRITICAL"
        if self.last_working_pool_count < 5:
            return "LOW"
        return "OK"


class WorkerScheduler:
    """Single coarse-grained scheduler loop (Code A's model, kept
    deliberately - NOT Code B's 10-second tight loop). Everything expensive
    (source fetch, revalidation, quarantine recheck, history cleanup) is
    driven from one periodic tick at SOURCE_REFRESH_SECONDS. A separate
    lightweight dispatcher just drains the bounded test queue with bounded
    concurrency."""

    def __init__(self, db: Database, sources: ProxySourceManager, validator: ProxyValidator,
                 reports: "ReportManager", state: WorkerState, notify) -> None:
        self.db = db
        self.sources = sources
        self.validator = validator
        self.reports = reports
        self.state = state
        self.notify = notify
        self.dispatcher_task: Optional[asyncio.Task] = None
        self.periodic_task: Optional[asyncio.Task] = None
        self.semaphore = asyncio.Semaphore(Config.TEST_CONCURRENCY)

    async def start(self) -> None:
        self.state.scheduler_running = True
        self.dispatcher_task = asyncio.create_task(self.dispatch_loop(), name="proxy-dispatcher")
        self.periodic_task = asyncio.create_task(self.periodic_loop(), name="proxy-periodic")

    async def stop(self) -> None:
        self.state.stop_event.set()
        
        # Safely gather only tasks that were actually created
        tasks_to_wait = []
        for task in (self.dispatcher_task, self.periodic_task):
            if task is not None:
                task.cancel()
                tasks_to_wait.append(task)
                
        if tasks_to_wait:
            await asyncio.gather(*tasks_to_wait, return_exceptions=True)
            
        for task in list(self.state.tasks):
            task.cancel()
            
        if self.state.tasks:
            await asyncio.gather(*self.state.tasks, return_exceptions=True)
            
        self.state.scheduler_running = False

    # --- bounded dispatch queue ----------------------------------------------

    async def enqueue_proxy(self, proxy_id: str, priority: int = 100, reason: str = "new") -> bool:
        async with self.state.pending_lock:
            if proxy_id in self.state.pending_ids:
                return False
            if self.state.queue.full():
                logger.warning("[QUEUE] full; skipping proxy=%s reason=%s", proxy_id[:12], reason)
                return False
            self.state.pending_ids.add(proxy_id)
            await self.state.queue.put((priority, time.monotonic(), proxy_id, reason))
            return True

    async def dispatch_loop(self) -> None:
        logger.info("[SCHEDULER] dispatcher started")
        while not self.state.stop_event.is_set():
            try:
                item = await asyncio.wait_for(self.state.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            _, _, proxy_id, reason = item
            async with self.state.pending_lock:
                self.state.pending_ids.discard(proxy_id)
            task = asyncio.create_task(self.run_proxy_test(proxy_id, reason), name=f"proxy-test-{proxy_id[:8]}")
            self.state.tasks.add(task)
            task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task) -> None:
        self.state.tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[TASK] proxy test task crashed")

    async def run_proxy_test(self, proxy_id: str, reason: str) -> None:
        # In-process claim (fast path) + DB lease (cross-process/cross-worker
        # path) together prevent duplicate concurrent work on the same proxy,
        # whether from this worker's own queue or a second worker instance
        # sharing the same MongoDB.
        claimed = await self.db.claim_proxy(proxy_id)
        if not claimed:
            return

        async with self.semaphore:
            self.state.active_tests += 1
            try:
                test_url = str(await self.db.get_config("youtube_test_url", Config.YOUTUBE_TEST_URL))
                was_working = bool(claimed.get("ever_working")) and claimed.get("state") == ProxyState.YOUTUBE_WORKING
                result = await self.validator.validate(claimed, test_url)
                updated = await self.db.record_test_result(proxy_id, result)
                self.state.completed_tests += 1

                now_working = updated.get("state") == ProxyState.YOUTUBE_WORKING
                if now_working:
                    self.state.successful_tests += 1
                elif result.get("error_category") not in FailureCategory.NON_ROUTE_SPECIFIC:
                    self.state.failed_tests += 1

                await self._notify_state_change(updated, was_working, now_working)
            except Exception:
                logger.exception("[TEST] proxy test crashed id=%s", proxy_id[:12])
                await self.db.release_lease(proxy_id)
            finally:
                self.state.active_tests -= 1

    async def _notify_state_change(self, proxy: dict[str, Any], was_working: bool, now_working: bool) -> None:
        """Only notify on a meaningful transition, and only once per
        transition (tracked via last_notified_state), to avoid spamming the
        same result every scheduler cycle."""
        proxy_id = proxy.get("proxy_id", "")
        state = proxy.get("state")
        last_notified = proxy.get("last_notified_state")

        should_notify = False
        headline = ""
        if now_working and not was_working:
            should_notify = True
            headline = "🟢 Proxy recovered" if last_notified else "🟢 New working proxy"
        elif state == ProxyState.PERMANENTLY_FAILED and last_notified != ProxyState.PERMANENTLY_FAILED:
            should_notify = True
            headline = "🔴 Proxy permanently failed"

        if not should_notify or last_notified == state:
            return

        await self.db.proxies.update_one({"proxy_id": proxy_id}, {"$set": {"last_notified_state": state}})
        proxy_str = mask_proxy_string(f"{proxy.get('scheme','http')}://{proxy.get('host','')}:{proxy.get('port','')}")
        lines = [
            headline,
            f"Proxy: {proxy_str}",
            f"Protocol: {proxy.get('scheme')}",
            f"Status: {state}",
            "Test target: YouTube",
        ]
        if proxy.get("latency_ms"):
            lines.append(f"Latency: {int(proxy['latency_ms'])} ms")
        if proxy.get("verified_country") or proxy.get("source_country"):
            lines.append(f"Country: {proxy.get('verified_country') or proxy.get('source_country')}")
        if proxy.get("source_ids"):
            lines.append(f"Source: {', '.join(proxy['source_ids'][:3])}")
        lines.append(f"Validated at: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}")
        await self.notify("\n".join(lines))

    # --- queue population ------------------------------------------------------

    async def enqueue_new_candidates(self, source_id: Optional[str] = None) -> int:
        query: dict[str, Any] = {"retired": {"$ne": True}, "enabled": True, "state": ProxyState.NORMALIZED}
        if source_id:
            query["source_ids"] = source_id
        added = 0
        cursor = self.db.proxies.find(query).sort("first_seen_at", ASCENDING).limit(Config.MAX_TEST_PER_REFRESH)
        async for doc in cursor:
            if await self.enqueue_proxy(doc["proxy_id"], priority=10, reason="new"):
                added += 1
        return added

    async def enqueue_revalidation(self) -> int:
        now = now_utc()
        query = {
            "retired": {"$ne": True}, "enabled": True,
            "state": ProxyState.YOUTUBE_WORKING,
            "$or": [{"next_validation_at": None}, {"next_validation_at": {"$lte": now}}],
        }
        count = 0
        cursor = self.db.proxies.find(query).sort("youtube_score", DESCENDING).limit(Config.MAX_TEST_PER_REFRESH)
        async for doc in cursor:
            if await self.enqueue_proxy(doc["proxy_id"], priority=30, reason="revalidation"):
                count += 1
        return count

    async def enqueue_quarantine_rechecks(self) -> int:
        query = {
            "retired": {"$ne": True}, "enabled": True,
            "state": ProxyState.QUARANTINED,
            "quarantine_until": {"$lte": now_utc()},
        }
        count = 0
        cursor = self.db.proxies.find(query).sort("quarantine_until", ASCENDING).limit(200)
        async for doc in cursor:
            if await self.enqueue_proxy(doc["proxy_id"], priority=40, reason="quarantine-expired"):
                count += 1
        return count

    # --- source refresh -------------------------------------------------------

    async def source_refresh_once(self) -> dict[str, Any]:
        enabled_sources = await self.db.get_sources(enabled_only=True)
        aggregate = Counter()

        for source in enabled_sources:
            interval = safe_int(source.get("fetch_interval"), Config.SOURCE_REFRESH_SECONDS)
            last_checked = parse_dt(source.get("last_checked_at"))
            if last_checked and (now_utc() - last_checked).total_seconds() < interval:
                continue  # this specific source isn't due yet (per-source interval)

            task_id = await self.db.create_task("SOURCE_REFRESH", source["source_id"])
            await self.db.start_task(task_id)
            try:
                result = await self.sources.import_source(source, task_id)
                aggregate["sources"] += 1
                aggregate["fetched"] += safe_int(result.get("fetched"))
                aggregate["valid"] += safe_int(result.get("valid"))
                aggregate["new"] += safe_int(result.get("new"))
                aggregate["duplicates"] += safe_int(result.get("duplicates"))
                aggregate["invalid"] += safe_int(result.get("invalid"))

                if not result.get("unchanged"):
                    await self.enqueue_new_candidates(source["source_id"])

                await self.db.finish_task(task_id, "COMPLETED", result_summary=result)
            except Exception as exc:
                await self.db.record_source_state(source["source_id"], success=False, error=short_error(exc))
                await self.db.finish_task(task_id, "FAILED", error=short_error(exc))
                logger.error("[SOURCE] %s failed: %s", source["source_id"], short_error(exc))

                latest = await self.db.get_source(source["source_id"])
                if latest and safe_int(latest.get("failure_count")) >= Config.SOURCE_FAILURE_ALERT_THRESHOLD:
                    await self.notify(
                        f"⚠️ Source failure threshold reached\nSource: {source['name']}\nError: {short_error(exc, 300)}"
                    )

        await self.enqueue_revalidation()
        await self.enqueue_quarantine_rechecks()
        self.state.last_source_refresh = now_utc()
        return dict(aggregate)

    async def check_test_target(self) -> bool:
        target = str(await self.db.get_config("youtube_test_url", Config.YOUTUBE_TEST_URL))
        self.state.test_target_checked_at = now_utc()
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": Config.USER_AGENT}) as session:
                async with session.head(target, allow_redirects=True) as response:
                    if response.status in {200, 301, 302, 303, 307, 308, 405}:
                        self.state.last_test_target_health = True
                        return True
                async with session.get(target, allow_redirects=True) as response:
                    self.state.last_test_target_health = response.status < 500
                    return self.state.last_test_target_health
        except Exception as exc:
            logger.warning("[TARGET] health check failed: %s", short_error(exc))
            self.state.last_test_target_health = False
            return False

    async def periodic_loop(self) -> None:
        logger.info("[SCHEDULER] periodic loop started (interval=%ss)", Config.SOURCE_REFRESH_SECONDS)
        first_run = True
        while not self.state.stop_event.is_set():
            try:
                if first_run:
                    first_run = False
                else:
                    await asyncio.sleep(Config.SOURCE_REFRESH_SECONDS)

                # Keep Code A's safeguard: never bulk-test against a
                # currently-unhealthy target, so we don't wrongly punish a
                # large batch of otherwise-good proxies.
                target_ok = await self.check_test_target()
                if not target_ok:
                    await self.notify("🟠 YouTube test target appears unhealthy. Bulk testing paused this cycle.")
                else:
                    try:
                        summary = await self.source_refresh_once()
                        logger.info(
                            "[SOURCE] refresh sources=%s fetched=%s valid=%s new=%s dup=%s invalid=%s",
                            summary.get("sources", 0), summary.get("fetched", 0), summary.get("valid", 0),
                            summary.get("new", 0), summary.get("duplicates", 0), summary.get("invalid", 0),
                        )
                    except Exception:
                        logger.exception("[SCHEDULER] refresh failed")

                await self.db.release_expired_leases()
                await self.db.retire_orphans(Config.ORPHAN_RETIRE_AFTER_SECONDS)
                await self.db.cleanup_history(days=30)
                await self.refresh_pool_health()
                await self.maybe_daily_report()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = short_error(exc)
                logger.exception("[SCHEDULER] periodic loop error")
                await asyncio.sleep(5)

    async def refresh_pool_health(self) -> str:
        count = await self.db.proxies.count_documents(
            {"enabled": True, "retired": {"$ne": True}, "state": ProxyState.YOUTUBE_WORKING}
        )
        old = self.state.last_working_pool_count
        self.state.last_working_pool_count = count
        health = self.state.pool_health()

        if health == "CRITICAL":
            if self.state.critical_since is None:
                self.state.critical_since = now_utc()
                await self.notify("🔴 CRITICAL: no verified YouTube-working proxy is currently available.")
        else:
            self.state.critical_since = None

        if old > 0 and count == 0:
            await self.notify("🔴 Proxy pool dropped to zero verified YouTube-working routes.")
        return health

    async def maybe_daily_report(self) -> None:
        if not Config.REPORT_ENABLED:
            return
        current = now_utc()
        already = await self.db.get_config("last_daily_report_date")
        today = current.strftime("%Y-%m-%d")
        if already == today:
            return
        if current.hour < Config.DAILY_REPORT_HOUR:
            return
        if current.hour == Config.DAILY_REPORT_HOUR and current.minute < Config.DAILY_REPORT_MINUTE:
            return

        summary = await self.reports.daily_summary()
        await self.reports.persist_daily_summary(summary)
        await self.notify(self.reports.format_daily_summary(summary))
        await self.db.set_config("last_daily_report_date", today)
        self.state.report_last_sent = now_utc()

    async def test_specific(self, proxy_id: str) -> dict[str, Any]:
        proxy = await self.db.get_proxy(proxy_id)
        if not proxy:
            return {"ok": False, "error": "Proxy not found."}
        test_url = str(await self.db.get_config("youtube_test_url", Config.YOUTUBE_TEST_URL))
        result = await self.validator.validate(proxy, test_url)
        updated = await self.db.record_test_result(proxy_id, result)
        return {"ok": True, **updated}


# ============================================================================
# REPORTS
# ============================================================================

class ReportManager:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def system_stats(self) -> dict[str, Any]:
        return await self.db.get_stats()

    async def country_report(self) -> list[dict[str, Any]]:
        pipeline = [
            {"$match": {"state": ProxyState.YOUTUBE_WORKING, "enabled": True}},
            {"$group": {"_id": "$verified_country", "working": {"$sum": 1}}},
            {"$sort": {"working": -1}},
            {"$limit": 20},
        ]
        return await self.db.proxies.aggregate(pipeline).to_list(length=20)

    async def export_working(self, country: Optional[str] = None) -> bytes:
        query: dict[str, Any] = {"state": ProxyState.YOUTUBE_WORKING, "enabled": True}
        if country:
            query["verified_country"] = country.upper()
        lines = []
        cursor = self.db.proxies.find(query).sort("youtube_score", DESCENDING).limit(5000)
        async for doc in cursor:
            entry = ProxyEntry(
                scheme=doc.get("scheme", "http"), host=doc["host"], port=doc["port"],
                username=doc.get("username"), password=doc.get("password"),
            )
            lines.append(entry.canonical)
        return "\n".join(lines).encode("utf-8")

    async def daily_summary(self) -> dict[str, Any]:
        cutoff = now_utc() - timedelta(hours=24)
        stats = await self.db.get_stats()
        new_24h = await self.db.proxies.count_documents({"first_seen_at": {"$gte": cutoff}})
        tested_24h = await self.db.proxies.count_documents({"last_tested_at": {"$gte": cutoff}})
        working_24h = await self.db.proxies.count_documents({"last_success_at": {"$gte": cutoff}})
        countries = await self.country_report()
        pipeline = [
            {"$match": {"state": ProxyState.YOUTUBE_WORKING, "latency_ms": {"$ne": None}}},
            {"$group": {"_id": None, "avg": {"$avg": "$latency_ms"}}},
        ]
        avg_result = await self.db.proxies.aggregate(pipeline).to_list(length=1)
        avg_latency = safe_int(avg_result[0]["avg"]) if avg_result else 0

        return {
            "total": stats["total"], "working": stats["working"],
            "new_24h": new_24h, "tested_24h": tested_24h, "working_24h": working_24h,
            "avg_latency_ms": avg_latency, "countries": countries,
        }

    async def persist_daily_summary(self, summary: dict[str, Any]) -> None:
        date_key = now_utc().strftime("%Y-%m-%d")
        await self.db.daily.update_one({"date": date_key}, {"$set": {**summary, "date": date_key}}, upsert=True)

    @staticmethod
    def format_daily_summary(summary: dict[str, Any]) -> str:
        lines = [
            "📊 DAILY WORKER REPORT", "",
            f"🌐 Total known: {summary.get('total', 0)}",
            f"🆕 New (24h): {summary.get('new_24h', 0)}",
            f"🧪 Tested (24h): {summary.get('tested_24h', 0)}",
            f"🟢 Working (24h): {summary.get('working_24h', 0)}",
            f"📈 Current working pool: {summary.get('working', 0)}",
            f"⏱️ Average latency: {summary.get('avg_latency_ms', 0)} ms",
        ]
        countries = summary.get("countries") or []
        if countries:
            lines.append("")
            lines.append("Top countries:")
            for row in countries[:10]:
                lines.append(f"{row.get('_id') or 'UNKNOWN'}: working={row.get('working', 0)}")
        return "\n".join(lines)


# ============================================================================
# TELEGRAM ADMIN UI
# ============================================================================

class TelegramAdminUI:
    def __init__(self, db: Database, sources: ProxySourceManager, scheduler: WorkerScheduler,
                 reports: ReportManager, state: WorkerState) -> None:
        self.db = db
        self.sources = sources
        self.scheduler = scheduler
        self.reports = reports
        self.state = state
        self.bot: Optional[Client] = None
        self._pending_input: dict[int, str] = {}

    def authorized(self, user_id: int) -> bool:
        return user_id == Config.OWNER_ID

    async def notify(self, text: str) -> None:
        if not self.bot:
            return
        try:
            await self.bot.send_message(Config.ADMIN_CHAT_ID, text)
        except FloodWait as exc:
            await asyncio.sleep(getattr(exc, "value", 5))
            try:
                await self.bot.send_message(Config.ADMIN_CHAT_ID, text)
            except Exception:
                logger.exception("[TG] notify retry failed")
        except Exception:
            logger.exception("[TG] notify failed")

    def dashboard_markup(self):
        if InlineKeyboardMarkup is None:
            return None
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📊 Stats", callback_data="stats"),
                 InlineKeyboardButton("🌍 Countries", callback_data="countries")],
                [InlineKeyboardButton("🗂 Sources", callback_data="sources"),
                 InlineKeyboardButton("➕ Add source", callback_data="add_source")],
                [InlineKeyboardButton("🔁 Refresh now", callback_data="refresh_now"),
                 InlineKeyboardButton("📤 Export working", callback_data="export")],
                [InlineKeyboardButton("⚙️ Health", callback_data="health")],
            ]
        )

    async def send_dashboard(self, message: Message) -> None:
        text = (
            "🤖 Proxy Worker Bot\n\n"
            f"Uptime: {self.state.uptime_seconds() // 60} min\n"
            f"Pool health: {self.state.pool_health()}\n"
            f"Working pool: {self.state.last_working_pool_count}"
        )
        await message.reply_text(text, reply_markup=self.dashboard_markup())

    async def setup(self) -> None:
        if Client is None:
            logger.warning("[TG] pyrogram not available; Telegram admin UI disabled.")
            return
        self.bot = Client(
            "proxy_worker_bot", bot_token=Config.BOT_TOKEN,
            api_id=env_int("API_ID", 0), api_hash=os.getenv("API_HASH", "").strip(),
            in_memory=True,
        )

        @self.bot.on_message(filters.command("start") & filters.private)
        async def _start(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            await self.send_dashboard(message)

        @self.bot.on_message(filters.command("add_source") & filters.private)
        async def _add_source(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                await message.reply_text(
                    "Usage: /add_source <url> [name]\n\n"
                    "Supports direct TXT/JSON/CSV URLs and GitHub blob/tree URLs."
                )
                return
            rest = parts[1].strip().split(maxsplit=1)
            url = rest[0]
            name = rest[1] if len(rest) > 1 else urlparse(url).netloc or url
            source_id = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
            try:
                preview = await self.sources.preview(url)
            except Exception as exc:
                await message.reply_text(f"❌ Could not fetch/parse source: {short_error(exc, 300)}")
                return
            await self.db.upsert_source(
                {
                    "source_id": source_id, "name": name, "url": url,
                    "source_type": "MANUAL", "enabled": True,
                    "country": None, "protocol": None,
                    "priority": 200, "fetch_interval": Config.SOURCE_REFRESH_SECONDS,
                }
            )
            await message.reply_text(
                f"✅ Source added: {name}\n"
                f"Resolved format: {preview['format']}\n"
                f"Estimated entries: {preview['estimated_entries']}\n"
                f"This source will now be treated as a configured, automatically-refreshed source "
                f"(every {Config.SOURCE_REFRESH_SECONDS}s by default)."
            )

        @self.bot.on_message(filters.command("sources") & filters.private)
        async def _sources(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            await self.send_sources(message)

        @self.bot.on_message(filters.command("stats") & filters.private)
        async def _stats(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            stats = await self.reports.system_stats()
            await message.reply_text(
                "📊 Stats\n" + "\n".join(f"{k}: {v}" for k, v in stats.items())
            )

        @self.bot.on_message(filters.command("refresh_now") & filters.private)
        async def _refresh(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            await message.reply_text("Running source refresh now...")
            summary = await self.scheduler.source_refresh_once()
            await message.reply_text(f"Done: {summary}")

        @self.bot.on_message(filters.command("test") & filters.private)
        async def _test(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                await message.reply_text("Usage: /test <proxy_id>")
                return
            result = await self.scheduler.test_specific(parts[1].strip())
            await message.reply_text(f"Result: {json.dumps({k: v for k, v in result.items() if k != '_id'}, default=str)[:3500]}")

        @self.bot.on_message(filters.command("export") & filters.private)
        async def _export(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            data = await self.reports.export_working()
            path = f"/tmp/working_proxies_{int(time.time())}.txt"
            with open(path, "wb") as fh:
                fh.write(data)
            await message.reply_document(path, caption="Verified YouTube-working proxies")

        @self.bot.on_callback_query()
        async def _callback(_, callback: CallbackQuery):
            if not self.authorized(callback.from_user.id):
                await callback.answer("Unauthorized", show_alert=True)
                return
            data = callback.data
            if data == "stats":
                stats = await self.reports.system_stats()
                await callback.message.edit_text(
                    "📊 Stats\n" + "\n".join(f"{k}: {v}" for k, v in stats.items()),
                    reply_markup=self.dashboard_markup(),
                )
            elif data == "countries":
                rows = await self.reports.country_report()
                text = "🌍 Countries\n" + "\n".join(
                    f"{r.get('_id') or 'UNKNOWN'}: {r.get('working', 0)}" for r in rows
                ) or "No data yet."
                await callback.message.edit_text(text, reply_markup=self.dashboard_markup())
            elif data == "sources":
                await self.send_sources(callback.message)
            elif data == "add_source":
                await callback.message.reply_text("Send: /add_source <url> [name]")
            elif data == "refresh_now":
                await callback.answer("Refreshing...")
                summary = await self.scheduler.source_refresh_once()
                await callback.message.reply_text(f"Refresh done: {summary}")
            elif data == "export":
                data_bytes = await self.reports.export_working()
                path = f"/tmp/working_proxies_{int(time.time())}.txt"
                with open(path, "wb") as fh:
                    fh.write(data_bytes)
                await callback.message.reply_document(path, caption="Verified YouTube-working proxies")
            elif data == "health":
                await callback.message.edit_text(await self.health_text(), reply_markup=self.dashboard_markup())
            await callback.answer()

    async def send_sources(self, message: Message) -> None:
        sources = await self.db.get_sources()
        if not sources:
            await message.reply_text("No sources configured yet. Use /add_source <url> [name].")
            return
        lines = ["🗂 Configured sources:"]
        for src in sources:
            status = "✅" if src.get("enabled") else "⛔"
            lines.append(
                f"{status} {src.get('name')} | fmt={src.get('resolved_format') or '?'} "
                f"| last_items={src.get('last_item_count', 0)} | fails={src.get('failure_count', 0)}"
            )
        await message.reply_text("\n".join(lines))

    async def health_text(self) -> str:
        stats = await self.reports.system_stats()
        db_ok = await self.db.ping()
        return (
            "⚙️ Health\n"
            f"Mongo: {'OK' if db_ok else 'DOWN'}\n"
            f"Pool health: {self.state.pool_health()}\n"
            f"Working: {stats['working']} / Total: {stats['total']}\n"
            f"Active tests: {self.state.active_tests}\n"
            f"Target healthy: {self.state.last_test_target_health}"
        )

    async def start(self) -> None:
        if not self.bot:
            return
        await self.bot.start()
        logger.info("[TG] admin bot started")

    async def stop(self) -> None:
        if self.bot:
            try:
                await self.bot.stop()
            except Exception:
                pass


# ============================================================================
# HEALTH SERVER
# ============================================================================

class HealthServer:
    def __init__(self, db: Database, state: WorkerState) -> None:
        self.db = db
        self.state = state
        self.app = web.Application()
        self.app.add_routes([
            web.get("/", self.root),
            web.get("/health", self.health),
            web.get("/ready", self.ready),
        ])
        self.runner: Optional[web.AppRunner] = None

    async def root(self, request: web.Request) -> web.Response:
        return web.json_response({"service": "proxy-worker-bot", "status": "running"})

    async def health(self, request: web.Request) -> web.Response:
        db_ok = await self.db.ping()
        status = 200 if db_ok else 503
        return web.json_response(
            {
                "status": "ok" if db_ok else "degraded",
                "mongo": db_ok,
                "uptime_seconds": self.state.uptime_seconds(),
                "scheduler_running": self.state.scheduler_running,
                "pool_health": self.state.pool_health(),
                "working_pool": self.state.last_working_pool_count,
                "active_tests": self.state.active_tests,
                "target_healthy": self.state.last_test_target_health,
            },
            status=status,
        )

    async def ready(self, request: web.Request) -> web.Response:
        ok = await self.db.ping() and self.state.scheduler_running
        return web.json_response({"ready": ok}, status=200 if ok else 503)

    async def start(self) -> None:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", Config.PORT)
        await site.start()
        logger.info("[HEALTH] server listening on 0.0.0.0:%s", Config.PORT)

    async def stop(self) -> None:
        if self.runner:
            await self.runner.cleanup()


# ============================================================================
# APPLICATION
# ============================================================================

class Application:
    def __init__(self) -> None:
        self.db = Database()
        self.sources = ProxySourceManager(self.db)
        self.validator = ProxyValidator()
        self.reports = ReportManager(self.db)
        self.state = WorkerState()
        self.admin_ui = TelegramAdminUI(self.db, self.sources, None, self.reports, self.state)  # scheduler set below
        self.scheduler = WorkerScheduler(self.db, self.sources, self.validator, self.reports, self.state,
                                          notify=self.admin_ui.notify)
        self.admin_ui.scheduler = self.scheduler
        self.health_server = HealthServer(self.db, self.state)

    async def start(self) -> None:
        Config.validate()
        await self.db.connect()
        await self.db.initialize_defaults()
        await self.sources.start()
        await self.validator.start()
        await self.admin_ui.setup()
        await self.admin_ui.start()
        await self.health_server.start()
        await self.scheduler.start()

        try:
            await self.admin_ui.notify(
                "🟢 Proxy Worker Bot v2 started.\n"
                "Foundation: Code A architecture, restart-safe, permanent working-proxy retention."
            )
        except Exception:
            logger.exception("[APP] startup notification failed")

        logger.info("[APP] initialization complete")

    async def stop(self) -> None:
        logger.info("[APP] shutting down...")
        await self.scheduler.stop()
        await self.health_server.stop()
        await self.admin_ui.stop()
        await self.validator.close()
        await self.sources.close()
        await self.db.close()
        logger.info("[APP] shutdown complete")

    async def run(self) -> None:
        await self.start()
        stop_signal = asyncio.Event()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_signal.set)
            except NotImplementedError:
                pass  # signal handlers unsupported on this platform (e.g. some Windows setups)

        await stop_signal.wait()
        await self.stop()


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main() -> None:
    app = Application()
    try:
        await app.run()
    except (KeyboardInterrupt, SystemExit):
        await app.stop()
    except Exception:
        logger.critical("Fatal error", exc_info=True)
        await app.stop()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
