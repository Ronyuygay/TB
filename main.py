#!/usr/bin/env python3
"""
Proxy Connectivity-Intelligence Bot (v3)

Purpose
-------
This service measures and tracks the CONNECTIVITY QUALITY of proxies for a
private platform. It does not download, extract, or redistribute content
from any third-party platform, and it does not attempt to bypass, defeat,
or evade authentication, CAPTCHAs, rate limits, or bot-detection controls
on any third-party platform.

YouTube, Instagram, and TikTok are used ONLY as fixed, highly-available
reachability targets: their public homepages are large, CDN-backed
endpoints that make good real-world indicators of whether a proxy route
has good latency, stability, and reliability. A request against one of
these homepages is treated purely as a network probe:

    - Did the proxy complete a TCP connection?
    - Did it complete a TLS handshake?
    - Did an HTTP response come back within the timeout?
    - How long did that round trip take?

ANY HTTP response code (200, 301, 403, 429, etc.) counts as a successful
connectivity probe, because it proves the proxy route can reach that
platform's edge infrastructure and relay a full HTTP round trip. This
service never inspects, retries-to-evade, or treats a security/bot-check
response (401/403/429/CAPTCHA page) as something to defeat - it is simply
recorded as one more piece of connection-quality telemetry (see
"HTTP status behaviour" below), on equal footing with everything else.
Only genuine connection failures (timeout, refused, DNS failure, TLS
failure, proxy-auth failure) affect a proxy's health state or score.

Tracked, purely-connectivity metrics per proxy per platform:
    - reachability / success rate
    - latency (p50-ish rolling average)
    - connection stability (failure streaks, variance)
    - timeout rate
    - HTTP status-code distribution (informational only)
    - proxy scheme/type
    - geographic location + ASN/network info (from IP geolocation, not
      from the tested platform)
    - HTTPS/TLS support
    - historical performance / health-state over time

Foundation kept from v2 ("Code A" architecture): coarse-grained scheduler,
bounded queue, bounded test concurrency, byte-capped source fetches,
content-hash change detection, lease-based duplicate-work prevention,
generic GitHub repo/tree source resolution, permanent-record retention
(a proxy that was ever WORKING is never hard-deleted).

New in v3:
    - Staged revalidation state machine per platform: WORKING (hourly
      recheck) -> QUARANTINED (5h recheck, up to 48h) -> DISABLED
      (excluded from delivery, record retained).
    - Three independent platform collections: proxies (YouTube, kept as
      the existing collection name for backward compatibility),
      proxies_instagram, proxies_tiktok - identical schema.
    - Three independent Telegram log channels, one per platform. Only
      WORKING and RECOVERED events are posted.
    - Manual single-proxy priority check (/addproxy), jumps the queue,
      tests concurrently across all enabled platforms.
    - Bulk paste / multi-file ingestion (/addlist) through the same
      parse -> normalize -> dedupe pipeline as configured sources.
    - No-password-given marking for proxies whose source string implies
      auth but is missing credentials (still tested, flagged for
      visibility, never hard-excluded).
    - Bandwidth-conscious scheduling: per-platform test budget per tick,
      skip-retest-if-not-due, TCP pre-check before any HTTP request.
    - Multi-source auto-discovery (GitHub sibling files; curated public
      aggregators, off by default) using the same fetch/hash/dedupe path
      as manual sources, so it can never cost more bandwidth per source.
    - Per-platform dashboard panels.
    - Fix: the dashboard's pool-refresh trigger now awaits its background
      task via a stored reference with a logged done-callback, instead of
      firing an un-awaited create_task() whose exceptions were silently
      swallowed by the event loop.

Python: 3.12+
Required env: BOT_TOKEN, OWNER_ID, MONGO_URI
Recommended packages: aiohttp, aiohttp-socks, pyrogram, tgcrypto, pymongo
Run: python main_v3.py
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
import signal
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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


PLATFORMS: tuple[str, ...] = ("youtube", "instagram", "tiktok")


class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
    OWNER_ID = env_int("OWNER_ID", 0, 1)
    MONGO_URI = os.getenv("MONGO_URI", "").strip()
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "telegram_downloader").strip()

    PORT = env_int("PORT", 8080, 1, 65535)

    # --- Scheduler cadence ---
    SOURCE_REFRESH_SECONDS = env_int("SOURCE_REFRESH_SECONDS", 300, 30)
    TEST_CONCURRENCY = env_int("PROXY_TEST_CONCURRENCY", 5, 1, 50)
    MAX_PENDING_TESTS = env_int("MAX_PENDING_TESTS", 1000, 1, 10000)

    # Per-platform new-candidate test budget per scheduler tick (bandwidth guard).
    PLATFORM_MAX_TEST_PER_REFRESH = env_int("PLATFORM_MAX_TEST_PER_REFRESH", 150, 1, 5000)

    # --- Timeouts ---
    CONNECT_CHECK_TIMEOUT = env_int("CONNECT_CHECK_TIMEOUT", 6, 1, 30)
    GEO_TIMEOUT = env_int("GEO_TIMEOUT", 10, 3, 60)

    PLATFORM_TIMEOUT_SECONDS: dict[str, int] = {
        "youtube": env_int("YOUTUBE_TEST_TIMEOUT", 15, 3, 60),
        "instagram": env_int("INSTAGRAM_TEST_TIMEOUT", 15, 3, 60),
        "tiktok": env_int("TIKTOK_TEST_TIMEOUT", 15, 3, 60),
    }

    # Reachability targets: fixed, public homepages only. Never content,
    # video, or API endpoints - purely a network round-trip probe.
    PLATFORM_TEST_URLS: dict[str, str] = {
        "youtube": os.getenv("YOUTUBE_TEST_URL", "https://www.youtube.com/").strip(),
        "instagram": os.getenv("INSTAGRAM_TEST_URL", "https://www.instagram.com/").strip(),
        "tiktok": os.getenv("TIKTOK_TEST_URL", "https://www.tiktok.com/").strip(),
    }

    PLATFORM_ENABLED_DEFAULT: dict[str, bool] = {
        "youtube": env_bool("YOUTUBE_VALIDATION_ENABLED", True),
        "instagram": env_bool("INSTAGRAM_VALIDATION_ENABLED", True),
        "tiktok": env_bool("TIKTOK_VALIDATION_ENABLED", True),
    }

    PLATFORM_COLLECTIONS: dict[str, str] = {
        "youtube": os.getenv("MONGO_COLLECTION", "proxies").strip(),  # kept for backward compat
        "instagram": "proxies_instagram",
        "tiktok": "proxies_tiktok",
    }

    PLATFORM_LOG_CHANNEL_ID: dict[str, int] = {
        "youtube": env_int("YOUTUBE_LOG_CHANNEL_ID", 0),
        "instagram": env_int("INSTAGRAM_LOG_CHANNEL_ID", 0),
        "tiktok": env_int("TIKTOK_LOG_CHANNEL_ID", 0),
    }

    # --- Staged revalidation state machine (requirement #1) ---
    # WORKING     -> rechecked hourly
    # QUARANTINED -> rechecked every 5h, escalates to DISABLED after 48h
    # DISABLED    -> excluded from delivery/scheduling; record retained
    WORKING_RECHECK_SECONDS = env_int("WORKING_RECHECK_SECONDS", 3600, 300, 24 * 3600)
    QUARANTINE_RECHECK_SECONDS = env_int("QUARANTINE_RECHECK_SECONDS", 5 * 3600, 300, 24 * 3600)
    QUARANTINE_DISABLE_AFTER_SECONDS = env_int("QUARANTINE_DISABLE_AFTER_SECONDS", 48 * 3600, 3600, 30 * 86400)

    # Only affects proxies that were never validated as working on ANY
    # platform. Ever-working records are permanent and never auto-retired.
    ORPHAN_RETIRE_AFTER_SECONDS = env_int("ORPHAN_RETIRE_AFTER_SECONDS", 7 * 86400, 3600, 60 * 86400)

    DAILY_REPORT_HOUR = env_int("DAILY_REPORT_HOUR", 9, 0, 23)
    DAILY_REPORT_MINUTE = env_int("DAILY_REPORT_MINUTE", 0, 0, 59)

    ENABLE_GEO_LOOKUP = env_bool("ENABLE_GEO_LOOKUP", True)
    # ipwho.is returns country + connection.{asn,isp,org} in one call, so
    # ASN/network metadata is free alongside the geo lookup.
    GEO_LOOKUP_URL = os.getenv("GEO_LOOKUP_URL", "https://ipwho.is/{ip}").strip()

    REPORT_ENABLED = env_bool("REPORT_ENABLED", True)
    DEBUG = env_bool("DEBUG", False)
    DRY_RUN = env_bool("DRY_RUN", False)

    MAX_SOURCE_BYTES = env_int("MAX_SOURCE_BYTES", 50 * 1024 * 1024, 1024, 200 * 1024 * 1024)
    MAX_DISCOVERED_PER_SOURCE = env_int("MAX_DISCOVERED_PER_SOURCE", 10000, 1, 100000)
    SOURCE_FAILURE_ALERT_THRESHOLD = env_int("SOURCE_FAILURE_ALERT_THRESHOLD", 3, 1, 20)
    MAX_RETRIES = env_int("NETWORK_RETRIES", 1, 0, 3)
    ADMIN_CHAT_ID = env_int("REPORT_CHAT_ID", OWNER_ID, 1)

    SOURCE_RESOLVE_CACHE_SECONDS = env_int("SOURCE_RESOLVE_CACHE_SECONDS", 6 * 3600, 300, 7 * 86400)
    PREFERRED_SOURCE_FORMATS = env_list("PREFERRED_SOURCE_FORMATS", ("json", "txt", "csv"))

    # --- Multi-source auto-discovery (requirement #7) ---
    DISCOVERY_ENABLED = env_bool("DISCOVERY_ENABLED", False)
    DISCOVERY_INTERVAL_SECONDS = env_int("DISCOVERY_INTERVAL_SECONDS", 45 * 60, 300, 6 * 3600)
    # Off by default: a small curated list of well-known public proxy-list
    # aggregators the operator can opt into. Each entry goes through the
    # exact same fetch->hash->parse->dedupe path (and byte cap) as any
    # manually-configured source, so it can never cost more bandwidth.
    DISCOVERY_SEED_AGGREGATORS: tuple[str, ...] = env_list("DISCOVERY_SEED_AGGREGATORS", ())

    USER_AGENT = os.getenv(
        "HTTP_USER_AGENT",
        "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0 Mobile Safari/537.36",
    ).strip()

    DEFAULT_SOURCES: tuple[dict[str, Any], ...] = ()

    # Manual bulk-ingestion limits (requirement #4/#5)
    MAX_MANUAL_ITEMS_PER_BATCH = env_int("MAX_MANUAL_ITEMS_PER_BATCH", 10, 1, 50)

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

        for platform, url in cls.PLATFORM_TEST_URLS.items():
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise RuntimeError(f"PLATFORM_TEST_URLS[{platform}] must be a valid http(s) URL.")


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

class PlatformState:
    """Per-platform connectivity health state (requirement #1)."""
    WORKING = "WORKING"
    QUARANTINED = "QUARANTINED"
    DISABLED = "DISABLED"          # retained (soft), excluded from delivery/scheduling


class LifecycleState:
    """Coarse record lifecycle, independent of per-platform health."""
    NORMALIZED = "NORMALIZED"      # parsed + deduplicated, not yet queued
    QUEUED = "QUEUED"
    TESTING = "TESTING"
    RETIRED = "RETIRED"            # orphan cleanup only; never applied to ever_working proxies


class FailureCategory:
    """Neutral connectivity-failure classification. These describe what
    happened to the NETWORK ROUND TRIP - never whether a security/bot
    check was defeated. A security/auth response from the platform is not
    a failure category here; it is recorded as a status code only."""
    TCP_TIMEOUT = "TCP_TIMEOUT"
    TCP_REFUSED = "TCP_REFUSED"
    DNS_FAILURE = "DNS_FAILURE"
    TLS_ERROR = "TLS_ERROR"
    PROXY_PROTOCOL_FAILURE = "PROXY_PROTOCOL_FAILURE"
    PROXY_AUTH_FAILURE = "PROXY_AUTH_FAILURE"
    AUTH_MISSING = "AUTH_MISSING"
    SERVER_ERROR = "SERVER_ERROR"          # 5xx from the platform's own edge - not the proxy's fault
    ENVIRONMENT_ERROR = "ENVIRONMENT_ERROR"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"

    # Categories that reflect a problem with the target/environment, not
    # the proxy route itself, and should not count against the proxy.
    NON_ROUTE_SPECIFIC = frozenset({SERVER_ERROR, ENVIRONMENT_ERROR})


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

    # Requirement #6: no-password-given marking. The source string implies
    # auth (a "user:pass@" shape was present) but the password segment is
    # empty. Still parsed and still tested normally - flagged for
    # visibility only, never hard-excluded.
    user = match.group("user")
    password = match.group("password")
    requires_auth_missing = bool(user) and not password

    return ProxyEntry(
        scheme=scheme,
        host=host,
        port=port,
        username=user,
        password=password,
        requires_auth_missing=requires_auth_missing,
    )


# --- format-specific parsers -------------------------------------------

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


def parse_source_payload(text: str, content_type: str, url: str = "") -> list[ParsedCandidate]:
    fmt = detect_format(content_type, url)
    if fmt == "json":
        parsed = parse_json_payload(text)
        return parsed if parsed else parse_txt_payload(text)
    if fmt == "csv":
        parsed = parse_csv_payload(text)
        return parsed if parsed else parse_txt_payload(text)
    return parse_txt_payload(text)


def parse_any_payload(text: str) -> list[ParsedCandidate]:
    """Format-sniffing entry point for manual paste/file ingestion, where
    there is no Content-Type header to rely on. Tries JSON, then CSV
    (only if it looks tabular), then falls back to plain TXT lines."""
    stripped = text.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        parsed = parse_json_payload(text)
        if parsed:
            return parsed
    first_line = stripped.splitlines()[0] if stripped else ""
    if "," in first_line and any(h in first_line.lower() for h in ("ip", "host", "port", "proxy")):
        parsed = parse_csv_payload(text)
        if parsed:
            return parsed
    return parse_txt_payload(text)


# ============================================================================
# QUALITY SCORING (legitimate connectivity metrics only)
# ============================================================================

class QualityScorer:
    """Composite 0-100 score from purely network-level signals: reliability
    (rolling success rate), latency, and recent stability. Nothing here is
    a function of whether a security/bot-check response was avoided."""

    @staticmethod
    def update_success_rate(previous: Optional[float], success: bool, alpha: float = 0.2) -> float:
        sample = 1.0 if success else 0.0
        if previous is None:
            return sample
        return previous * (1 - alpha) + sample * alpha

    @staticmethod
    def update_latency_ewma(previous: Optional[float], latency_ms: Optional[float], alpha: float = 0.3) -> Optional[float]:
        if latency_ms is None:
            return previous
        if previous is None:
            return latency_ms
        return previous * (1 - alpha) + latency_ms * alpha

    @staticmethod
    def compute(success_rate_ewma: Optional[float], latency_ewma_ms: Optional[float],
                consecutive_fail_windows: int) -> float:
        rate = success_rate_ewma if success_rate_ewma is not None else 0.0
        reliability_component = rate * 70.0  # up to 70 points for reliability

        if latency_ewma_ms is None:
            latency_component = 0.0
        else:
            # 0ms -> 30 points, 3000ms+ -> 0 points, linear between.
            latency_component = max(0.0, 30.0 * (1 - min(latency_ewma_ms, 3000.0) / 3000.0))

        stability_penalty = min(15.0, consecutive_fail_windows * 3.0)

        score = reliability_component + latency_component - stability_penalty
        return round(max(0.0, min(100.0, score)), 2)


# ============================================================================
# DATABASE
# ============================================================================

class Database:
    """Manages the shared auxiliary collections plus one Mongo collection
    per platform (requirement #2), all sharing an identical schema so a
    Main Bot's query pattern transfers directly across platforms."""

    def __init__(self) -> None:
        self.client: Optional[AsyncMongoClient] = None
        self.db = None
        self.platform_collections: dict[str, Any] = {}
        self.sources = None
        self.tasks = None
        self.snapshots = None
        self.events = None
        self.daily = None
        self.worker_config = None

    def collection(self, platform: str):
        return self.platform_collections[platform]

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

        for platform in PLATFORMS:
            self.platform_collections[platform] = self.db[Config.PLATFORM_COLLECTIONS[platform]]

        self.sources = self.db["proxy_sources"]
        self.tasks = self.db["proxy_tasks"]
        self.snapshots = self.db["proxy_source_snapshots"]
        self.events = self.db["proxy_events"]
        self.daily = self.db["proxy_daily_summary"]
        self.worker_config = self.db["worker_config"]

        await self.ensure_indexes()
        logger.info("[DB] MongoDB connected (idempotent init, no destructive operations)")

    async def ensure_indexes(self) -> None:
        for platform in PLATFORMS:
            coll = self.platform_collections[platform]
            try:
                await coll.create_index([("proxy_id", ASCENDING)], unique=True, sparse=True)
            except OperationFailure as e:
                if e.code == 86:
                    logger.warning("[DB] Index conflict for proxy_id on %s. Recreating...", platform)
                    await coll.drop_index("proxy_id_1")
                    await coll.create_index([("proxy_id", ASCENDING)], unique=True, sparse=True)
                else:
                    raise

            await coll.create_index(
                [
                    ("enabled", ASCENDING),
                    ("state", ASCENDING),
                    ("next_check_at", ASCENDING),
                    ("quality_score", DESCENDING),
                ]
            )
            await coll.create_index([("ever_working", ASCENDING)])
            await coll.create_index([("verified_country", ASCENDING)])
            await coll.create_index([("asn", ASCENDING)])
            await coll.create_index([("scheme", ASCENDING)])
            await coll.create_index([("last_checked_at", ASCENDING)])
            await coll.create_index([("next_check_at", ASCENDING)])
            await coll.create_index([("lease_until", ASCENDING)])
            await coll.create_index([("source_ids", ASCENDING)])

        await self.sources.create_index([("source_id", ASCENDING)], unique=True)
        await self.tasks.create_index([("task_id", ASCENDING)], unique=True)
        await self.tasks.create_index([("status", ASCENDING), ("created_at", DESCENDING)])
        await self.snapshots.create_index([("source_id", ASCENDING), ("fetched_at", DESCENDING)])
        await self.events.create_index([("proxy_id", ASCENDING), ("platform", ASCENDING), ("created_at", DESCENDING)])
        await self.daily.create_index([("date", ASCENDING)], unique=True)

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
            await self.client.close()
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

    async def platform_enabled(self, platform: str) -> bool:
        return bool(await self.get_config(f"platform_enabled:{platform}", Config.PLATFORM_ENABLED_DEFAULT[platform]))

    async def set_platform_enabled(self, platform: str, enabled: bool) -> None:
        await self.set_config(f"platform_enabled:{platform}", enabled)

    async def initialize_defaults(self) -> None:
        for src in Config.DEFAULT_SOURCES:
            await self.upsert_source({**src}, only_if_missing=True)
        defaults = {"dry_run": Config.DRY_RUN}
        for key, value in defaults.items():
            if await self.get_config(key) is None:
                await self.set_config(key, value)
        for platform in PLATFORMS:
            if await self.get_config(f"platform_enabled:{platform}") is None:
                await self.set_config(f"platform_enabled:{platform}", Config.PLATFORM_ENABLED_DEFAULT[platform])

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
            "discovered": source_copy.get("discovered", False),
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
        await self.snapshots.insert_one(doc)

    # --- tasks / events ------------------------------------------------------

    async def create_task(self, task_type: str, source_id: Optional[str] = None) -> str:
        task_id = hashlib.sha1(f"{task_type}:{source_id}:{time.time_ns()}".encode()).hexdigest()[:16]
        await self.tasks.insert_one(
            {
                "task_id": task_id, "task_type": task_type, "source_id": source_id,
                "status": "PENDING", "created_at": now_utc(), "started_at": None, "finished_at": None,
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

    async def create_event(self, platform: str, proxy_id: str, event_type: str, **data: Any) -> None:
        await self.events.insert_one(
            {"platform": platform, "proxy_id": proxy_id, "event_type": event_type,
             "created_at": now_utc(), **data}
        )

    # --- proxies (per platform) ----------------------------------------------

    async def get_proxy(self, platform: str, proxy_id: str) -> Optional[dict[str, Any]]:
        return await self.collection(platform).find_one({"proxy_id": proxy_id})

    async def upsert_proxy(self, platform: str, entry: ProxyEntry, country: Optional[str] = None) -> tuple[bool, dict[str, Any]]:
        """Insert-or-update a NORMALIZED proxy record for one platform's
        collection. Never overwrites existing validation/working history -
        only source-linkage metadata is refreshed on an existing doc."""
        now = now_utc()
        coll = self.collection(platform)
        existing = await coll.find_one({"proxy_id": entry.proxy_id})

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
            if entry.requires_auth_missing and not existing.get("requires_auth_missing"):
                update["requires_auth_missing"] = True
            await coll.update_one({"proxy_id": entry.proxy_id}, {"$set": update})
            return False, {**existing, **update}

        doc = {
            "proxy_id": entry.proxy_id,
            "platform": platform,
            "scheme": entry.scheme,
            "host": entry.host,
            "port": entry.port,
            "username": entry.username,
            "password": entry.password,
            "requires_auth_missing": entry.requires_auth_missing,
            "source_ids": [entry.source_id] if entry.source_id else [],
            "source_country": country or entry.source_country,
            "source_present": True,
            "state": PlatformState.QUARANTINED,   # neutral starting point until first test
            "lifecycle": LifecycleState.NORMALIZED,
            "enabled": True,
            "retired": False,
            "ever_working": False,
            "consecutive_fail_windows": 0,
            "quarantined_since": None,
            "success_rate_ewma": None,
            "latency_ewma_ms": None,
            "quality_score": 0.0,
            "status_code_counts": {},
            "verified_country": None,
            "asn": None,
            "isp": None,
            "first_seen_at": now,
            "last_seen_at": now,
            "last_checked_at": None,
            "next_check_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "last_error_category": None,
            "last_error": None,
            "last_status_code": None,
            "last_latency_ms": None,
            "tls_ok": None,
            "lease_until": None,
            "last_notified_state": None,
        }
        await coll.insert_one(doc)
        return True, doc

    async def claim_proxy(self, platform: str, proxy_id: str, lease_seconds: int = 120) -> Optional[dict[str, Any]]:
        now = now_utc()
        return await self.collection(platform).find_one_and_update(
            {"proxy_id": proxy_id, "$or": [{"lease_until": None}, {"lease_until": {"$lte": now}}]},
            {"$set": {"lease_until": now + timedelta(seconds=lease_seconds), "lifecycle": LifecycleState.TESTING}},
            return_document=True,
        )

    async def release_lease(self, platform: str, proxy_id: str) -> None:
        await self.collection(platform).update_one({"proxy_id": proxy_id}, {"$set": {"lease_until": None}})

    async def release_expired_leases(self, platform: str) -> int:
        now = now_utc()
        result = await self.collection(platform).update_many(
            {"lease_until": {"$ne": None, "$lte": now}}, {"$set": {"lease_until": None}}
        )
        return result.modified_count

    async def record_test_result(self, platform: str, proxy_id: str, result: dict[str, Any]) -> dict[str, Any]:
        """Applies the staged state machine (requirement #1):

        WORKING success        -> state=WORKING, next_check in 1h
        WORKING -> fail         -> state=QUARANTINED, next_check in 5h
        QUARANTINED -> fail     -> +1 window; DISABLED at 48h elapsed, else next_check in 5h
        QUARANTINED -> success  -> state=WORKING (RECOVERED), next_check in 1h

        `success` here means the network round trip completed (reachable);
        it is entirely independent of the HTTP status code returned.
        """
        now = now_utc()
        coll = self.collection(platform)
        doc = await coll.find_one({"proxy_id": proxy_id})
        if not doc:
            return {}

        reachable = bool(result.get("reachable"))
        status_code = result.get("status_code")
        error_category = result.get("error_category")
        non_route_specific = error_category in FailureCategory.NON_ROUTE_SPECIFIC

        success_rate = QualityScorer.update_success_rate(doc.get("success_rate_ewma"), reachable)
        latency_ewma = QualityScorer.update_latency_ewma(doc.get("latency_ewma_ms"), result.get("latency_ms"))

        status_counts = dict(doc.get("status_code_counts") or {})
        if status_code is not None:
            key = str(status_code)
            status_counts[key] = safe_int(status_counts.get(key)) + 1

        update: dict[str, Any] = {
            "lease_until": None,
            "lifecycle": LifecycleState.NORMALIZED,
            "last_checked_at": now,
            "last_error_category": error_category,
            "last_error": short_error(result.get("error")) if result.get("error") else None,
            "last_status_code": status_code,
            "last_latency_ms": result.get("latency_ms"),
            "tls_ok": result.get("tls_ok"),
            "success_rate_ewma": success_rate,
            "latency_ewma_ms": latency_ewma,
            "status_code_counts": status_counts,
        }

        if result.get("country_code"):
            update["verified_country"] = result["country_code"]
            update["country_name"] = result.get("country_name")
        if result.get("asn"):
            update["asn"] = result.get("asn")
        if result.get("isp"):
            update["isp"] = result.get("isp")

        was_state = doc.get("state")
        quarantined_since = parse_dt(doc.get("quarantined_since"))

        if reachable:
            fail_windows = 0
            update.update(
                {
                    "state": PlatformState.WORKING,
                    "ever_working": True,
                    "enabled": True,
                    "consecutive_fail_windows": fail_windows,
                    "quarantined_since": None,
                    "last_success_at": now,
                    "next_check_at": now + timedelta(seconds=Config.WORKING_RECHECK_SECONDS),
                }
            )
        elif non_route_specific:
            # Environment/target-side issue - don't penalize the proxy,
            # just recheck soon without touching the state machine.
            update["next_check_at"] = now + timedelta(minutes=10)
            update["state"] = was_state or PlatformState.QUARANTINED
        else:
            update["last_failure_at"] = now
            if was_state == PlatformState.WORKING:
                update.update(
                    {
                        "state": PlatformState.QUARANTINED,
                        "quarantined_since": now,
                        "consecutive_fail_windows": 1,
                        "next_check_at": now + timedelta(seconds=Config.QUARANTINE_RECHECK_SECONDS),
                    }
                )
            else:
                fail_windows = safe_int(doc.get("consecutive_fail_windows")) + 1
                elapsed = (now - quarantined_since).total_seconds() if quarantined_since else 0
                if elapsed >= Config.QUARANTINE_DISABLE_AFTER_SECONDS:
                    update.update(
                        {
                            "state": PlatformState.DISABLED,
                            "consecutive_fail_windows": fail_windows,
                            "next_check_at": None,
                        }
                    )
                else:
                    update.update(
                        {
                            "state": PlatformState.QUARANTINED,
                            "quarantined_since": quarantined_since or now,
                            "consecutive_fail_windows": fail_windows,
                            "next_check_at": now + timedelta(seconds=Config.QUARANTINE_RECHECK_SECONDS),
                        }
                    )

        update["quality_score"] = QualityScorer.compute(
            update.get("success_rate_ewma", success_rate),
            update.get("latency_ewma_ms", latency_ewma),
            update.get("consecutive_fail_windows", doc.get("consecutive_fail_windows", 0)),
        )

        await coll.update_one({"proxy_id": proxy_id}, {"$set": update})
        merged = {**doc, **update}
        await self.create_event(platform, proxy_id, "TEST_RESULT", state=update.get("state"),
                                 reachable=reachable, status_code=status_code, error_category=error_category)
        return merged

    async def mark_missing_from_sources(self, platform: str, proxy_ids: list[str]) -> int:
        if not proxy_ids:
            return 0
        result = await self.collection(platform).update_many(
            {"proxy_id": {"$in": proxy_ids}},
            {"$set": {"source_present": False, "source_missing_since": now_utc()}},
        )
        return result.modified_count

    async def retire_orphans(self, platform: str, older_than_seconds: int) -> int:
        cutoff = now_utc() - timedelta(seconds=older_than_seconds)
        result = await self.collection(platform).update_many(
            {
                "ever_working": False,
                "source_present": False,
                "source_missing_since": {"$lte": cutoff},
                "retired": {"$ne": True},
            },
            {"$set": {"retired": True, "lifecycle": LifecycleState.RETIRED, "enabled": False}},
        )
        return result.modified_count

    async def cleanup_history(self, days: int = 30) -> dict[str, int]:
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

    async def get_stats(self, platform: str) -> dict[str, Any]:
        coll = self.collection(platform)
        total = await coll.count_documents({})
        working = await coll.count_documents({"state": PlatformState.WORKING, "enabled": True})
        quarantined = await coll.count_documents({"state": PlatformState.QUARANTINED})
        disabled = await coll.count_documents({"state": PlatformState.DISABLED})
        retired = await coll.count_documents({"retired": True})
        ever_working = await coll.count_documents({"ever_working": True})
        return {
            "total": total, "working": working, "quarantined": quarantined,
            "disabled": disabled, "retired": retired, "ever_working": ever_working,
        }


# ============================================================================
# SOURCE MANAGER - generic GitHub + multi-format resolution
# ============================================================================

class ProxySourceManager:
    """Fetches configured sources and turns them into normalized,
    deduplicated proxy records, one set per enabled platform collection.
    Parsing is fully separate from validation (ConnectivityValidator)."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        headers = {"User-Agent": Config.USER_AGENT, "Accept": "*/*"}
        self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    @staticmethod
    def _is_github_repo_url(url: str) -> bool:
        return urlparse(url).netloc.lower() == "github.com"

    async def _list_github_directory(self, owner: str, repo: str, branch: str, path: str) -> list[dict[str, Any]]:
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
        for entry in entries:
            if entry.get("type") == "file":
                return entry
        return None

    async def resolve_source_url(self, source: dict[str, Any]) -> tuple[str, str]:
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

        if len(parts) >= 5 and parts[2] == "blob":
            owner, repo, _, branch = parts[:4]
            file_path = "/".join(parts[4:])
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}"
            fmt = detect_format("", raw_url)
            await self.db.record_source_state(source["source_id"], resolved_url=raw_url, resolved_format=fmt)
            return raw_url, fmt

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

        return url, detect_format("", url)

    async def list_github_sibling_files(self, url: str) -> list[str]:
        """Requirement #8: for a GitHub source, list sibling files in the
        same directory using the same generic (non-provider-specific)
        listing already used for tree resolution, so discovery can find
        adjacent proxy list files the operator wasn't explicitly pointed
        at, without any provider-specific scraping hacks."""
        if not self._is_github_repo_url(url):
            return []
        parsed = urlparse(url)
        parts = [unquote(x) for x in parsed.path.split("/") if x]
        if len(parts) < 5 or parts[2] not in ("blob", "tree"):
            return []
        owner, repo, _, branch = parts[:4]
        path = "/".join(parts[4:-1]) if parts[2] == "blob" else "/".join(parts[4:])
        try:
            entries = await self._list_github_directory(owner, repo, branch, path)
        except Exception:
            return []
        out = []
        for entry in entries:
            if entry.get("type") == "file" and entry.get("download_url"):
                name = str(entry.get("name", "")).lower()
                if name.endswith((".txt", ".json", ".csv")):
                    out.append(entry["download_url"])
        return out

    async def fetch(self, fetch_url: str) -> tuple[str, str, int]:
        if not self.session:
            raise RuntimeError("Source manager is not started.")

        max_bytes = Config.MAX_SOURCE_BYTES
        last_exc: Optional[Exception] = None
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=45)
                async with self.session.get(
                    fetch_url, timeout=timeout, allow_redirects=True,
                    headers={"User-Agent": Config.USER_AGENT,
                             "Accept": "application/json,text/plain,text/*,*/*;q=0.8"},
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
        return {"url": url, "fetch_url": fetch_url, "format": fmt, "bytes": byte_count,
                "estimated_entries": len(candidates)}

    async def import_source(self, source: dict[str, Any], task_id: Optional[str] = None) -> dict[str, Any]:
        """Imports a source into EVERY enabled platform's collection, since
        the same proxy list is a candidate for all platform connectivity
        tests independently."""
        source_id = source["source_id"]
        fetch_url, fmt = await self.resolve_source_url(source)
        text, content_type, byte_count = await self.fetch(fetch_url)
        content_hash = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()

        if source.get("last_content_hash") == content_hash:
            await self.db.record_source_state(source_id, content_hash=content_hash, item_count=0, success=True)
            if task_id:
                await self.db.finish_task(task_id, "COMPLETED", unchanged=True)
            return {"source_id": source_id, "unchanged": True, "fetched": 0, "valid": 0,
                     "new": {}, "duplicates": 0, "invalid": 0, "bytes": byte_count}

        candidates = parse_source_payload(text, content_type, fetch_url)
        if len(candidates) > Config.MAX_DISCOVERED_PER_SOURCE:
            candidates = candidates[: Config.MAX_DISCOVERED_PER_SOURCE]

        seen: set[str] = set()
        invalid = 0
        duplicates = 0
        new_per_platform: dict[str, int] = {p: 0 for p in PLATFORMS}
        known_ids: list[str] = []
        source_country_default = source.get("country")
        target_platforms = [p for p in PLATFORMS if await self.db.platform_enabled(p)]

        for candidate in candidates:
            entry = parse_proxy_string(candidate.raw, default_scheme=candidate.scheme_hint or "http")
            if not entry:
                invalid += 1
                continue
            country = candidate.country or source_country_default
            entry = ProxyEntry(
                scheme=entry.scheme, host=entry.host, port=entry.port,
                username=entry.username, password=entry.password,
                source_id=source_id, source_country=country,
                requires_auth_missing=entry.requires_auth_missing,
            )
            if entry.proxy_id in seen:
                duplicates += 1
                continue
            seen.add(entry.proxy_id)
            known_ids.append(entry.proxy_id)

            for platform in target_platforms:
                is_new, _ = await self.db.upsert_proxy(platform, entry, country=country)
                if is_new:
                    new_per_platform[platform] += 1

        previous_ids = set(source.get("known_proxy_ids") or [])
        missing_ids = list(previous_ids - set(known_ids))
        if missing_ids:
            for platform in target_platforms:
                await self.db.mark_missing_from_sources(platform, missing_ids)

        await self.db.save_snapshot(
            {
                "source_id": source_id, "fetched_at": now_utc(), "content_hash": content_hash,
                "count": len(known_ids), "added_count": new_per_platform, "duplicate_count": duplicates,
                "invalid_count": invalid, "removed_count": len(missing_ids),
            }
        )
        await self.db.record_source_state(
            source_id, content_hash=content_hash, item_count=len(known_ids),
            known_proxy_ids=known_ids, success=True,
        )

        if task_id:
            await self.db.update_task(
                task_id, total_items=len(candidates), new_items=new_per_platform,
                duplicates=duplicates, invalid=invalid, removed=len(missing_ids),
            )

        return {
            "source_id": source_id, "unchanged": False, "fetched": len(candidates),
            "valid": len(known_ids), "new": new_per_platform, "duplicates": duplicates,
            "invalid": invalid, "removed": len(missing_ids), "bytes": byte_count, "format": fmt,
        }


# ============================================================================
# CONNECTIVITY VALIDATOR - pure network probe, no content/auth interaction
# ============================================================================

class ConnectivityValidator:
    """Tests a proxy's raw connectivity to a platform's public homepage.

    This performs no login, no content extraction, no retries-to-evade,
    and no special headers/behavior designed to get past a security
    control. It is the same shape of probe for all three platforms:

        TCP connect -> (via proxy) TLS handshake + HTTP GET of the
        homepage -> record whether a response came back, its status code,
        and the round-trip latency.

    A security/bot-check response (401/403/429/etc.) is reachable=True:
    the proxy successfully relayed a full request/response cycle to the
    platform's infrastructure, which is exactly the connectivity signal
    being measured. It is recorded in status_code_counts for informational
    "HTTP status behaviour" tracking only - it never triggers a retry, a
    header change, or any other evasive behavior, and it is never treated
    as a route-specific failure.
    """

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @staticmethod
    async def tcp_connect_check(host: str, port: int) -> bool:
        try:
            fut = asyncio.open_connection(host, port)
            reader, writer = await asyncio.wait_for(fut, timeout=Config.CONNECT_CHECK_TIMEOUT)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            return False

    async def resolve_geo_asn(self, ip: str) -> dict[str, Any]:
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
                    connection = data.get("connection") or {}
                    return {
                        "country_code": (data.get("country_code") or "").upper() or None,
                        "country_name": data.get("country"),
                        "asn": connection.get("asn"),
                        "isp": connection.get("isp") or connection.get("org"),
                    }
        except Exception:
            return {}

    @staticmethod
    def _classify_connection_error(exc: Exception) -> str:
        text = short_error(exc).lower()
        if isinstance(exc, asyncio.TimeoutError) or "timeout" in text:
            return FailureCategory.TCP_TIMEOUT
        if any(k in text for k in ("407", "unauthorized", "proxy auth")):
            return FailureCategory.PROXY_AUTH_FAILURE
        if "name or service not known" in text or "getaddrinfo" in text:
            return FailureCategory.DNS_FAILURE
        if "ssl" in text or "tls" in text or "certificate" in text:
            return FailureCategory.TLS_ERROR
        if "refused" in text:
            return FailureCategory.TCP_REFUSED
        return FailureCategory.PROXY_PROTOCOL_FAILURE

    async def _probe(self, entry: ProxyEntry, target_url: str, timeout_seconds: int) -> dict[str, Any]:
        proxy_url = entry.canonical
        started = time.monotonic()

        if entry.scheme.startswith("socks") and ProxyConnector is None:
            return {
                "reachable": False, "error_category": FailureCategory.ENVIRONMENT_ERROR,
                "error": "aiohttp-socks is required for SOCKS proxy validation.",
            }

        try:
            timeout = aiohttp.ClientTimeout(total=timeout_seconds)
            headers = {"User-Agent": Config.USER_AGENT, "Accept": "*/*"}

            if entry.scheme.startswith("socks"):
                connector = ProxyConnector.from_url(proxy_url)
                session = aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers)
                try:
                    async with session.get(target_url, allow_redirects=True) as response:
                        status_code = response.status
                        tls_ok = target_url.lower().startswith("https")
                        await response.read()
                finally:
                    await session.close()
            else:
                async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                    async with session.get(target_url, proxy=proxy_url, allow_redirects=True) as response:
                        status_code = response.status
                        tls_ok = target_url.lower().startswith("https")
                        await response.read()

            latency_ms = (time.monotonic() - started) * 1000.0
            if status_code >= 500:
                # Platform-side issue, not the proxy's fault.
                return {
                    "reachable": False, "status_code": status_code, "tls_ok": tls_ok,
                    "latency_ms": latency_ms, "error_category": FailureCategory.SERVER_ERROR,
                    "error": f"Target returned HTTP {status_code}.",
                }
            # Any response below 500 (including a security/bot-check
            # response) proves the round trip completed successfully.
            return {
                "reachable": True, "status_code": status_code, "tls_ok": tls_ok,
                "latency_ms": latency_ms, "error_category": FailureCategory.SUCCESS,
            }
        except asyncio.TimeoutError:
            return {
                "reachable": False, "error_category": FailureCategory.TCP_TIMEOUT,
                "error": "Connection timed out.", "latency_ms": (time.monotonic() - started) * 1000.0,
            }
        except Exception as exc:
            category = self._classify_connection_error(exc)
            return {
                "reachable": False, "error_category": category, "error": short_error(exc),
                "latency_ms": (time.monotonic() - started) * 1000.0,
            }

    async def validate(self, proxy: dict[str, Any], platform: str) -> dict[str, Any]:
        entry = ProxyEntry(
            scheme=proxy.get("scheme", "http"), host=proxy["host"], port=safe_int(proxy["port"]),
            username=proxy.get("username"), password=proxy.get("password"),
        )
        target_url = Config.PLATFORM_TEST_URLS[platform]
        timeout_seconds = Config.PLATFORM_TIMEOUT_SECONDS[platform]

        # Stage 1: cheapest possible check - pure TCP, no bandwidth at all.
        reachable_tcp = await self.tcp_connect_check(entry.host, entry.port)
        if not reachable_tcp:
            return {
                "reachable": False, "error_category": FailureCategory.TCP_TIMEOUT,
                "error": "TCP connect to proxy failed.",
            }

        # Stage 2: the actual reachability probe against the platform homepage.
        result = await self._probe(entry, target_url, timeout_seconds)

        # Stage 3: geo/ASN lookup, reusing source-provided country if we
        # already have one (bandwidth optimization), otherwise resolving
        # from the platform response is not possible (we don't get the
        # exit IP from a same-origin GET), so we do a lightweight,
        # separate exit-IP check only when geo data is actually needed.
        known_country = proxy.get("source_country") or proxy.get("verified_country")
        if not known_country and result.get("reachable"):
            exit_ip = await self._resolve_exit_ip(entry, timeout_seconds)
            if exit_ip:
                geo = await self.resolve_geo_asn(exit_ip)
                result.update(geo)
        elif known_country:
            result.setdefault("country_code", known_country)

        return result

    async def _resolve_exit_ip(self, entry: ProxyEntry, timeout_seconds: int) -> Optional[str]:
        proxy_url = entry.canonical
        try:
            timeout = aiohttp.ClientTimeout(total=min(timeout_seconds, 10))
            headers = {"User-Agent": Config.USER_AGENT}
            if entry.scheme.startswith("socks"):
                if ProxyConnector is None:
                    return None
                connector = ProxyConnector.from_url(proxy_url)
                session = aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers)
                try:
                    async with session.get("https://api.ipify.org?format=json") as response:
                        data = await response.json(content_type=None)
                        return str(data.get("ip", "")).strip() or None
                finally:
                    await session.close()
            else:
                async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                    async with session.get("https://api.ipify.org?format=json", proxy=proxy_url) as response:
                        data = await response.json(content_type=None)
                        return str(data.get("ip", "")).strip() or None
        except Exception:
            return None


# ============================================================================
# WORKER STATE / SCHEDULER
# ============================================================================

class WorkerState:
    def __init__(self) -> None:
        self.started_at = now_utc()
        self.stop_event = asyncio.Event()
        self.queues: dict[str, asyncio.PriorityQueue] = {
            p: asyncio.PriorityQueue(maxsize=Config.MAX_PENDING_TESTS) for p in PLATFORMS
        }
        self.pending_ids: dict[str, set[str]] = {p: set() for p in PLATFORMS}
        self.pending_lock = asyncio.Lock()
        self.tasks: set[asyncio.Task] = set()
        self.active_tests = 0
        self.completed_tests = 0
        self.last_source_refresh: Optional[datetime] = None
        self.last_working_pool_count: dict[str, int] = {p: 0 for p in PLATFORMS}
        self.critical_since: dict[str, Optional[datetime]] = {p: None for p in PLATFORMS}
        self.last_error: Optional[str] = None
        self.scheduler_running = False
        self.report_last_sent: Optional[datetime] = None
        # Requirement #4: manual priority checks pause normal dequeuing.
        self.dispatch_paused = False

    def uptime_seconds(self) -> int:
        return int((now_utc() - self.started_at).total_seconds())

    def pool_health(self, platform: str) -> str:
        count = self.last_working_pool_count.get(platform, 0)
        if count <= 0:
            return "CRITICAL"
        if count < 5:
            return "LOW"
        return "OK"


class WorkerScheduler:
    """One coarse-grained scheduler loop per Code A's model: everything
    expensive (source fetch, revalidation, quarantine recheck, history
    cleanup) is driven from one periodic tick. A lightweight dispatcher per
    platform drains its bounded test queue with bounded concurrency."""

    def __init__(self, db: Database, sources: ProxySourceManager, validator: ConnectivityValidator,
                 reports: "ReportManager", state: WorkerState, notify) -> None:
        self.db = db
        self.sources = sources
        self.validator = validator
        self.reports = reports
        self.state = state
        self.notify = notify
        self.dispatcher_tasks: dict[str, asyncio.Task] = {}
        self.periodic_task: Optional[asyncio.Task] = None
        self.discovery_task: Optional[asyncio.Task] = None
        self.semaphore = asyncio.Semaphore(Config.TEST_CONCURRENCY)
        self.pool_refresh_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self.state.scheduler_running = True
        for platform in PLATFORMS:
            self.dispatcher_tasks[platform] = asyncio.create_task(
                self.dispatch_loop(platform), name=f"dispatcher-{platform}"
            )
        self.periodic_task = asyncio.create_task(self.periodic_loop(), name="periodic")
        if Config.DISCOVERY_ENABLED:
            self.discovery_task = asyncio.create_task(self.discovery_loop(), name="discovery")

    async def stop(self) -> None:
        self.state.stop_event.set()
        tasks_to_wait = []
        for task in (*self.dispatcher_tasks.values(), self.periodic_task, self.discovery_task):
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

    # --- bounded per-platform dispatch queues ---------------------------------

    async def enqueue_proxy(self, platform: str, proxy_id: str, priority: int = 100, reason: str = "new") -> bool:
        async with self.state.pending_lock:
            pending = self.state.pending_ids[platform]
            queue = self.state.queues[platform]
            if proxy_id in pending:
                return False
            if queue.full():
                logger.warning("[QUEUE] %s full; skipping proxy=%s reason=%s", platform, proxy_id[:12], reason)
                return False
            pending.add(proxy_id)
            await queue.put((priority, time.monotonic(), proxy_id, reason))
            return True

    async def dispatch_loop(self, platform: str) -> None:
        logger.info("[SCHEDULER] dispatcher started for %s", platform)
        queue = self.state.queues[platform]
        while not self.state.stop_event.is_set():
            if self.state.dispatch_paused:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            _, _, proxy_id, reason = item
            async with self.state.pending_lock:
                self.state.pending_ids[platform].discard(proxy_id)
            task = asyncio.create_task(
                self.run_proxy_test(platform, proxy_id, reason), name=f"test-{platform}-{proxy_id[:8]}"
            )
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

    async def run_proxy_test(self, platform: str, proxy_id: str, reason: str) -> None:
        claimed = await self.db.claim_proxy(platform, proxy_id)
        if not claimed:
            return
        async with self.semaphore:
            self.state.active_tests += 1
            try:
                was_working = claimed.get("state") == PlatformState.WORKING
                result = await self.validator.validate(claimed, platform)
                updated = await self.db.record_test_result(platform, proxy_id, result)
                self.state.completed_tests += 1
                now_working = updated.get("state") == PlatformState.WORKING
                await self._notify_state_change(platform, updated, was_working, now_working)
            except Exception:
                logger.exception("[TEST] proxy test crashed platform=%s id=%s", platform, proxy_id[:12])
                await self.db.release_lease(platform, proxy_id)
            finally:
                self.state.active_tests -= 1

    async def _notify_state_change(self, platform: str, proxy: dict[str, Any], was_working: bool, now_working: bool) -> None:
        """Requirement #3: only WORKING (first-time) and RECOVERED events
        are posted, and each only once per transition."""
        proxy_id = proxy.get("proxy_id", "")
        state = proxy.get("state")
        last_notified = proxy.get("last_notified_state")
        was_ever_notified_working = last_notified in (PlatformState.WORKING,)

        should_notify = now_working and not was_working
        if not should_notify or last_notified == state:
            return

        await self.db.collection(platform).update_one({"proxy_id": proxy_id}, {"$set": {"last_notified_state": state}})

        proxy_str = mask_proxy_string(f"{proxy.get('scheme','http')}://{proxy.get('host','')}:{proxy.get('port','')}")
        recovered = last_notified is not None
        headline = "✅ RECOVERED" if recovered else "✅ WORKING"
        platform_label = platform.capitalize()

        status_line = "status: first-time verified"
        quarantined_since = parse_dt(proxy.get("quarantined_since"))
        if recovered and quarantined_since:
            downtime_h = round((now_utc() - quarantined_since).total_seconds() / 3600, 1)
            status_line = f"status: recovered after quarantine ({downtime_h}h downtime)"
        elif recovered:
            status_line = "status: recovered after quarantine"

        lines = [
            f"{headline} — {platform_label}",
            "```",
            f"proxy: {proxy_str}",
            f"country: {proxy.get('verified_country') or proxy.get('source_country') or 'unknown'}",
            f"source: {', '.join((proxy.get('source_ids') or ['unknown'])[:2])}",
            f"latency: {int(proxy['latency_ewma_ms']) if proxy.get('latency_ewma_ms') else '?'} ms",
            status_line,
            f"checked_at: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}",
            "```",
        ]
        await self.notify(platform, "\n".join(lines))

    # --- queue population ------------------------------------------------------

    async def enqueue_new_candidates(self, platform: str, source_id: Optional[str] = None) -> int:
        query: dict[str, Any] = {"retired": {"$ne": True}, "enabled": True, "lifecycle": LifecycleState.NORMALIZED,
                                  "lease_until": None}
        if source_id:
            query["source_ids"] = source_id
        added = 0
        cursor = self.db.collection(platform).find(query).sort("first_seen_at", ASCENDING).limit(
            Config.PLATFORM_MAX_TEST_PER_REFRESH
        )
        async for doc in cursor:
            if await self.enqueue_proxy(platform, doc["proxy_id"], priority=10, reason="new"):
                added += 1
        return added

    async def enqueue_due_rechecks(self, platform: str) -> int:
        """Requirement #7: skip re-test if not due yet (next_check_at in
        the future), even if the proxy reappears in a freshly-fetched
        source. Covers both WORKING (hourly) and QUARANTINED (5h) recheck
        cadences in one query since both use next_check_at."""
        now = now_utc()
        query = {
            "retired": {"$ne": True}, "enabled": True,
            "state": {"$in": [PlatformState.WORKING, PlatformState.QUARANTINED]},
            "next_check_at": {"$ne": None, "$lte": now},
        }
        count = 0
        cursor = self.db.collection(platform).find(query).sort("next_check_at", ASCENDING).limit(
            Config.PLATFORM_MAX_TEST_PER_REFRESH
        )
        async for doc in cursor:
            priority = 30 if doc.get("state") == PlatformState.WORKING else 40
            if await self.enqueue_proxy(platform, doc["proxy_id"], priority=priority, reason="due-recheck"):
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
                continue

            task_id = await self.db.create_task("SOURCE_REFRESH", source["source_id"])
            await self.db.start_task(task_id)
            try:
                result = await self.sources.import_source(source, task_id)
                aggregate["sources"] += 1
                aggregate["fetched"] += safe_int(result.get("fetched"))
                aggregate["valid"] += safe_int(result.get("valid"))
                aggregate["duplicates"] += safe_int(result.get("duplicates"))
                aggregate["invalid"] += safe_int(result.get("invalid"))

                if not result.get("unchanged"):
                    for platform in PLATFORMS:
                        if await self.db.platform_enabled(platform):
                            await self.enqueue_new_candidates(platform, source["source_id"])

                await self.db.finish_task(task_id, "COMPLETED", result_summary=result)
            except Exception as exc:
                await self.db.record_source_state(source["source_id"], success=False, error=short_error(exc))
                await self.db.finish_task(task_id, "FAILED", error=short_error(exc))
                logger.error("[SOURCE] %s failed: %s", source["source_id"], short_error(exc))

                latest = await self.db.get_source(source["source_id"])
                if latest and safe_int(latest.get("failure_count")) >= Config.SOURCE_FAILURE_ALERT_THRESHOLD:
                    await self.notify(
                        "youtube",
                        f"⚠️ Source failure threshold reached\nSource: {source['name']}\nError: {short_error(exc, 300)}",
                    )

        for platform in PLATFORMS:
            if await self.db.platform_enabled(platform):
                await self.enqueue_due_rechecks(platform)

        self.state.last_source_refresh = now_utc()
        return dict(aggregate)

    # --- multi-source auto-discovery (requirement #8) -------------------------

    async def discovery_loop(self) -> None:
        logger.info("[DISCOVERY] loop started (interval=%ss)", Config.DISCOVERY_INTERVAL_SECONDS)
        while not self.state.stop_event.is_set():
            try:
                await asyncio.sleep(Config.DISCOVERY_INTERVAL_SECONDS)
                await self.run_discovery_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[DISCOVERY] cycle failed")

    async def run_discovery_once(self) -> int:
        seeds = await self.db.get_sources(enabled_only=True)
        discovered_count = 0

        for seed in seeds:
            try:
                siblings = await self.sources.list_github_sibling_files(seed["url"])
            except Exception:
                continue
            for sibling_url in siblings:
                if sibling_url == seed.get("resolved_url") or sibling_url == seed.get("url"):
                    continue
                source_id = hashlib.sha1(sibling_url.encode("utf-8")).hexdigest()[:16]
                existing = await self.db.get_source(source_id)
                if existing:
                    continue
                await self.db.upsert_source(
                    {
                        "source_id": source_id, "name": f"auto:{urlparse(sibling_url).path.split('/')[-1]}",
                        "url": sibling_url, "source_type": "DISCOVERED", "enabled": True,
                        "discovered": True, "priority": 50, "fetch_interval": Config.SOURCE_REFRESH_SECONDS,
                    },
                    only_if_missing=True,
                )
                discovered_count += 1

        for aggregator_url in Config.DISCOVERY_SEED_AGGREGATORS:
            source_id = hashlib.sha1(aggregator_url.encode("utf-8")).hexdigest()[:16]
            existing = await self.db.get_source(source_id)
            if existing:
                continue
            await self.db.upsert_source(
                {
                    "source_id": source_id, "name": f"aggregator:{urlparse(aggregator_url).netloc}",
                    "url": aggregator_url, "source_type": "DISCOVERED", "enabled": True,
                    "discovered": True, "priority": 30, "fetch_interval": Config.SOURCE_REFRESH_SECONDS,
                },
                only_if_missing=True,
            )
            discovered_count += 1

        if discovered_count:
            logger.info("[DISCOVERY] added %s new source(s)", discovered_count)
        return discovered_count

    # --- periodic loop ---------------------------------------------------------

    async def periodic_loop(self) -> None:
        logger.info("[SCHEDULER] periodic loop started (interval=%ss)", Config.SOURCE_REFRESH_SECONDS)
        first_run = True
        while not self.state.stop_event.is_set():
            try:
                if first_run:
                    first_run = False
                else:
                    await asyncio.sleep(Config.SOURCE_REFRESH_SECONDS)

                try:
                    summary = await self.source_refresh_once()
                    logger.info(
                        "[SOURCE] refresh sources=%s fetched=%s valid=%s dup=%s invalid=%s",
                        summary.get("sources", 0), summary.get("fetched", 0), summary.get("valid", 0),
                        summary.get("duplicates", 0), summary.get("invalid", 0),
                    )
                except Exception:
                    logger.exception("[SCHEDULER] refresh failed")

                for platform in PLATFORMS:
                    await self.db.release_expired_leases(platform)
                    await self.db.retire_orphans(platform, Config.ORPHAN_RETIRE_AFTER_SECONDS)
                    await self.refresh_pool_health(platform)

                await self.db.cleanup_history(days=30)
                await self.maybe_daily_report()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = short_error(exc)
                logger.exception("[SCHEDULER] periodic loop error")
                await asyncio.sleep(5)

    async def refresh_pool_health(self, platform: str) -> str:
        count = await self.db.collection(platform).count_documents(
            {"enabled": True, "retired": {"$ne": True}, "state": PlatformState.WORKING}
        )
        old = self.state.last_working_pool_count.get(platform, 0)
        self.state.last_working_pool_count[platform] = count
        health = self.state.pool_health(platform)

        if health == "CRITICAL":
            if self.state.critical_since.get(platform) is None:
                self.state.critical_since[platform] = now_utc()
                await self.notify(platform, f"🔴 CRITICAL ({platform}): no verified working proxy is currently available.")
        else:
            self.state.critical_since[platform] = None

        if old > 0 and count == 0:
            await self.notify(platform, f"🔴 {platform.capitalize()} proxy pool dropped to zero WORKING routes.")
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

        for platform in PLATFORMS:
            summary = await self.reports.daily_summary(platform)
            await self.reports.persist_daily_summary(platform, summary)
            await self.notify(platform, self.reports.format_daily_summary(platform, summary))
        await self.db.set_config("last_daily_report_date", today)
        self.state.report_last_sent = now_utc()

    # --- manual single/bulk priority check (requirement #4/#5) ---------------

    async def priority_check_one(self, proxy_url: str) -> dict[str, Any]:
        """Tests one proxy immediately across every enabled platform,
        concurrently, jumping the normal queue via dispatch_paused."""
        entry = parse_proxy_string(proxy_url)
        if not entry:
            return {"ok": False, "error": f"Could not parse proxy string: {proxy_url!r}"}

        self.state.dispatch_paused = True
        try:
            results: dict[str, dict[str, Any]] = {}
            enabled_platforms = [p for p in PLATFORMS if await self.db.platform_enabled(p)]

            async def run_one(platform: str) -> None:
                is_new, doc = await self.db.upsert_proxy(platform, entry, country=entry.source_country)
                result = await self.validator.validate(doc, platform)
                updated = await self.db.record_test_result(platform, doc["proxy_id"], result)
                was_working = False  # manual check: treat any WORKING result as notify-worthy below
                now_working = updated.get("state") == PlatformState.WORKING
                if now_working:
                    await self._notify_state_change(platform, {**updated, "last_notified_state": None},
                                                      was_working, now_working)
                results[platform] = {**result, "state": updated.get("state")}

            await asyncio.gather(*(run_one(p) for p in enabled_platforms))
            return {"ok": True, "proxy": entry.canonical, "results": results}
        finally:
            self.state.dispatch_paused = False

    async def priority_check_batch(self, proxy_urls: list[str]) -> list[dict[str, Any]]:
        """Requirement #4: up to MAX_MANUAL_ITEMS_PER_BATCH proxies, run
        through the priority path SEQUENTIALLY (not concurrently with each
        other) to protect bandwidth, each with concurrent per-platform
        testing internally."""
        out = []
        for url in proxy_urls[: Config.MAX_MANUAL_ITEMS_PER_BATCH]:
            out.append(await self.priority_check_one(url))
        return out

    async def bulk_ingest_text(self, text: str) -> dict[str, Any]:
        """Requirement #5: paste a block of text through the same
        parse -> normalize -> dedupe pipeline as configured sources."""
        candidates = parse_any_payload(text)
        added_per_platform = {p: 0 for p in PLATFORMS}
        invalid = 0
        duplicates = 0
        seen: set[str] = set()
        target_platforms = [p for p in PLATFORMS if await self.db.platform_enabled(p)]

        for candidate in candidates:
            entry = parse_proxy_string(candidate.raw, default_scheme=candidate.scheme_hint or "http")
            if not entry:
                invalid += 1
                continue
            entry = ProxyEntry(
                scheme=entry.scheme, host=entry.host, port=entry.port,
                username=entry.username, password=entry.password,
                source_id="manual:paste", source_country=candidate.country,
                requires_auth_missing=entry.requires_auth_missing,
            )
            if entry.proxy_id in seen:
                duplicates += 1
                continue
            seen.add(entry.proxy_id)
            for platform in target_platforms:
                is_new, _ = await self.db.upsert_proxy(platform, entry, country=candidate.country)
                if is_new:
                    added_per_platform[platform] += 1

        for platform in target_platforms:
            await self.enqueue_new_candidates(platform, source_id="manual:paste")

        return {
            "parsed": len(candidates), "invalid": invalid, "duplicates": duplicates,
            "added": added_per_platform,
        }

    # --- pool refresh (dashboard action) --------------------------------------
    # §11 FIX: the original handler fired `asyncio.create_task(...)` without
    # storing the reference or attaching a done-callback, so an exception in
    # the background task was silently swallowed by the event loop instead
    # of reaching any error handling. Fixed by storing the task on the
    # scheduler and always attaching `_pool_refresh_task_done` as a
    # done-callback, which logs (and notifies on) any exception.

    async def trigger_pool_refresh(self, platform: str) -> None:
        working_proxies = await self.db.collection(platform).find(
            {"state": PlatformState.WORKING, "enabled": True}
        ).to_list(length=None)

        if not working_proxies:
            await self.notify(platform, f"♻️ Pool Refresh cancelled ({platform}): no WORKING proxies found.")
            return

        async with self.state.pending_lock:
            queue = self.state.queues[platform]
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
            self.state.pending_ids[platform].clear()

        await self.notify(
            platform,
            f"♻️ Pool Refresh started for {len(working_proxies)} {platform} proxies. "
            f"Other background tasks for this platform are paused.",
        )

        task = asyncio.create_task(self._run_pool_refresh_task(platform, working_proxies), name=f"pool-refresh-{platform}")
        self.pool_refresh_task = task
        task.add_done_callback(lambda t: self._pool_refresh_task_done(platform, t))

    def _pool_refresh_task_done(self, platform: str, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[POOL REFRESH] background task crashed for %s", platform)
            asyncio.create_task(
                self.notify(platform, f"🔴 Pool Refresh ({platform}) crashed: see server logs.")
            )

    async def _run_pool_refresh_task(self, platform: str, proxies: list) -> None:
        initial_count = len(proxies)
        final_working_count = 0

        async def check_proxy_with_retry(proxy_doc):
            nonlocal final_working_count
            proxy_id = proxy_doc["proxy_id"]
            async with self.semaphore:
                self.state.active_tests += 1
                try:
                    result = await self.validator.validate(proxy_doc, platform)
                    if not result.get("reachable"):
                        await asyncio.sleep(2)
                        result = await self.validator.validate(proxy_doc, platform)
                    updated = await self.db.record_test_result(platform, proxy_id, result)
                    if updated.get("state") == PlatformState.WORKING:
                        final_working_count += 1
                except Exception:
                    logger.exception("[POOL REFRESH] error testing proxy %s on %s", proxy_id[:8], platform)
                finally:
                    self.state.active_tests -= 1

        tasks = [asyncio.create_task(check_proxy_with_retry(p)) for p in proxies]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        report = (
            f"♻️ **Pool Refresh Report ({platform})**\n\n"
            f"Previous working pool: {initial_count}\n"
            f"Currently working: {final_working_count}\n"
            f"Moved to quarantine (failed double-check): {initial_count - final_working_count}\n\n"
            "Normal background tasks will now resume automatically."
        )
        await self.notify(platform, report)

    async def test_specific(self, platform: str, proxy_id: str) -> dict[str, Any]:
        proxy = await self.db.get_proxy(platform, proxy_id)
        if not proxy:
            return {"ok": False, "error": "Proxy not found."}
        result = await self.validator.validate(proxy, platform)
        updated = await self.db.record_test_result(platform, proxy_id, result)
        return {"ok": True, **updated}


# ============================================================================
# REPORTS
# ============================================================================

class ReportManager:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def system_stats(self, platform: str) -> dict[str, Any]:
        return await self.db.get_stats(platform)

    async def country_report(self, platform: str) -> list[dict[str, Any]]:
        pipeline = [
            {"$match": {"state": PlatformState.WORKING, "enabled": True}},
            {"$group": {"_id": "$verified_country", "working": {"$sum": 1}}},
            {"$sort": {"working": -1}},
            {"$limit": 20},
        ]
        return await self.db.collection(platform).aggregate(pipeline).to_list(length=20)

    async def export_working(self, platform: str, country: Optional[str] = None) -> bytes:
        query: dict[str, Any] = {"state": PlatformState.WORKING, "enabled": True}
        if country:
            query["verified_country"] = country.upper()
        lines = []
        cursor = self.db.collection(platform).find(query).sort("quality_score", DESCENDING).limit(5000)
        async for doc in cursor:
            entry = ProxyEntry(
                scheme=doc.get("scheme", "http"), host=doc["host"], port=doc["port"],
                username=doc.get("username"), password=doc.get("password"),
            )
            lines.append(entry.canonical)
        return "\n".join(lines).encode("utf-8")

    async def daily_summary(self, platform: str) -> dict[str, Any]:
        cutoff = now_utc() - timedelta(hours=24)
        coll = self.db.collection(platform)
        stats = await self.db.get_stats(platform)
        new_24h = await coll.count_documents({"first_seen_at": {"$gte": cutoff}})
        tested_24h = await coll.count_documents({"last_checked_at": {"$gte": cutoff}})
        working_24h = await coll.count_documents({"last_success_at": {"$gte": cutoff}})
        countries = await self.country_report(platform)
        pipeline = [
            {"$match": {"state": PlatformState.WORKING, "latency_ewma_ms": {"$ne": None}}},
            {"$group": {"_id": None, "avg": {"$avg": "$latency_ewma_ms"}}},
        ]
        avg_result = await coll.aggregate(pipeline).to_list(length=1)
        avg_latency = safe_int(avg_result[0]["avg"]) if avg_result else 0
        return {
            "total": stats["total"], "working": stats["working"], "quarantined": stats["quarantined"],
            "disabled": stats["disabled"], "new_24h": new_24h, "tested_24h": tested_24h,
            "working_24h": working_24h, "avg_latency_ms": avg_latency, "countries": countries,
        }

    async def persist_daily_summary(self, platform: str, summary: dict[str, Any]) -> None:
        date_key = now_utc().strftime("%Y-%m-%d")
        await self.db.daily.update_one(
            {"date": date_key, "platform": platform},
            {"$set": {**summary, "date": date_key, "platform": platform}},
            upsert=True,
        )

    @staticmethod
    def format_daily_summary(platform: str, summary: dict[str, Any]) -> str:
        lines = [
            f"📊 DAILY REPORT — {platform.capitalize()}", "",
            f"🌐 Total known: {summary.get('total', 0)}",
            f"🆕 New (24h): {summary.get('new_24h', 0)}",
            f"🧪 Tested (24h): {summary.get('tested_24h', 0)}",
            f"🟢 Working (24h): {summary.get('working_24h', 0)}",
            f"📈 Current working pool: {summary.get('working', 0)}",
            f"🟡 Quarantined: {summary.get('quarantined', 0)}",
            f"⚪ Disabled: {summary.get('disabled', 0)}",
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
        self._pending_bulk_paste: set[int] = set()

    def authorized(self, user_id: int) -> bool:
        return user_id == Config.OWNER_ID

    async def notify(self, platform: str, text: str) -> None:
        """Requirement #3: routes to the platform-specific log channel.
        Falls back to ADMIN_CHAT_ID if that platform's channel isn't
        configured, so nothing is silently dropped during setup."""
        if not self.bot:
            return
        chat_id = Config.PLATFORM_LOG_CHANNEL_ID.get(platform) or Config.ADMIN_CHAT_ID
        try:
            await self.bot.send_message(chat_id, text)
        except FloodWait as exc:
            await asyncio.sleep(getattr(exc, "value", 5))
            try:
                await self.bot.send_message(chat_id, text)
            except Exception:
                logger.exception("[TG] notify retry failed")
        except Exception:
            logger.exception("[TG] notify failed")

    def dashboard_markup(self):
        if InlineKeyboardMarkup is None:
            return None
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📺 YouTube", callback_data="platform:youtube"),
                 InlineKeyboardButton("📸 Instagram", callback_data="platform:instagram"),
                 InlineKeyboardButton("🎵 TikTok", callback_data="platform:tiktok")],
                [InlineKeyboardButton("🗂 Sources", callback_data="sources"),
                 InlineKeyboardButton("➕ Add source", callback_data="add_source")],
                [InlineKeyboardButton("🔁 Refresh sources now", callback_data="refresh_now")],
                [InlineKeyboardButton("⚙️ Health", callback_data="health")],
            ]
        )

    def platform_markup(self, platform: str):
        if InlineKeyboardMarkup is None:
            return None
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📊 Counts", callback_data=f"pf:{platform}:stats"),
                 InlineKeyboardButton("🌍 Countries", callback_data=f"pf:{platform}:countries")],
                [InlineKeyboardButton("🔀 Toggle enabled", callback_data=f"pf:{platform}:toggle"),
                 InlineKeyboardButton("📤 Export working", callback_data=f"pf:{platform}:export")],
                [InlineKeyboardButton("♻️ Force pool refresh", callback_data=f"pf:{platform}:refresh_confirm")],
                [InlineKeyboardButton("⬅️ Back", callback_data="dashboard")],
            ]
        )

    async def send_dashboard(self, message: Message) -> None:
        lines = ["🤖 Proxy Connectivity-Intelligence Bot", "", f"Uptime: {self.state.uptime_seconds() // 60} min", ""]
        for platform in PLATFORMS:
            lines.append(f"{platform.capitalize()}: pool={self.state.pool_health(platform)} "
                         f"working={self.state.last_working_pool_count.get(platform, 0)}")
        await message.reply_text("\n".join(lines), reply_markup=self.dashboard_markup())

    async def setup(self) -> None:
        if Client is None:
            logger.warning("[TG] pyrogram not available; Telegram admin UI disabled.")
            return
        self.bot = Client(
            "proxy_worker_bot_v3", bot_token=Config.BOT_TOKEN,
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
                    "source_id": source_id, "name": name, "url": url, "source_type": "MANUAL",
                    "enabled": True, "country": None, "protocol": None, "priority": 200,
                    "fetch_interval": Config.SOURCE_REFRESH_SECONDS,
                }
            )
            await message.reply_text(
                f"✅ Source added: {name}\n"
                f"Resolved format: {preview['format']}\n"
                f"Estimated entries: {preview['estimated_entries']}\n"
                f"Will be fetched for all enabled platforms every {Config.SOURCE_REFRESH_SECONDS}s."
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
            lines = []
            for platform in PLATFORMS:
                stats = await self.reports.system_stats(platform)
                lines.append(f"— {platform.capitalize()} —")
                lines.extend(f"{k}: {v}" for k, v in stats.items())
                lines.append("")
            await message.reply_text("📊 Stats\n\n" + "\n".join(lines))

        @self.bot.on_message(filters.command("refresh_now") & filters.private)
        async def _refresh(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            await message.reply_text("Running source refresh now...")
            summary = await self.scheduler.source_refresh_once()
            await message.reply_text(f"Done: {summary}")

        # --- Requirement #4: manual single/priority proxy check ---
        @self.bot.on_message(filters.command("addproxy") & filters.private)
        async def _addproxy(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                await message.reply_text(
                    "Usage: /addproxy <proxy_url> [proxy_url2 ...] (up to "
                    f"{Config.MAX_MANUAL_ITEMS_PER_BATCH})"
                )
                return
            candidates = [p.strip() for p in re.split(r"[\s,]+", parts[1]) if p.strip()]
            status_msg = await message.reply_text(f"⏳ Priority-checking {len(candidates)} proxy(ies)...")
            outcomes = await self.scheduler.priority_check_batch(candidates)

            reply_lines = []
            ok_count = 0
            for outcome in outcomes:
                if not outcome.get("ok"):
                    reply_lines.append(f"❌ {outcome.get('error')}")
                    continue
                proxy_str = mask_proxy_string(outcome["proxy"])
                working = [p for p, r in outcome["results"].items() if r.get("state") == PlatformState.WORKING]
                failing = {p: r for p, r in outcome["results"].items() if r.get("state") != PlatformState.WORKING}
                if working:
                    ok_count += 1
                    line = f"✅ {proxy_str} — working on: {', '.join(w.capitalize() for w in working)}"
                    for p, r in failing.items():
                        line += f"\n   ❌ {p.capitalize()}: {r.get('error') or r.get('error_category')}"
                    reply_lines.append(line)
                else:
                    line = f"❌ {proxy_str} — not working on any platform"
                    for p, r in outcome["results"].items():
                        line += f"\n   {p.capitalize()}: {r.get('error') or r.get('error_category')}"
                    reply_lines.append(line)

            reply_lines.append(f"\nSummary: {ok_count}/{len(outcomes)} working on at least one platform.")
            await status_msg.edit_text("\n\n".join(reply_lines)[:4000])

        # --- Requirement #4/#5: bulk paste / file ingestion ---
        @self.bot.on_message(filters.command("addlist") & filters.private)
        async def _addlist(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            parts = message.text.split(maxsplit=1)
            if len(parts) >= 2 and parts[1].strip():
                result = await self.scheduler.bulk_ingest_text(parts[1])
                await message.reply_text(
                    f"✅ Parsed {result['parsed']} candidate(s)\n"
                    f"Invalid: {result['invalid']} | Duplicates: {result['duplicates']}\n"
                    f"New per platform: {result['added']}"
                )
                return
            self._pending_bulk_paste.add(message.from_user.id)
            await message.reply_text(
                "Send your proxy list now: paste raw text, or send up to "
                f"{Config.MAX_MANUAL_ITEMS_PER_BATCH} .txt/.json/.csv file(s) "
                "in one or more messages. Send /done_addlist when finished."
            )

        @self.bot.on_message(filters.command("done_addlist") & filters.private)
        async def _done_addlist(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            self._pending_bulk_paste.discard(message.from_user.id)
            await message.reply_text("Bulk ingestion session closed.")

        @self.bot.on_message(filters.document & filters.private)
        async def _addlist_file(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            if message.from_user.id not in self._pending_bulk_paste:
                return
            doc = message.document
            if not doc or not str(doc.file_name or "").lower().endswith((".txt", ".json", ".csv")):
                await message.reply_text("Only .txt/.json/.csv files are accepted for /addlist.")
                return
            path = await message.download(file_name=f"/tmp/addlist_{doc.file_id}_{doc.file_name}")
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    text = fh.read()
            except Exception as exc:
                await message.reply_text(f"❌ Could not read file: {short_error(exc, 200)}")
                return
            result = await self.scheduler.bulk_ingest_text(text)
            await message.reply_text(
                f"✅ {doc.file_name}: parsed {result['parsed']}, invalid {result['invalid']}, "
                f"duplicates {result['duplicates']}, new per platform {result['added']}"
            )

        @self.bot.on_message(filters.text & filters.private & ~filters.command([
            "start", "add_source", "sources", "stats", "refresh_now", "addproxy", "addlist", "done_addlist", "test", "export",
        ]))
        async def _bulk_paste_text(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            if message.from_user.id not in self._pending_bulk_paste:
                return
            result = await self.scheduler.bulk_ingest_text(message.text)
            await message.reply_text(
                f"✅ Parsed {result['parsed']} candidate(s), invalid {result['invalid']}, "
                f"duplicates {result['duplicates']}, new per platform {result['added']}\n\n"
                "Send more, or /done_addlist when finished."
            )

        @self.bot.on_message(filters.command("test") & filters.private)
        async def _test(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                await message.reply_text("Usage: /test <youtube|instagram|tiktok> <proxy_id>")
                return
            args = parts[1].split(maxsplit=1)
            if len(args) < 2 or args[0] not in PLATFORMS:
                await message.reply_text("Usage: /test <youtube|instagram|tiktok> <proxy_id>")
                return
            result = await self.scheduler.test_specific(args[0], args[1].strip())
            await message.reply_text(
                f"Result: {json.dumps({k: v for k, v in result.items() if k != '_id'}, default=str)[:3500]}"
            )

        @self.bot.on_message(filters.command("export") & filters.private)
        async def _export(_, message: Message):
            if not self.authorized(message.from_user.id):
                return
            parts = message.text.split(maxsplit=1)
            platform = parts[1].strip().lower() if len(parts) > 1 else "youtube"
            if platform not in PLATFORMS:
                await message.reply_text("Usage: /export <youtube|instagram|tiktok>")
                return
            data = await self.reports.export_working(platform)
            path = f"/tmp/working_{platform}_{int(time.time())}.txt"
            with open(path, "wb") as fh:
                fh.write(data)
            await message.reply_document(path, caption=f"Verified WORKING {platform} proxies")

        @self.bot.on_callback_query()
        async def _callback(_, callback: CallbackQuery):
            if not self.authorized(callback.from_user.id):
                await callback.answer("Unauthorized", show_alert=True)
                return
            data = callback.data

            if data == "dashboard":
                await callback.message.edit_text(
                    "🤖 Proxy Connectivity-Intelligence Bot", reply_markup=self.dashboard_markup()
                )
            elif data.startswith("platform:"):
                platform = data.split(":", 1)[1]
                await callback.message.edit_text(
                    f"{platform.capitalize()} panel", reply_markup=self.platform_markup(platform)
                )
            elif data == "sources":
                await self.send_sources(callback.message)
            elif data == "add_source":
                await callback.message.reply_text("Send: /add_source <url> [name]")
            elif data == "refresh_now":
                await callback.answer("Refreshing...")
                summary = await self.scheduler.source_refresh_once()
                await callback.message.reply_text(f"Refresh done: {summary}")
            elif data == "health":
                await callback.message.edit_text(await self.health_text(), reply_markup=self.dashboard_markup())
            elif data.startswith("pf:"):
                _, platform, action = data.split(":", 2)
                await self._handle_platform_action(callback, platform, action)

            await callback.answer()

    async def _handle_platform_action(self, callback: CallbackQuery, platform: str, action: str) -> None:
        if action == "stats":
            stats = await self.reports.system_stats(platform)
            text = f"📊 {platform.capitalize()} stats\n" + "\n".join(f"{k}: {v}" for k, v in stats.items())
            await callback.message.edit_text(text, reply_markup=self.platform_markup(platform))
        elif action == "countries":
            rows = await self.reports.country_report(platform)
            text = f"🌍 {platform.capitalize()} countries\n" + (
                "\n".join(f"{r.get('_id') or 'UNKNOWN'}: {r.get('working', 0)}" for r in rows) or "No data yet."
            )
            await callback.message.edit_text(text, reply_markup=self.platform_markup(platform))
        elif action == "toggle":
            current = await self.db.platform_enabled(platform)
            await self.db.set_platform_enabled(platform, not current)
            await callback.message.edit_text(
                f"{platform.capitalize()} validation is now {'ENABLED' if not current else 'DISABLED'}.",
                reply_markup=self.platform_markup(platform),
            )
        elif action == "export":
            data = await self.reports.export_working(platform)
            path = f"/tmp/working_{platform}_{int(time.time())}.txt"
            with open(path, "wb") as fh:
                fh.write(data)
            await callback.message.reply_document(path, caption=f"Verified WORKING {platform} proxies")
        elif action == "refresh_confirm":
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, refresh", callback_data=f"pf:{platform}:refresh_go")],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"platform:{platform}")],
            ])
            await callback.message.edit_text(
                f"⚠️ This will pause {platform} background testing and double-check every "
                f"currently WORKING {platform} proxy. Continue?",
                reply_markup=markup,
            )
        elif action == "refresh_go":
            await callback.message.edit_text(
                f"♻️ Pool refresh started for {platform}. A report will be posted to the "
                f"{platform} log channel."
            )
            await self.scheduler.trigger_pool_refresh(platform)

    async def send_sources(self, message: Message) -> None:
        sources = await self.db.get_sources()
        if not sources:
            await message.reply_text("No sources configured yet. Use /add_source <url> [name].")
            return
        lines = ["🗂 Configured sources:"]
        for src in sources:
            status = "✅" if src.get("enabled") else "⛔"
            auto = " (auto-discovered)" if src.get("discovered") else ""
            lines.append(
                f"{status} {src.get('name')}{auto} | fmt={src.get('resolved_format') or '?'} "
                f"| last_items={src.get('last_item_count', 0)} | fails={src.get('failure_count', 0)}"
            )
        await message.reply_text("\n".join(lines))

    async def health_text(self) -> str:
        db_ok = await self.db.ping()
        lines = ["⚙️ Health", f"Mongo: {'OK' if db_ok else 'DOWN'}", f"Active tests: {self.state.active_tests}",
                  f"Dispatch paused: {self.state.dispatch_paused}"]
        for platform in PLATFORMS:
            stats = await self.reports.system_stats(platform)
            lines.append(f"{platform.capitalize()}: pool={self.state.pool_health(platform)} "
                         f"working={stats['working']}/{stats['total']}")
        return "\n".join(lines)

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
        return web.json_response({"service": "proxy-connectivity-intelligence-bot", "status": "running"})

    async def health(self, request: web.Request) -> web.Response:
        db_ok = await self.db.ping()
        status = 200 if db_ok else 503
        return web.json_response(
            {
                "status": "ok" if db_ok else "degraded",
                "mongo": db_ok,
                "uptime_seconds": self.state.uptime_seconds(),
                "scheduler_running": self.state.scheduler_running,
                "pool_health": {p: self.state.pool_health(p) for p in PLATFORMS},
                "working_pool": self.state.last_working_pool_count,
                "active_tests": self.state.active_tests,
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
        self.validator = ConnectivityValidator()
        self.reports = ReportManager(self.db)
        self.state = WorkerState()
        self.admin_ui = TelegramAdminUI(self.db, self.sources, None, self.reports, self.state)
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
                "youtube",
                "🟢 Proxy Connectivity-Intelligence Bot v3 started.\n"
                "Platforms: YouTube, Instagram, TikTok (public homepage reachability probes only).\n"
                "Staged state machine: WORKING (1h) -> QUARANTINED (5h, up to 48h) -> DISABLED.",
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
                pass

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
