#!/usr/bin/env python3
"""
Dedicated Proxy Worker Bot - Multi-Platform Implementation (v5).

============================================================================
V5 CHANGELOG (maps directly to the requested fixes)
============================================================================
 1. MIGRATION SYSTEM FULLY REMOVED
    - Deleted the legacy per-platform "migration source" collections, the
      Settings subpanels, the Check/Retry + Migrate/Import buttons, the
      migration audit log, and every related DB/UI code path.
    - This also fixes a live crash bug: v4 still *instantiated* an
      undefined `MigrationService` class at startup (a leftover from a
      partial manual removal), which would raise NameError before the
      bot could ever come online. That dangling reference is gone.

 2. NO MORE GLOBAL 429 / CHALLENGE FREEZE
    - Removed `BaseValidator.trigger_rate_limit_backoff()` /
      `is_rate_limited()`, which used to pause an ENTIRE platform
      validator (shared across every proxy) for 15-30 minutes whenever
      any single proxy got a 429.
    - 429/challenge responses now only ever touch the ONE proxy that
      triggered them: `Database.record_platform_result()` applies an
      escalating, per-proxy-only cooldown (`consecutive_429_count`) and
      immediately frees up the dispatcher to claim the next proxy.

 3. VALIDATION ENGINE / TIKTOK DETECTION
    - New dedicated `TikTokValidator` with modern browser headers,
      redirect following, and response-body inspection for soft-block /
      captcha "verify" walls that return HTTP 200 (not just status-code
      checks), to cut down false negatives AND false positives.
    - Quality score (0-100) now genuinely drives proxy selection (see #8).

 4/#9. REAL <=2-HOUR NON-DESTRUCTIVE REVALIDATION
    - `Config.QUARANTINE_RETEST_MAX_SECONDS` (default 7200s = 2h) is now
      hard-clamped onto every quarantine retry calculation, regardless of
      flap-recovery speed-ups or reputation-penalty slow-downs, so a
      failed proxy is *always* re-tested within 2 hours - never silently
      pushed out by the (unrelated) 48h WORKING-proxy stagger window.
    - Failures never delete a proxy; they only move it through
      WORKING -> QUARANTINED -> DISABLED, and DISABLED proxies are only
      ever archived (moved to `proxy_archive`), never destroyed.

 5/#9/#10. FASTER, CONCURRENT, CONTINUOUS DISCOVERY
    - Source refresh and GitHub-tree discovery now run many sources
      concurrently (bounded by `SOURCE_FETCH_CONCURRENCY`) instead of
      one-at-a-time, without blocking the validation dispatchers.
    - Runs forever on its own schedule (`discovery_scheduler_loop` /
      `periodic_scheduler_loop`), not just when a user asks.

 6. PER-PLATFORM "ADD FILE" BUTTON
    - Every platform subpanel (YouTube/Instagram/TikTok) now has an
      "Add File" button. Tapping it arms a per-user, per-platform upload
      state; the next document that user sends is fast-tracked and
      tested ONLY against that platform, with live-edited progress
      (done/working/failed counts + a rolling list of hits).
    - The original, platform-agnostic Global TXT/CSV/JSON upload (which
      tests a file against all three platforms) is untouched.

 8. QUALITY SCORE ACTUALLY DRIVES SELECTION
    - `claim_proxy()` now sorts pinned-first, then by `quality_score`
      DESC, then by due time - so the queue itself, not just the export,
      prefers proven, fast, reliable proxies. Exports were already
      quality-ranked and remain so.

 9 (independence). Platforms remain fully isolated: separate Mongo
    collections per platform, independent `platform_status` state
    machines, independent cooldowns/circuit breakers/bandwidth budgets.
    `enqueue_cross_platform_check()` only ever *queues a candidate test*
    on other platforms - it never marks a proxy "working" anywhere
    without that platform verifying it itself.

 11. ADAPTIVE CONCURRENCY / BANDWIDTH
    - Each platform now runs a small pool of concurrent worker
      coroutines (not one sequential loop), sized dynamically by
      `concurrency_controller_loop()` based on backlog size and recent
      failure rate, capped by `MAX_WORKERS_PER_PLATFORM` and the global
      `TEST_CONCURRENCY` semaphore. Circuit-open platforms scale to 0
      workers instead of hammering a dead route.

 12. FORMAT-AGNOSTIC FILE IMPORT
    - `sniff_and_parse()` content-sniffs uploads (JSON / CSV-like /
      plain text) regardless of file extension, so .log, .lst, or
      extension-less exports all work. Malformed lines are skipped
      without aborting the batch.

 13/#14/#15/#16. SELF-DIAGNOSTIC FIXES
    - Fixed the dangling `MigrationService` crash (see #1).
    - `upsert_proxy_to_platforms()` is now a single atomic
      `update_one(..., upsert=True)` per platform (via `$setOnInsert` /
      `$addToSet`) instead of find-then-insert, removing a race window
      where concurrent discovery workers or multi-worker validation
      pools could create duplicate proxy documents.
    - `claim_proxy()` was already atomic (`find_one_and_update`), which
      is what makes it safe to run several concurrent workers per
      platform now that dispatch is no longer a single sequential loop.

Everything else from v4 (YouTube/Instagram/TikTok validators, staged
WORKING -> QUARANTINED -> DISABLED state machine, quality scoring,
staggered revalidation, circuit breaker, bandwidth budget, pinning,
pruning/archival, cross-platform reuse, reputation memory, export-diff
snapshots, Telegram admin UI, health server) is preserved.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import random
import re
import shutil
import signal
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, unquote, urlparse

import aiohttp
from aiohttp import web
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
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


def env_int(name: str, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        val = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        val = default
    if minimum is not None:
        val = max(minimum, val)
    if maximum is not None:
        val = min(val, maximum)
    return val


def env_float(name: str, default: float, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    try:
        val = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        val = default
    if minimum is not None:
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
    OWNER_ID = env_int("OWNER_ID", 0)
    MONGO_URI = os.getenv("MONGO_URI", "").strip()
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "telegram_downloader").strip()

    # Active per-platform collections. These are the ONLY proxy
    # collections the engine reads from or writes to (Requirement #1:
    # legacy migration collections/logic have been fully removed).
    COLLECTION_NAMES = {"youtube": "YouTube", "instagram": "Instagram", "tiktok": "TikTok"}

    PORT = env_int("PORT", 8080, 1, 65535)

    # Per-platform Telegram Log Channels
    YOUTUBE_LOG_CHANNEL_ID = env_int("YOUTUBE_LOG_CHANNEL_ID", OWNER_ID)
    INSTAGRAM_LOG_CHANNEL_ID = env_int("INSTAGRAM_LOG_CHANNEL_ID", OWNER_ID)
    TIKTOK_LOG_CHANNEL_ID = env_int("TIKTOK_LOG_CHANNEL_ID", OWNER_ID)
    ADMIN_CHAT_ID = env_int("REPORT_CHAT_ID", OWNER_ID)

    # Concurrency & Schedulers
    SOURCE_REFRESH_SECONDS = env_int("SOURCE_REFRESH_SECONDS", 300, 30)
    SOURCE_FETCH_CONCURRENCY = env_int("SOURCE_FETCH_CONCURRENCY", 5, 1, 50)
    TEST_CONCURRENCY = env_int("PROXY_TEST_CONCURRENCY", 20, 1, 200)
    MAX_WORKERS_PER_PLATFORM = env_int("MAX_WORKERS_PER_PLATFORM", 8, 1, 50)
    PER_PLATFORM_TEST_BUDGET = env_int("PER_PLATFORM_TEST_BUDGET", 600, 10, 20000)
    DISCOVERY_INTERVAL_SECONDS = env_int("DISCOVERY_INTERVAL_SECONDS", 1800, 300)
    DISCOVERY_FETCH_CONCURRENCY = env_int("DISCOVERY_FETCH_CONCURRENCY", 5, 1, 50)
    ADHOC_TEST_CONCURRENCY = env_int("ADHOC_TEST_CONCURRENCY", 10, 1, 100)
    CONTROLLER_INTERVAL_SECONDS = env_int("CONTROLLER_INTERVAL_SECONDS", 20, 5, 300)

    # Timeouts
    CONNECT_CHECK_TIMEOUT = env_int("CONNECT_CHECK_TIMEOUT", 6, 1, 30)
    GENERIC_TIMEOUT = env_int("HTTP_CONNECT_TIMEOUT", 12, 3, 120)
    GEO_TIMEOUT = env_int("GEO_TIMEOUT", 10, 3, 60)
    YOUTUBE_TIMEOUT = env_int("YOUTUBE_TEST_TIMEOUT", 35, 10, 180)
    INSTAGRAM_TIMEOUT = env_int("INSTAGRAM_TEST_TIMEOUT", 15, 5, 60)
    TIKTOK_TIMEOUT = env_int("TIKTOK_TEST_TIMEOUT", 15, 5, 60)

    # State Machine Intervals
    WORKING_CHECK_INTERVAL = env_int("WORKING_CHECK_INTERVAL", 3600, 300)        # 1 hour base
    WORKING_REVALIDATION_WINDOW_SECONDS = env_int("WORKING_REVALIDATION_WINDOW_SECONDS", 172800, 3600)  # stagger window for healthy proxies
    QUARANTINE_CHECK_INTERVAL = env_int("QUARANTINE_CHECK_INTERVAL", 3600, 300)   # base retry wait while quarantined
    QUARANTINE_RETEST_MAX_SECONDS = env_int("QUARANTINE_RETEST_MAX_SECONDS", 7200, 300, 21600)  # HARD cap: retested within <= 2h
    QUARANTINE_HARD_CUTOFF = env_int("QUARANTINE_HARD_CUTOFF", 172800, 3600)      # 48h of continuous failure before DISABLED (never deleted)
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

    # --- Bandwidth budget guard ---
    BANDWIDTH_BUDGET_WINDOW_SECONDS = env_int("BANDWIDTH_BUDGET_WINDOW_SECONDS", 3600, 60)

    # --- Circuit breaker ---
    CIRCUIT_BREAKER_FAILURE_THRESHOLD = env_float("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 0.8, 0.1, 1.0)
    CIRCUIT_BREAKER_MIN_SAMPLES = env_int("CIRCUIT_BREAKER_MIN_SAMPLES", 20, 5)
    CIRCUIT_BREAKER_WINDOW_SECONDS = env_int("CIRCUIT_BREAKER_WINDOW_SECONDS", 600, 60)
    CIRCUIT_BREAKER_COOLDOWN_SECONDS = env_int("CIRCUIT_BREAKER_COOLDOWN_SECONDS", 1800, 60)

    # --- Pruning / archival (non-destructive: archive-then-delete only after this long DISABLED) ---
    PRUNE_DISABLED_AFTER_SECONDS = env_int("PRUNE_DISABLED_AFTER_SECONDS", 7 * 86400, 3600)
    PRUNE_CHECK_INTERVAL_SECONDS = env_int("PRUNE_CHECK_INTERVAL_SECONDS", 3600, 300)

    # --- Cross-platform reuse ---
    CROSS_PLATFORM_REUSE_ENABLED = env_bool("CROSS_PLATFORM_REUSE_ENABLED", True)

    # --- Reputation memory ---
    REPUTATION_FAILURE_PENALTY_STEP = env_int("REPUTATION_FAILURE_PENALTY_STEP", 5, 1, 50)
    REPUTATION_MAX_PENALTY = env_int("REPUTATION_MAX_PENALTY", 60, 0, 100)

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
logger = logging.getLogger("proxy-worker-v5")
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

    # Categories that reflect a transient/target-side condition rather than
    # a real proxy fault. Handled distinctly from 429 (see #2 below), which
    # gets its own escalating-but-strictly-per-proxy cooldown.
    NON_ROUTE_SPECIFIC = frozenset({ENVIRONMENT_ERROR, TARGET_UNAVAILABLE})

    RATE_LIMIT_CATEGORIES = frozenset({HTTP_429, RATE_LIMITED})


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

            # Handle username/password for proxy authentication
            user = col(row, "user") or col(row, "username")
            password = col(row, "pass") or col(row, "password")

            if ip and port:
                # Build proxy URL with credentials if present
                auth_part = f"{user}:{password}@" if user and password is not None else ""
                candidate = f"{proto + '://' if proto else ''}{auth_part}{ip}:{port}"
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


def sniff_and_parse(text: str) -> List[ParsedCandidate]:
    """
    Requirement #12: format-agnostic parsing. Content-sniffs a payload
    instead of trusting a file extension - so .log, .lst, extension-less,
    or mislabeled exports all still work. Tries JSON, then CSV-like,
    then falls back to a plain newline-delimited list. Malformed/garbage
    lines are silently skipped by the underlying parsers rather than
    aborting the whole import.
    """
    text_stripped = text.strip()
    if not text_stripped:
        return []

    if text_stripped[0] in "[{":
        parsed = parse_json_payload(text)
        if parsed:
            return parsed

    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if lines:
        sample = lines[: min(25, len(lines))]
        comma_lines = sum(1 for ln in sample if ln.count(",") >= 2)
        if comma_lines >= max(1, len(sample) // 2):
            parsed = parse_csv_payload(text)
            if parsed:
                return parsed

    return parse_txt_payload(text)


def parse_source_payload(text: str, content_type: str, url: str = "") -> List[ParsedCandidate]:
    fmt = detect_format(content_type, url)
    if fmt == "json":
        parsed = parse_json_payload(text)
        if parsed:
            return parsed
        return sniff_and_parse(text)
    if fmt == "csv":
        parsed = parse_csv_payload(text)
        if parsed:
            return parsed
        return sniff_and_parse(text)
    # Even for plain-text-looking sources, fall back to full content
    # sniffing in case the extension/content-type was misleading.
    parsed = parse_txt_payload(text)
    if parsed:
        return parsed
    return sniff_and_parse(text)


# ============================================================================
# QUALITY SCORE & STAGGERED SCHEDULING
# ============================================================================

def compute_quality_score(doc: Dict[str, Any], platform: str, reputation_penalty: int = 0) -> int:
    """
    Blends recent success rate, average latency, and verification recency
    into a single 0-100 score, then subtracts any reputation penalty so
    chronically-bad proxies sink to the bottom of both the retest queue
    (see Database.claim_proxy) and exports.
    """
    p_stat = (doc.get("platform_status") or {}).get(platform, {})
    success_count = safe_int(p_stat.get("success_count", 0))
    fail_count = safe_int(p_stat.get("fail_count", 0))
    total = success_count + fail_count
    success_rate = (success_count / total) if total > 0 else 0.5  # neutral prior for brand-new proxies

    latency_ms = safe_float(doc.get("latency_ms") or 0)
    if latency_ms <= 0:
        latency_factor = 0.5
    else:
        # ~300ms -> close to 1.0, ~5000ms -> close to 0.0
        latency_factor = max(0.0, min(1.0, 1.0 - (latency_ms - 300.0) / 4700.0))

    last_tested = parse_dt(doc.get("last_tested_at"))
    if last_tested:
        age_hours = (now_utc() - last_tested).total_seconds() / 3600.0
        recency_factor = max(0.0, min(1.0, 1.0 - age_hours / 48.0))
    else:
        recency_factor = 0.0

    raw = (0.5 * success_rate + 0.2 * latency_factor + 0.3 * recency_factor) * 100.0
    raw -= min(raw, reputation_penalty)
    return int(max(0, min(100, round(raw))))


def stagger_offset_seconds(proxy_id: str, window_seconds: int) -> int:
    h = int(hashlib.sha256(proxy_id.encode("utf-8")).hexdigest(), 16)
    return h % max(1, window_seconds)


def staggered_next_check(proxy_id: str, base_interval_seconds: int, window_seconds: int) -> datetime:
    """
    Spreads WORKING re-checks across a rolling window (default 48h) based
    on a stable per-proxy hash offset, instead of everyone becoming due at
    once. NOTE: this only ever applies to already-WORKING (successful)
    proxies - quarantined/failed proxies use the separate, hard-capped
    <=2h retry logic in Database.record_platform_result, so this window
    can never delay a failing proxy's retest (Requirement #4/#9).
    """
    offset = stagger_offset_seconds(proxy_id, window_seconds) - (window_seconds // 2)
    seconds = max(60, base_interval_seconds + offset)
    return now_utc() + timedelta(seconds=seconds)


# ============================================================================
# CIRCUIT BREAKER & BANDWIDTH BUDGET
# ============================================================================

class CircuitBreaker:
    """Pauses a platform's testing when mass failure is detected."""

    def __init__(self, platform: str) -> None:
        self.platform = platform
        self.window: "deque[Tuple[datetime, bool]]" = deque()
        self.state = "CLOSED"  # CLOSED -> OPEN -> HALF_OPEN -> CLOSED/OPEN
        self.open_until: Optional[datetime] = None
        self._half_open_probe_results: List[bool] = []

    def _prune(self) -> None:
        cutoff = now_utc() - timedelta(seconds=Config.CIRCUIT_BREAKER_WINDOW_SECONDS)
        while self.window and self.window[0][0] < cutoff:
            self.window.popleft()

    def allow_request(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if self.open_until and now_utc() >= self.open_until:
                self.state = "HALF_OPEN"
                self._half_open_probe_results = []
                return True
            return False
        return True  # HALF_OPEN allows probes through

    def record(self, success: bool) -> bool:
        """Returns True if the breaker just tripped OPEN (caller should alert admin)."""
        now = now_utc()
        if self.state == "HALF_OPEN":
            self._half_open_probe_results.append(success)
            if len(self._half_open_probe_results) >= 3:
                good = sum(1 for r in self._half_open_probe_results if r)
                if good >= 2:
                    self.state = "CLOSED"
                    self.window.clear()
                else:
                    self.state = "OPEN"
                    self.open_until = now + timedelta(seconds=Config.CIRCUIT_BREAKER_COOLDOWN_SECONDS)
                self._half_open_probe_results = []
            return False

        self.window.append((now, success))
        self._prune()
        if len(self.window) >= Config.CIRCUIT_BREAKER_MIN_SAMPLES and self.state == "CLOSED":
            fail_rate = sum(1 for _, s in self.window if not s) / len(self.window)
            if fail_rate >= Config.CIRCUIT_BREAKER_FAILURE_THRESHOLD:
                self.state = "OPEN"
                self.open_until = now + timedelta(seconds=Config.CIRCUIT_BREAKER_COOLDOWN_SECONDS)
                return True
        return False

    def fail_rate(self) -> float:
        self._prune()
        if not self.window:
            return 0.0
        return sum(1 for _, s in self.window if not s) / len(self.window)


class BandwidthBudget:
    """Caps the number of tests a platform may run per rolling window."""

    def __init__(self, max_tests_per_window: int, window_seconds: int) -> None:
        self.max_tests = max_tests_per_window
        self.window_seconds = window_seconds
        self.window_start = now_utc()
        self.count = 0

    def _maybe_reset(self) -> None:
        if (now_utc() - self.window_start).total_seconds() >= self.window_seconds:
            self.window_start = now_utc()
            self.count = 0

    def try_consume(self) -> bool:
        self._maybe_reset()
        if self.count >= self.max_tests:
            return False
        self.count += 1
        return True

    def seconds_until_reset(self) -> float:
        self._maybe_reset()
        return max(0.0, self.window_seconds - (now_utc() - self.window_start).total_seconds())


# ============================================================================
# DATABASE LAYER (Per-Platform Collections, Retention, No Migration Baggage)
# ============================================================================

class Database:
    def __init__(self) -> None:
        self.client: Optional[AsyncMongoClient] = None
        self.db = None

        # The ONLY proxy collections in the system now (Requirement #1).
        self.cols: Dict[str, Any] = {}

        self.sources = None
        self.tasks = None
        self.snapshots = None
        self.events = None
        self.daily = None
        self.worker_config = None
        self.reputation = None
        self.archive = None
        self.export_snapshots = None

    def get_col(self, platform: str):
        return self.cols.get(platform, self.cols["youtube"])

    async def connect(self) -> None:
        max_retries = 5
        base_delay = 1.0  # Start with 1 second delay

        for attempt in range(max_retries):
            try:
                self.client = AsyncMongoClient(
                    Config.MONGO_URI,
                    serverSelectionTimeoutMS=8000,
                    connectTimeoutMS=8000,
                    socketTimeoutMS=20000,
                    retryWrites=True,
                )
                await self.client.admin.command("ping")
                self.db = self.client[Config.MONGO_DB_NAME]

                self.cols = {
                    "youtube": self.db[Config.COLLECTION_NAMES["youtube"]],
                    "instagram": self.db[Config.COLLECTION_NAMES["instagram"]],
                    "tiktok": self.db[Config.COLLECTION_NAMES["tiktok"]],
                }

                self.sources = self.db["proxy_sources"]
                self.tasks = self.db["proxy_tasks"]
                self.snapshots = self.db["proxy_source_snapshots"]
                self.events = self.db["proxy_events"]
                self.daily = self.db["proxy_daily_summary"]
                self.worker_config = self.db["worker_config"]
                self.reputation = self.db["proxy_reputation"]
                self.archive = self.db["proxy_archive"]
                self.export_snapshots = self.db["export_snapshots"]

                await self.ensure_indexes()
                logger.info("[DB] Connected. Active collections: %s", list(Config.COLLECTION_NAMES.values()))
                return  # Success, exit the retry loop
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    logger.error(f"[DB] Failed to connect to MongoDB after {max_retries} attempts: {e}")
                    raise
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"[DB] MongoDB connection attempt {attempt + 1} failed: {e}. Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)

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
            await col.create_index([("quality_score", DESCENDING)])
            await col.create_index([("pinned", DESCENDING)])
            await col.create_index([("verified_country", ASCENDING)])
            await col.create_index([("source_ids", ASCENDING)])

        await self.sources.create_index([("source_id", ASCENDING)], unique=True)
        await self.tasks.create_index([("task_id", ASCENDING)], unique=True)
        await self.events.create_index([("proxy_id", ASCENDING), ("created_at", DESCENDING)])
        await self.daily.create_index([("date", ASCENDING)], unique=True)
        await self.reputation.create_index([("proxy_id", ASCENDING)], unique=True)
        await self.archive.create_index([("archived_platform", ASCENDING), ("archived_at", DESCENDING)])
        await self.export_snapshots.create_index([("platform", ASCENDING), ("created_at", DESCENDING)])

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
        inc: Dict[str, Any] = {}
        if content_hash is not None:
            update["last_content_hash"] = content_hash
        if item_count is not None:
            update["last_item_count"] = item_count
            inc["yield_total_discovered"] = item_count
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
            if inc:
                await self.sources.update_one({"source_id": source_id}, {"$set": update, "$inc": inc})
            else:
                await self.sources.update_one({"source_id": source_id}, {"$set": update})
        else:
            update["last_failure_at"] = now_utc()
            if error:
                update["last_error"] = short_error(error)
            inc["failure_count"] = 1
            await self.sources.update_one({"source_id": source_id}, {"$set": update, "$inc": inc})

    async def increment_source_yield(self, source_id: Optional[str]) -> None:
        if not source_id:
            return
        await self.sources.update_one({"source_id": source_id}, {"$inc": {"yield_working_count": 1}})

    # --- Proxy ingestion (atomic, race-free upsert) ---

    @staticmethod
    def _default_platform_status(now: datetime) -> Dict[str, Any]:
        return {
            "state": PlatformState.QUARANTINED,
            "working": False,
            "last_checked_at": None,
            "next_check_at": now,  # eligible for immediate test
            "quarantined_since": now,
            "consecutive_fail_windows": 0,
            "consecutive_429_count": 0,
            "flap_recovery_count": 0,
            "success_count": 0,
            "fail_count": 0,
            "last_error": None,
            "last_error_category": None,
            "last_notified_state": None,
        }

    async def upsert_proxy_to_platforms(
        self, entry: ProxyEntry, country: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Requirement #15: atomic, idempotent upsert. Uses a single
        `update_one(..., upsert=True)` per platform collection (rather
        than find-then-insert) so concurrent discovery workers or
        concurrent validation workers can never race each other into
        creating duplicate proxy_id documents.
        """
        now = now_utc()
        is_new_overall = False
        sample_doc: Dict[str, Any] = {}

        for platform, col in self.cols.items():
            insert_doc: Dict[str, Any] = {
                "proxy_id": entry.proxy_id,
                "proxy_url": entry.canonical,
                "scheme": entry.scheme,
                "host": entry.host,
                "port": entry.port,
                "username": entry.username,
                "password": entry.password,
                "source_country": country or entry.source_country,
                "requires_auth_missing": entry.requires_auth_missing,
                "enabled": True,
                "retired": False,
                "ever_working": False,
                "pinned": False,
                "quality_score": 0,
                "verified_country": None,
                "country_name": None,
                "latency_ms": None,
                "first_seen_at": now,
                "last_tested_at": None,
                "lease_until": None,
                "platform_status": {platform: self._default_platform_status(now)},
            }

            set_fields: Dict[str, Any] = {"last_seen_at": now, "source_present": True}
            update_ops: Dict[str, Any] = {"$set": set_fields, "$setOnInsert": insert_doc}
            if entry.source_id:
                update_ops["$addToSet"] = {"source_ids": entry.source_id}
            if country and not entry.source_id:
                # If we somehow have country info without a source id, still
                # try to backfill it on existing docs that lack one.
                pass

            try:
                result = await col.update_one({"proxy_id": entry.proxy_id}, update_ops, upsert=True)
            except Exception:
                logger.exception("[DB] Upsert failed for proxy on %s", platform)
                continue

            if getattr(result, "upserted_id", None) is not None:
                is_new_overall = True

            doc = await col.find_one({"proxy_id": entry.proxy_id})
            if doc:
                sample_doc = doc

        return is_new_overall, sample_doc

    async def claim_proxy(self, platform: str, lease_seconds: int = 180) -> Optional[Dict[str, Any]]:
        """
        Requirement #8: quality_score now genuinely drives selection.
        Pinned proxies go first, then the highest-quality/most-reliable
        proxies among those currently due, then earliest-due as a
        tiebreaker. `find_one_and_update` is atomic, which is what makes
        it safe to run several concurrent workers per platform.
        """
        now = now_utc()
        col = self.get_col(platform)
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
            sort=[
                ("pinned", DESCENDING),
                ("quality_score", DESCENDING),
                (f"platform_status.{platform}.next_check_at", ASCENDING),
            ],
            return_document=True,
        )

    async def count_due(self, platform: str) -> int:
        now = now_utc()
        col = self.get_col(platform)
        return await col.count_documents(
            {
                "enabled": True,
                "retired": False,
                f"platform_status.{platform}.state": {"$ne": PlatformState.DISABLED},
                f"platform_status.{platform}.next_check_at": {"$lte": now},
            }
        )

    async def release_lease(self, platform: str, proxy_id: str) -> None:
        await self.get_col(platform).update_one({"proxy_id": proxy_id}, {"$set": {"lease_until": None}})

    async def release_expired_leases(self) -> int:
        now = now_utc()
        released = 0
        for col in self.cols.values():
            res = await col.update_many(
                {"lease_until": {"$ne": None, "$lte": now}},
                {"$set": {"lease_until": None}},
            )
            released += res.modified_count
        return released

    # --- Persistent Reputation Memory ---

    async def record_reputation(self, proxy_id: str, success: bool) -> int:
        now = now_utc()
        inc = {"successes": 1} if success else {"failures": 1}
        await self.reputation.update_one(
            {"proxy_id": proxy_id},
            {"$inc": inc, "$set": {"last_seen_at": now}, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        doc = await self.reputation.find_one({"proxy_id": proxy_id}) or {}
        failures = safe_int(doc.get("failures", 0))
        successes = safe_int(doc.get("successes", 0))
        penalty = min(
            Config.REPUTATION_MAX_PENALTY,
            max(0, (failures - successes) * Config.REPUTATION_FAILURE_PENALTY_STEP),
        )
        if penalty != safe_int(doc.get("penalty", 0)):
            await self.reputation.update_one({"proxy_id": proxy_id}, {"$set": {"penalty": penalty}})
        return penalty

    # --- Staged Revalidation State Machine ---

    async def record_platform_result(
        self, platform: str, proxy_id: str, result: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Applies the WORKING/QUARANTINED/DISABLED state machine plus scoring & staggering."""
        now = now_utc()
        col = self.get_col(platform)
        doc = await col.find_one({"proxy_id": proxy_id})
        if not doc:
            return {}, {}

        p_stat = doc.get("platform_status", {}).get(platform, {})
        current_state = p_stat.get("state", PlatformState.QUARANTINED)
        consecutive_fails = safe_int(p_stat.get("consecutive_fail_windows", 0))
        q_since = parse_dt(p_stat.get("quarantined_since"))
        flap_count = safe_int(p_stat.get("flap_recovery_count", 0))
        success_count = safe_int(p_stat.get("success_count", 0))
        fail_count = safe_int(p_stat.get("fail_count", 0))

        success = bool(result.get("ok"))
        category = result.get("category", FailureCategory.UNKNOWN)
        error_msg = result.get("error")

        # Reputation memory persists across re-adds, keyed by proxy_id only.
        penalty = await self.record_reputation(proxy_id, success)

        meta_update: Dict[str, Any] = {
            "lease_until": None,
            "last_tested_at": now,
        }
        if result.get("latency_ms"):
            meta_update["latency_ms"] = result["latency_ms"]
        if result.get("country_code"):
            meta_update["verified_country"] = result["country_code"]
            meta_update["country_name"] = result.get("country_name")

        p_update: Dict[str, Any] = {
            "last_checked_at": now,
            "last_error": short_error(error_msg),
            "last_error_category": category,
            "success_count": success_count + (1 if success else 0),
            "fail_count": fail_count + (0 if success else 1),
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
            p_update["consecutive_429_count"] = 0
            # Staggered scheduling instead of a flat interval, further
            # stretched out for proxies with reputation baggage.
            p_update["next_check_at"] = staggered_next_check(
                proxy_id,
                Config.WORKING_CHECK_INTERVAL + int(penalty * 60),
                Config.WORKING_REVALIDATION_WINDOW_SECONDS,
            )

            transition_meta["now_working"] = True
            if current_state == PlatformState.QUARANTINED:
                transition_meta["recovered"] = True
                p_update["flap_recovery_count"] = flap_count + 1
                if q_since:
                    transition_meta["downtime_hours"] = round((now - q_since).total_seconds() / 3600.0, 1)

            if doc.get("source_ids"):
                await self.increment_source_yield(doc["source_ids"][0])
        else:
            p_update["working"] = False
            transition_meta["now_working"] = False

            if category in FailureCategory.RATE_LIMIT_CATEGORIES:
                # Requirement #2: NO platform-wide freeze. Only THIS proxy
                # gets a cooldown, and it escalates only for repeat
                # offenders on this exact proxy - every other proxy in the
                # pool (including ones behind the same source) keeps
                # testing normally without any interruption.
                consec_429 = safe_int(p_stat.get("consecutive_429_count", 0)) + 1
                p_update["consecutive_429_count"] = consec_429
                cooldown_minutes = min(60, 5 * consec_429)
                p_update["state"] = current_state
                p_update["next_check_at"] = now + timedelta(minutes=cooldown_minutes)
            elif category in FailureCategory.NON_ROUTE_SPECIFIC:
                p_update["state"] = current_state
                p_update["next_check_at"] = now + timedelta(minutes=15)
            elif current_state == PlatformState.WORKING:
                p_update["state"] = PlatformState.QUARANTINED
                p_update["quarantined_since"] = now
                p_update["consecutive_fail_windows"] = 1

                base_wait = min(Config.QUARANTINE_CHECK_INTERVAL, Config.QUARANTINE_RETEST_MAX_SECONDS)
                if flap_count > 0:
                    base_wait = max(600, int(base_wait / (1 + flap_count * 0.5)))
                base_wait = int(base_wait * (1 + penalty / 100.0))
                # Requirement #4/#9: HARD cap - never delayed past 2h no
                # matter what the flap/penalty math produces.
                base_wait = min(base_wait, Config.QUARANTINE_RETEST_MAX_SECONDS)
                p_update["next_check_at"] = now + timedelta(seconds=base_wait)
            elif current_state == PlatformState.QUARANTINED:
                p_update["consecutive_fail_windows"] = consecutive_fails + 1
                effective_q_since = q_since or now
                elapsed = (now - effective_q_since).total_seconds()

                if elapsed >= Config.QUARANTINE_HARD_CUTOFF:
                    p_update["state"] = PlatformState.DISABLED
                    p_update["next_check_at"] = None
                    transition_meta["permanently_disabled"] = True
                else:
                    base_wait = min(Config.QUARANTINE_CHECK_INTERVAL, Config.QUARANTINE_RETEST_MAX_SECONDS)
                    if flap_count > 0:
                        base_wait = max(600, int(base_wait / (1 + flap_count * 0.5)))
                    base_wait = int(base_wait * (1 + penalty / 100.0))
                    base_wait = min(base_wait, Config.QUARANTINE_RETEST_MAX_SECONDS)
                    p_update["next_check_at"] = now + timedelta(seconds=base_wait)
            else:
                p_update["state"] = PlatformState.DISABLED
                p_update["next_check_at"] = None

        # Set working field correctly: True only if state is WORKING and validation succeeded
        final_state = p_update.get("state", p_stat.get("state"))
        p_update["working"] = success and (final_state == PlatformState.WORKING)

        meta_update[f"platform_status.{platform}"] = {**p_stat, **p_update}

        # Recompute quality score using the freshly-merged state.
        temp_doc = {**doc, **meta_update}
        meta_update["quality_score"] = compute_quality_score(temp_doc, platform, reputation_penalty=penalty)

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

    async def get_average_quality(self, platform: str) -> int:
        col = self.get_col(platform)
        docs = await col.find({"enabled": True}, {"quality_score": 1}).to_list(length=20000)
        scores = [safe_int(d.get("quality_score", 0)) for d in docs]
        return int(round(sum(scores) / len(scores))) if scores else 0

    async def count_pinned(self, platform: str) -> int:
        return await self.get_col(platform).count_documents({"pinned": True})

    async def count_archived(self, platform: str) -> int:
        return await self.archive.count_documents({"archived_platform": platform})

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

    async def mark_missing_from_sources(self, proxy_ids: List[str]) -> None:
        if not proxy_ids:
            return
        now = now_utc()
        for col in self.cols.values():
            await col.update_many(
                {"proxy_id": {"$in": proxy_ids}},
                {"$set": {"source_present": False, "source_missing_since": now}},
            )

    # --- Manual Pin / Priority Override ---

    async def set_pinned(self, platform: str, proxy_id_prefix: str, pinned: bool) -> Optional[str]:
        col = self.get_col(platform)
        doc = await col.find_one({"proxy_id": {"$regex": f"^{re.escape(proxy_id_prefix)}"}})
        if not doc:
            return None
        await col.update_one({"_id": doc["_id"]}, {"$set": {"pinned": pinned}})
        return doc["proxy_id"]

    # --- Automatic Pruning of Dead Weight (archive, never hard-delete without archiving) ---

    async def prune_dead_weight(self) -> Dict[str, int]:
        cutoff_seconds = Config.PRUNE_DISABLED_AFTER_SECONDS
        result: Dict[str, int] = {}
        now = now_utc()
        for platform, col in self.cols.items():
            docs = await col.find({f"platform_status.{platform}.state": PlatformState.DISABLED}).to_list(length=5000)
            archived = 0
            for doc in docs:
                p_stat = (doc.get("platform_status") or {}).get(platform, {})
                last_checked = parse_dt(p_stat.get("last_checked_at"))
                if not last_checked or (now - last_checked).total_seconds() < cutoff_seconds:
                    continue
                archive_doc = dict(doc)
                archive_doc.pop("_id", None)
                archive_doc["archived_at"] = now
                archive_doc["archived_platform"] = platform
                try:
                    await self.archive.insert_one(archive_doc)
                    await col.delete_one({"_id": doc["_id"]})
                    archived += 1
                except Exception:
                    logger.exception("[PRUNE] Failed to archive proxy %s on %s", str(doc.get("proxy_id", ""))[:8], platform)
            result[platform] = archived
        return result

    # --- Cross-Platform Reuse Check (queue-only, never auto-verifies) ---

    async def enqueue_cross_platform_check(self, doc: Dict[str, Any], source_platform: str) -> None:
        if not Config.CROSS_PLATFORM_REUSE_ENABLED:
            return
        host = doc.get("host")
        port = safe_int(doc.get("port"))
        scheme = doc.get("scheme", "http")
        proxy_id = doc.get("proxy_id")
        if not host or not port or not proxy_id:
            return

        now = now_utc()
        for other in ALL_PLATFORMS:
            if other == source_platform:
                continue
            col = self.cols[other]
            existing = await col.find_one({"proxy_id": proxy_id})
            if existing:
                # Already known on this platform - just bump it to the front
                # of ITS OWN queue. It still has to pass ITS OWN validator
                # to ever be marked working there (Requirement #9).
                await col.update_one(
                    {"proxy_id": proxy_id},
                    {"$set": {f"platform_status.{other}.next_check_at": now}},
                )
                continue

            insert_doc = {
                "proxy_id": proxy_id,
                "proxy_url": doc.get("proxy_url"),
                "scheme": scheme,
                "host": host,
                "port": port,
                "username": doc.get("username"),
                "password": doc.get("password"),
                "source_country": doc.get("source_country"),
                "source_present": True,
                "requires_auth_missing": doc.get("requires_auth_missing", False),
                "enabled": True,
                "retired": False,
                "ever_working": False,
                "pinned": False,
                "quality_score": 0,
                "verified_country": doc.get("verified_country"),
                "country_name": doc.get("country_name"),
                "latency_ms": None,
                "first_seen_at": now,
                "last_seen_at": now,
                "last_tested_at": None,
                "lease_until": None,
                "platform_status": {other: self._default_platform_status(now)},
            }
            update_ops: Dict[str, Any] = {"$setOnInsert": insert_doc}
            if doc.get("source_ids"):
                update_ops["$addToSet"] = {"source_ids": doc["source_ids"][0]}
            try:
                await col.update_one({"proxy_id": proxy_id}, update_ops, upsert=True)
            except Exception:
                pass  # benign race with a concurrent insert; safe to ignore

    # --- Snapshot Export History ---

    async def save_export_snapshot(self, platform: str, proxy_ids: List[str]) -> Dict[str, Any]:
        prev = await self.export_snapshots.find_one({"platform": platform}, sort=[("created_at", DESCENDING)])
        prev_ids = set(prev.get("proxy_ids", [])) if prev else set()
        cur_ids = set(proxy_ids)
        added = list(cur_ids - prev_ids)
        removed = list(prev_ids - cur_ids)
        await self.export_snapshots.insert_one(
            {"platform": platform, "proxy_ids": list(cur_ids), "count": len(cur_ids), "created_at": now_utc()}
        )
        return {"added": len(added), "removed": len(removed), "total": len(cur_ids)}


# ============================================================================
# VALIDATORS & PLUGIN ARCHITECTURE
# ============================================================================

def modern_browser_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Requirement #3: realistic, current browser fingerprint for HTTP validators."""
    headers = {
        "User-Agent": Config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }
    if extra:
        headers.update(extra)
    return headers


class BaseValidator:
    """
    NOTE (Requirement #2): there is intentionally NO platform-wide backoff
    state on this class anymore. A validator instance is shared by every
    concurrent worker testing that platform, so any "pause the validator"
    flag here would freeze the ENTIRE platform for every proxy at once -
    exactly the bug we removed. Per-proxy cooldowns live in
    Database.record_platform_result() instead, keyed by proxy_id.
    """

    def __init__(self, platform: str, test_urls: Tuple[str, ...], timeout_seconds: int):
        self.platform = platform
        self.test_urls = test_urls
        self.timeout_seconds = timeout_seconds

    def pick_target_url(self) -> str:
        return random.choice(self.test_urls)

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
        target_url = self.pick_target_url()
        started = time.monotonic()
        proxy_url = entry.canonical
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        headers = modern_browser_headers()

        if entry.scheme.startswith("socks") and ProxyConnector is None:
            return {
                "ok": False,
                "category": FailureCategory.ENVIRONMENT_ERROR,
                "error": "aiohttp-socks is required for SOCKS validation",
            }

        try:
            if entry.scheme.startswith("socks"):
                connector = ProxyConnector.from_url(proxy_url)
                session_ctx = aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers)
            else:
                session_ctx = aiohttp.ClientSession(timeout=timeout, headers=headers)

            async with session_ctx as session:
                kwargs = {} if entry.scheme.startswith("socks") else {"proxy": proxy_url}
                async with session.get(target_url, allow_redirects=True, **kwargs) as resp:
                    duration = time.monotonic() - started
                    if resp.status == 429:
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


class TikTokValidator(BaseValidator):
    """
    Requirement #3: dedicated TikTok validator. Uses a modern browser
    fingerprint, follows redirects, and inspects the response BODY (not
    just the status code) for TikTok's soft-block / verification-wall
    pages, which frequently come back as a plain HTTP 200 rather than a
    hard error - a common source of false negatives/positives in naive
    status-code-only checks.
    """

    _CHALLENGE_MARKERS = (
        "verify to continue",
        "captcha",
        "punish_control",
        "/captcha/",
        "secsdk-captcha",
        "verify you are human",
    )

    def __init__(self):
        super().__init__("tiktok", Config.TIKTOK_TEST_URLS, Config.TIKTOK_TIMEOUT)

    async def test(self, entry: ProxyEntry) -> Dict[str, Any]:
        target_url = self.pick_target_url()
        started = time.monotonic()
        proxy_url = entry.canonical
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        headers = modern_browser_headers({"Referer": "https://www.tiktok.com/"})

        if entry.scheme.startswith("socks") and ProxyConnector is None:
            return {
                "ok": False,
                "category": FailureCategory.ENVIRONMENT_ERROR,
                "error": "aiohttp-socks is required for SOCKS validation",
            }

        try:
            if entry.scheme.startswith("socks"):
                connector = ProxyConnector.from_url(proxy_url)
                session_ctx = aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers)
            else:
                session_ctx = aiohttp.ClientSession(timeout=timeout, headers=headers)

            async with session_ctx as session:
                kwargs = {} if entry.scheme.startswith("socks") else {"proxy": proxy_url}
                async with session.get(target_url, allow_redirects=True, max_redirects=5, **kwargs) as resp:
                    duration = time.monotonic() - started
                    final_url = str(resp.url).lower()

                    if resp.status == 429:
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

                    body_sample = ""
                    if resp.status < 400:
                        try:
                            body_sample = (await resp.text(errors="replace"))[:20000].lower()
                        except Exception:
                            body_sample = ""

                    if "/verify" in final_url or any(marker in body_sample for marker in self._CHALLENGE_MARKERS):
                        return {
                            "ok": False,
                            "category": FailureCategory.RATE_LIMITED,
                            "error": "TikTok verification/captcha wall detected",
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
            "tiktok": TikTokValidator(),
        }

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

    async def resolve_ip_and_country(self, entry: ProxyEntry) -> Dict[str, Any]:
        if not Config.ENABLE_GEO_LOOKUP:
            return {}
        proxy_url = entry.canonical
        timeout = aiohttp.ClientTimeout(total=Config.GENERIC_TIMEOUT)
        try:
            ip = ""
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
            requires_auth_missing=proxy_doc.get("requires_auth_missing", False),
        )

        # Early failure detection (Requirement #16): a plain TCP dial to
        # the proxy port is far cheaper than a full platform request, so
        # dead proxies are rejected almost instantly without spending any
        # of the platform's bandwidth budget on a doomed HTTP/yt-dlp call.
        reachable = await self.tcp_connect_check(entry)
        if not reachable:
            return {
                "ok": False,
                "category": FailureCategory.CONNECTION_TIMEOUT,
                "error": "TCP connect to proxy port timed out / refused",
            }

        geo_data = {}
        if not (proxy_doc.get("verified_country") or proxy_doc.get("source_country")):
            geo_data = await self.resolve_ip_and_country(entry)

        validator = self.validators.get(platform)
        if not validator:
            return {"ok": False, "category": FailureCategory.ENVIRONMENT_ERROR, "error": f"No validator for {platform}"}

        res = await validator.test(entry)
        return {**geo_data, **res}


# ============================================================================
# SOURCE MANAGER & AUTO-DISCOVERY (now concurrent, non-blocking)
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
                raise RuntimeError("No usable proxy file found in GitHub tree.")
            raw_url = chosen["download_url"]
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
        except Exception:
            logger.warning("[SOURCE] Initial fetch failed for %s, trying fresh directory resolution...", source_id)
            try:
                fetch_url, fmt = await self.resolve_source_url(source, force_re_resolve=True)
                text, content_type, byte_count = await self.fetch(fetch_url)
            except Exception:
                logger.error("[SOURCE] Retry fetch also failed for %s", source_id)
                raise

        content_hash = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
        if source.get("last_content_hash") == content_hash:
            await self.db.record_source_state(source_id, content_hash=content_hash, item_count=0, success=True)
            return {"source_id": source_id, "unchanged": True, "added": 0}

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

        prev_ids = set(source.get("known_proxy_ids") or [])
        missing_ids = list(prev_ids - seen_ids)
        if missing_ids:
            await self.db.mark_missing_from_sources(missing_ids)

        await self.db.record_source_state(
            source_id,
            content_hash=content_hash,
            item_count=len(known_ids),
            known_proxy_ids=known_ids,
            success=True,
        )
        return {"source_id": source_id, "unchanged": False, "added": new_count, "total": len(known_ids)}

    async def run_discovery_pass(self) -> int:
        """
        Requirement #5/#9/#10: crawls known GitHub-tree sources for new
        proxy list FILES concurrently (bounded by
        DISCOVERY_FETCH_CONCURRENCY) instead of one at a time, so a slow
        or unreachable repo can't stall discovery of the rest.
        """
        sources = await self.db.get_sources(enabled_only=True)
        github_sources = [s for s in sources if self._is_github_repo_url(str(s.get("url", "")))]
        if not github_sources:
            return 0

        sem = asyncio.Semaphore(Config.DISCOVERY_FETCH_CONCURRENCY)
        discovered_total = 0
        lock = asyncio.Lock()

        async def process(src: Dict[str, Any]) -> None:
            nonlocal discovered_total
            url = str(src.get("url", ""))
            parsed = urlparse(url)
            parts = [unquote(x) for x in parsed.path.split("/") if x]
            if len(parts) < 4:
                return

            owner, repo = parts[0], parts[1]
            branch = parts[3] if len(parts) >= 4 and parts[2] in ("tree", "blob") else "main"
            path = "/".join(parts[4:-1]) if len(parts) >= 5 else ""

            async with sem:
                try:
                    entries = await self._list_github_directory(owner, repo, branch, path)
                except Exception:
                    return

            found_here = 0
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

                cand_id = hashlib.sha1(raw_download.encode()).hexdigest()[:16]
                exists = await self.db.get_source(cand_id)
                if not exists:
                    await self.db.upsert_source(
                        {
                            "source_id": cand_id,
                            "name": f"Auto: {repo}/{item.get('name')}",
                            "url": raw_download,
                            "enabled": True,
                            "discovered": True,
                            "priority": 50,
                            "fetch_interval": Config.SOURCE_REFRESH_SECONDS * 2,
                        },
                        only_if_missing=True,
                    )
                    found_here += 1

            if found_here:
                async with lock:
                    discovered_total += found_here

        await asyncio.gather(*(process(src) for src in github_sources))
        return discovered_total


# ============================================================================
# SCHEDULER & DISPATCHER (adaptive multi-worker pools per platform)
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
        self.pause_event.set()  # set = running; clear = paused

        # Global concurrency cap shared across ALL platform workers.
        self.semaphore = asyncio.Semaphore(Config.TEST_CONCURRENCY)
        self.active_tests = 0

        # Requirement #3/#11: instead of a single sequential dispatch loop
        # per platform, each platform runs a small POOL of worker
        # coroutines whose size is adjusted live based on backlog and
        # health, so the engine "immediately rotates to the next
        # available proxy" instead of stalling on one test at a time.
        self.worker_tasks: Dict[str, List[asyncio.Task]] = {p: [] for p in ALL_PLATFORMS}
        self.controller_task: Optional[asyncio.Task] = None
        self.periodic_task: Optional[asyncio.Task] = None
        self.discovery_task: Optional[asyncio.Task] = None
        self.prune_task: Optional[asyncio.Task] = None

        self.breakers: Dict[str, CircuitBreaker] = {p: CircuitBreaker(p) for p in ALL_PLATFORMS}
        self.bandwidth: Dict[str, BandwidthBudget] = {
            p: BandwidthBudget(Config.PER_PLATFORM_TEST_BUDGET, Config.BANDWIDTH_BUDGET_WINDOW_SECONDS)
            for p in ALL_PLATFORMS
        }
        # Throttle working proxy notifications to prevent Telegram FloodWait errors
        self._last_notification_time: Dict[str, float] = {p: 0.0 for p in ALL_PLATFORMS}

    async def start(self) -> None:
        self.running = True
        self.stop_event.clear()
        self.pause_event.set()

        for platform in ALL_PLATFORMS:
            self._spawn_worker(platform)

        self.controller_task = asyncio.create_task(self.concurrency_controller_loop(), name="scheduler-controller")
        self.periodic_task = asyncio.create_task(self.periodic_scheduler_loop(), name="scheduler-periodic")
        self.discovery_task = asyncio.create_task(self.discovery_scheduler_loop(), name="scheduler-discovery")
        self.prune_task = asyncio.create_task(self.prune_scheduler_loop(), name="scheduler-prune")
        logger.info("[SCHEDULER] Adaptive worker pools and scheduler loops initialized.")

    async def stop(self) -> None:
        self.running = False
        self.stop_event.set()
        self.pause_event.set()

        all_tasks: List[asyncio.Task] = []
        for tasks in self.worker_tasks.values():
            all_tasks.extend(tasks)
        for t in all_tasks:
            t.cancel()
        for t in (self.controller_task, self.periodic_task, self.discovery_task, self.prune_task):
            if t:
                t.cancel()

        await asyncio.gather(
            *all_tasks,
            self.controller_task,
            self.periodic_task,
            self.discovery_task,
            self.prune_task,
            return_exceptions=True,
        )
        logger.info("[SCHEDULER] All tasks successfully stopped.")

    # --- Adaptive worker pool management (Requirement #11) ---

    def _spawn_worker(self, platform: str) -> None:
        idx = len(self.worker_tasks[platform])
        t = asyncio.create_task(self._worker_loop(platform), name=f"worker-{platform}-{idx}")
        self.worker_tasks[platform].append(t)

    async def _compute_desired_workers(self, platform: str) -> int:
        breaker = self.breakers[platform]
        if breaker.state == "OPEN":
            return 0  # avoid wasting bandwidth hammering a route that's actively blocking us

        try:
            backlog = await self.db.count_due(platform)
        except Exception:
            backlog = 0

        if backlog <= 0:
            return 1  # keep one idle poller so newly-due proxies get picked up promptly

        fail_rate = breaker.fail_rate()
        desired = 1 + backlog // 15
        if fail_rate > 0.5:
            # Don't blindly throw more concurrency at a platform that's
            # mostly failing right now - back off instead of wasting
            # bandwidth (Requirement #11/#16).
            desired = max(1, desired // 2)
        return max(1, min(Config.MAX_WORKERS_PER_PLATFORM, desired))

    async def concurrency_controller_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(Config.CONTROLLER_INTERVAL_SECONDS)
                for platform in ALL_PLATFORMS:
                    desired = await self._compute_desired_workers(platform)
                    current = [t for t in self.worker_tasks[platform] if not t.done()]
                    self.worker_tasks[platform] = current

                    if len(current) < desired:
                        for _ in range(desired - len(current)):
                            self._spawn_worker(platform)
                    elif len(current) > desired:
                        extras = current[desired:]
                        for t in extras:
                            t.cancel()
                        self.worker_tasks[platform] = current[:desired]
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[SCHEDULER] Error in concurrency controller loop")

    async def _worker_loop(self, platform: str) -> None:
        """One concurrent validation worker for a single platform. Several
        of these run at once per platform (see concurrency_controller_loop),
        each independently claiming and testing proxies, so a slow test on
        one proxy never blocks the others from rotating through the pool
        (Requirement #3)."""
        breaker = self.breakers[platform]
        budget = self.bandwidth[platform]

        while not self.stop_event.is_set():
            await self.pause_event.wait()

            enabled = await self.db.get_config(f"{platform}_validation_enabled", True)
            if not enabled:
                await asyncio.sleep(5)
                continue

            if not breaker.allow_request():
                await asyncio.sleep(5)
                continue

            if not budget.try_consume():
                await asyncio.sleep(min(30, max(1, budget.seconds_until_reset())))
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

                    tripped = breaker.record(bool(res.get("ok")))
                    if tripped:
                        await self.notify_func(
                            platform,
                            (
                                f"🚨 CIRCUIT BREAKER TRIPPED — {platform.title()}\n"
                                f"Mass failure detected across recent tests — pausing {platform.title()} "
                                f"testing for ~{Config.CIRCUIT_BREAKER_COOLDOWN_SECONDS // 60} minutes. "
                                f"This is more likely a network-level block than individually bad proxies."
                            ),
                        )

                    await self._handle_platform_notification(platform, updated, meta)

                    if meta.get("now_working"):
                        await self.db.enqueue_cross_platform_check(updated, platform)

                except asyncio.CancelledError:
                    await self.db.release_lease(platform, doc["proxy_id"])
                    raise
                except Exception:
                    logger.exception("[DISPATCH] Test task failed for %s on %s", str(doc.get("proxy_id", ""))[:8], platform)
                    await self.db.release_lease(platform, doc["proxy_id"])
                finally:
                    self.active_tests -= 1

    async def _handle_platform_notification(self, platform: str, doc: Dict[str, Any], meta: Dict[str, Any]) -> None:
        if not meta.get("now_working"):
            return

        # Throttle notifications to prevent Telegram FloodWait errors (max 1 per 2 seconds per platform)
        now = time.monotonic()
        if now - self._last_notification_time[platform] < 2.0:
            return
        self._last_notification_time[platform] = now

        proxy_str = mask_proxy_string(doc.get("proxy_url", ""))
        country = doc.get("verified_country") or doc.get("source_country") or "UNKNOWN"
        source_name = doc.get("source_ids", ["manual"])[0] if doc.get("source_ids") else "manual"
        t_str = now_utc().strftime("%Y-%m-%d %H:%M UTC")
        score = doc.get("quality_score", 0)

        if meta.get("recovered"):
            downtime = meta.get("downtime_hours", 0.0)
            status_line = f"recovered after quarantine ({downtime}h downtime)"
        else:
            status_line = "first-time verified"

        lines = [
            f"✅ WORKING — {platform.title()}",
            "```",
            f"proxy: {doc.get('proxy_url')}",
            f"country: {country}",
            f"source: {source_name}",
            f"status: {status_line}",
            f"quality_score: {score}/100",
            f"checked_at: {t_str}",
            "```",
        ]
        await self.notify_func(platform, "\n".join(lines))

    # --- Manual all-platform priority checking flow (unchanged behavior; global TXT upload) ---

    async def manual_priority_check(self, proxies_raw: List[str]) -> str:
        self.pause_event.clear()
        logger.info("[PRIORITY] Background dequeuing paused for manual priority check (%s proxies).", len(proxies_raw))
        await asyncio.sleep(0.2)

        sem = asyncio.Semaphore(Config.ADHOC_TEST_CONCURRENCY)
        results_summary: List[Optional[str]] = [None] * len(proxies_raw)

        async def handle_one(idx: int, raw: str) -> None:
            entry = parse_proxy_string(raw)
            if not entry:
                results_summary[idx] = f"❌ `{short_error(raw, 60)}` — Invalid proxy string format"
                return

            await self.db.upsert_proxy_to_platforms(entry, country=entry.source_country)

            async def test_plat(p: str):
                doc = await self.db.get_col(p).find_one({"proxy_id": entry.proxy_id})
                res = await self.engine.validate(doc, p)
                updated, meta = await self.db.record_platform_result(p, entry.proxy_id, res)
                await self._handle_platform_notification(p, updated, meta)
                if meta.get("now_working"):
                    await self.db.enqueue_cross_platform_check(updated, p)
                return p, res

            plat_results = await asyncio.gather(*(test_plat(p) for p in ALL_PLATFORMS), return_exceptions=True)

            working_on: List[str] = []
            failures: List[str] = []
            for r in plat_results:
                if isinstance(r, tuple):
                    pname, res = r
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
                msg = f"❌ `{proxy_masked}` — Not working on any platform:\n  " + "\n  ".join(failures)
            results_summary[idx] = msg

        async def bounded(idx: int, raw: str) -> None:
            async with sem:
                await handle_one(idx, raw)

        try:
            await asyncio.gather(*(bounded(i, raw) for i, raw in enumerate(proxies_raw)))
        finally:
            self.pause_event.set()
            logger.info("[PRIORITY] Manual priority check completed. Background dequeuing resumed.")

        return "\n\n".join(r for r in results_summary if r) or "No valid proxies parsed."

    # --- Requirement #6: per-platform "Add File" fast-track flow ---

    async def platform_priority_check(
        self,
        platform: str,
        proxies_raw: List[str],
        progress_cb: Optional[Callable[[Dict[str, int], List[str], int], Any]] = None,
    ) -> str:
        """
        Tests a batch of proxies against ONE platform only, streaming
        progress via `progress_cb(counts, recent_working_lines, total)`.
        Used by the per-platform "📥 Add File" button.
        """
        self.pause_event.clear()
        total = len(proxies_raw)
        counts = {"working": 0, "failed": 0, "invalid": 0, "done": 0}
        detail_lines: List[str] = []
        sem = asyncio.Semaphore(Config.ADHOC_TEST_CONCURRENCY)
        lock = asyncio.Lock()
        last_update = time.monotonic()

        async def maybe_emit(force: bool = False) -> None:
            nonlocal last_update
            if not progress_cb:
                return
            now_m = time.monotonic()
            if force or now_m - last_update > 2.0:
                last_update = now_m
                try:
                    await progress_cb(dict(counts), list(detail_lines[-8:]), total)
                except Exception:
                    logger.exception("[ADDFILE] progress callback failed")

        async def handle_one(raw: str) -> None:
            entry = parse_proxy_string(raw)
            if not entry:
                async with lock:
                    counts["invalid"] += 1
                    counts["done"] += 1
                await maybe_emit()
                return

            await self.db.upsert_proxy_to_platforms(entry, country=entry.source_country)
            async with sem:
                doc = await self.db.get_col(platform).find_one({"proxy_id": entry.proxy_id})
                res = await self.engine.validate(doc, platform)

            updated, meta = await self.db.record_platform_result(platform, entry.proxy_id, res)
            await self._handle_platform_notification(platform, updated, meta)
            if meta.get("now_working"):
                await self.db.enqueue_cross_platform_check(updated, platform)

            async with lock:
                counts["done"] += 1
                if res.get("ok"):
                    counts["working"] += 1
                    latency = updated.get("latency_ms") or 0
                    score = updated.get("quality_score", 0)
                    detail_lines.append(
                        f"✅ {mask_proxy_string(entry.canonical)} — {safe_float(latency):.0f}ms — score {score}/100"
                    )
                else:
                    counts["failed"] += 1
            await maybe_emit()

        try:
            await asyncio.gather(*(handle_one(raw) for raw in proxies_raw))
            await maybe_emit(force=True)
        finally:
            self.pause_event.set()

        return (
            f"📥 Add File — {platform.title()} fast-track complete\n"
            f"Total: {total} | ✅ Working: {counts['working']} | "
            f"❌ Failed: {counts['failed']} | ⚠️ Invalid: {counts['invalid']}"
        )

    # --- Periodic maintenance & source refresh loops (now concurrent) ---

    async def periodic_scheduler_loop(self) -> None:
        first_run = True
        sem = asyncio.Semaphore(Config.SOURCE_FETCH_CONCURRENCY)

        while not self.stop_event.is_set():
            try:
                if first_run:
                    first_run = False
                else:
                    await asyncio.sleep(15)  # Short delay between cycles to prevent CPU overload

                sources = await self.db.get_sources(enabled_only=True)
                due_sources = []
                for src in sources:
                    interval = safe_int(src.get("fetch_interval"), Config.SOURCE_REFRESH_SECONDS)
                    last_checked = parse_dt(src.get("last_checked_at"))
                    if last_checked and (now_utc() - last_checked).total_seconds() < interval:
                        continue
                    due_sources.append(src)

                async def process_source(src: Dict[str, Any]) -> None:
                    async with sem:
                        try:
                            res = await self.sources.import_source(src)
                            if not res.get("unchanged"):
                                logger.info(
                                    "[SOURCE] Ingested %s (New: %s, Total: %s)",
                                    src["name"], res.get("added"), res.get("total"),
                                )
                        except Exception as e:
                            logger.error("[SOURCE] Ingestion error on %s: %s", src.get("name"), short_error(e))

                if due_sources:
                    # Requirement #5/#10: many sources refreshed concurrently,
                    # in the background, without blocking validation workers.
                    await asyncio.gather(*(process_source(s) for s in due_sources))

                await self.db.release_expired_leases()
                await self.db.retire_orphans()

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[SCHEDULER] Error in periodic scheduler loop")
                await asyncio.sleep(5)

    async def discovery_scheduler_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(15)  # Short delay between cycles to prevent CPU overload
                added = await self.sources.run_discovery_pass()
                if added > 0:
                    logger.info("[DISCOVERY] Auto-discovered %s new proxy sources.", added)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[DISCOVERY] Error in auto-discovery loop")

    async def prune_scheduler_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(15)  # Short delay between cycles to prevent CPU overload
                result = await self.db.prune_dead_weight()
                total = sum(result.values())
                if total > 0:
                    logger.info("[PRUNE] Archived dead-weight proxies: %s", result)
                    await self.notify_func(
                        "youtube",
                        f"🗄 Pruned {total} long-dead proxies into the archive collection: {result}",
                    )
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[PRUNE] Error in pruning loop")


# ============================================================================
# REPORTING ENGINE
# ============================================================================

class ReportEngine:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.scheduler: Optional[WorkerScheduler] = None  # wired in later by Application

    async def export_working(self, platform: str) -> Tuple[bytes, Dict[str, Any]]:
        """Latency/quality-ranked export + geo-diversity guard + snapshot diffing."""
        col = self.db.get_col(platform)
        cursor = col.find(
            {f"platform_status.{platform}.state": PlatformState.WORKING, "enabled": True}
        ).sort([("quality_score", DESCENDING), ("latency_ms", ASCENDING)])

        docs = await cursor.to_list(length=10000)
        if not docs:
            return b"# No active working proxies for this platform\n", {"added": 0, "removed": 0, "total": 0}

        by_country = defaultdict(list)
        for d in docs:
            c = d.get("verified_country") or d.get("source_country") or "UNKNOWN"
            by_country[c].append(d.get("proxy_url"))

        interleaved: List[str] = []
        max_len = max(len(v) for v in by_country.values())
        for i in range(max_len):
            for c_list in by_country.values():
                if i < len(c_list):
                    interleaved.append(c_list[i])

        diff = await self.db.save_export_snapshot(platform, [d.get("proxy_id") for d in docs])
        return "\n".join(interleaved).encode("utf-8"), diff

    async def generate_daily_digest(self) -> str:
        lines = ["📊 DAILY PROXY WORKER DIGEST", f"Date: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}", ""]
        for p in ALL_PLATFORMS:
            stats = await self.db.get_platform_stats(p)
            avg_q = await self.db.get_average_quality(p)
            pinned = await self.db.count_pinned(p)
            archived = await self.db.count_archived(p)
            breaker_state = self.scheduler.breakers[p].state if self.scheduler else "N/A"
            workers = len(self.scheduler.worker_tasks[p]) if self.scheduler else 0
            lines.append(f"• **{p.title()}**:")
            lines.append(f"   🟢 Working: {stats['working']}")
            lines.append(f"   🟠 Quarantined: {stats['quarantined']}")
            lines.append(f"   🔴 Disabled: {stats['disabled']}")
            lines.append(f"   🌐 Total Pool: {stats['total']}")
            lines.append(f"   ⭐ Avg Quality Score: {avg_q}/100")
            lines.append(
                f"   📌 Pinned: {pinned}   🗄 Archived: {archived}   🚦 Breaker: {breaker_state}   👷 Workers: {workers}"
            )
        return "\n".join(lines)


# ============================================================================
# TELEGRAM ADMIN UI (migration-free; per-platform Add File support)
# ============================================================================

class TelegramAdminUI:
    def __init__(
        self,
        db: Database,
        scheduler: Optional[WorkerScheduler],
        reports: ReportEngine,
    ) -> None:
        self.db = db
        self.scheduler = scheduler
        self.reports = reports
        self.bot: Optional[Client] = None
        self.log_channels = {
            "youtube": Config.YOUTUBE_LOG_CHANNEL_ID,
            "instagram": Config.INSTAGRAM_LOG_CHANNEL_ID,
            "tiktok": Config.TIKTOK_LOG_CHANNEL_ID,
        }
        # Requirement #6: user_id -> platform awaiting a fast-tracked file.
        self.pending_file_platform: Dict[int, str] = {}

    async def notify_platform(self, platform: str, text: str) -> None:
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
        except (KeyError, ValueError):
            try:
                url = f"https://api.telegram.org/bot{Config.BOT_TOKEN}/sendMessage"
                payload = {"chat_id": target_channel, "text": text, "parse_mode": "Markdown"}
                async with aiohttp.ClientSession() as session:
                    await session.post(url, json=payload)
            except Exception:
                logger.exception("[TG] HTTP fallback also failed for %s", platform)
        except Exception:
            logger.exception("[TG] Failed to dispatch alert to %s log channel", platform)

    def is_authorized(self, user_id: int) -> bool:
        return user_id == Config.OWNER_ID

    # --- Dashboards & Keyboards ---

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
            ]
        )

    @staticmethod
    def platform_subpanel_markup(platform: str, enabled: bool) -> InlineKeyboardMarkup:
        toggle_text = "⏸ Disable Platform" if enabled else "▶️ Enable Platform"
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📥 Export Working (Best Quality First)", callback_data=f"exp_{platform}")],
                [InlineKeyboardButton("📥 Add File", callback_data=f"addfile_{platform}")],
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
            "proxy_worker_v5",
            bot_token=Config.BOT_TOKEN,
            api_id=env_int("API_ID", 12345),
            api_hash=os.getenv("API_HASH", "placeholder").strip(),
            in_memory=True,
        )

        @self.bot.on_message(filters.command("start") & filters.private)
        async def _cmd_start(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            await message.reply_text(
                "🤖 **Proxy Worker Bot v5 (Multi-Platform)**\nSelect a platform panel below:",
                reply_markup=self.main_dashboard_markup(),
            )

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
            await wait_msg.edit_text(res[:4000])

        @self.bot.on_message(filters.command("digest") & filters.private)
        async def _cmd_digest(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            digest = await self.reports.generate_daily_digest()
            await message.reply_text(digest)

        @self.bot.on_message(filters.command("pin") & filters.private)
        async def _cmd_pin(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            parts = message.text.split()
            if len(parts) < 3 or parts[1].lower() not in ALL_PLATFORMS:
                await message.reply_text("Usage: `/pin <youtube|instagram|tiktok> <proxy_id_prefix>`")
                return
            platform = parts[1].lower()
            result_id = await self.db.set_pinned(platform, parts[2], True)
            if result_id:
                await message.reply_text(f"📌 Pinned `{result_id[:16]}...` for {platform.title()} — tested first from now on.")
            else:
                await message.reply_text("No matching proxy found for that ID prefix.")

        @self.bot.on_message(filters.command("unpin") & filters.private)
        async def _cmd_unpin(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return
            parts = message.text.split()
            if len(parts) < 3 or parts[1].lower() not in ALL_PLATFORMS:
                await message.reply_text("Usage: `/unpin <youtube|instagram|tiktok> <proxy_id_prefix>`")
                return
            platform = parts[1].lower()
            result_id = await self.db.set_pinned(platform, parts[2], False)
            if result_id:
                await message.reply_text(f"Unpinned `{result_id[:16]}...` for {platform.title()}.")
            else:
                await message.reply_text("No matching proxy found for that ID prefix.")

        @self.bot.on_message(filters.document & filters.private)
        async def _on_document_upload(_, message: Message):
            if not self.is_authorized(message.from_user.id):
                return

            user_id = message.from_user.id
            pending_platform = self.pending_file_platform.pop(user_id, None)

            wait_msg = await message.reply_text("📥 Downloading & analyzing proxy list...")
            fpath = await message.download()
            try:
                with open(fpath, "r", encoding="utf-8", errors="replace") as fh:
                    raw_text = fh.read()
            finally:
                if os.path.exists(fpath):
                    os.remove(fpath)

            # Requirement #12: format-agnostic - content is sniffed
            # regardless of the file's extension.
            candidates = sniff_and_parse(raw_text)
            proxies = [c.raw for c in candidates]
            if not proxies:
                await wait_msg.edit_text("❌ No valid proxy entries could be detected in that file.")
                return

            if pending_platform:
                # Requirement #6: platform-specific fast-track with
                # live-edited progress.
                await wait_msg.edit_text(
                    f"📥 Fast-tracking {len(proxies)} proxies for {pending_platform.title()}...\nStarting..."
                )

                async def progress_cb(counts: Dict[str, int], recent_lines: List[str], total: int) -> None:
                    text = (
                        f"📥 Testing for {pending_platform.title()}: {counts['done']}/{total}\n"
                        f"✅ Working: {counts['working']}  ❌ Failed: {counts['failed']}  "
                        f"⚠️ Invalid: {counts['invalid']}\n\n" + "\n".join(recent_lines)
                    )
                    try:
                        await wait_msg.edit_text(text[:4000])
                    except Exception:
                        pass

                summary = await self.scheduler.platform_priority_check(pending_platform, proxies, progress_cb)
                await wait_msg.edit_text(summary)
            else:
                # Global upload: unchanged behavior, tests against all
                # three platforms.
                res = await self.scheduler.manual_priority_check(proxies)
                await wait_msg.edit_text(f"📁 Ingestion Summary ({len(proxies)} parsed):\n\n{res[:3800]}")

        @self.bot.on_callback_query()
        async def _on_callback(_, query: CallbackQuery):
            if not self.is_authorized(query.from_user.id):
                await query.answer("Unauthorized", show_alert=True)
                return

            data = query.data
            if data == "panel_main":
                await query.message.edit_text(
                    "🤖 **Proxy Worker Bot v5 (Multi-Platform)**\nSelect a platform panel below:",
                    reply_markup=self.main_dashboard_markup(),
                )

            elif data.startswith("panel_"):
                p = data.split("_")[1]
                stats = await self.db.get_platform_stats(p)
                avg_q = await self.db.get_average_quality(p)
                enabled = await self.db.get_config(f"{p}_validation_enabled", True)
                status_str = "🟢 Active" if enabled else "⏸ Paused"

                text = (
                    f"**[{p.title()} Panel]** — Status: {status_str}\n\n"
                    f"🟢 Working: `{stats['working']}`\n"
                    f"🟠 Quarantined: `{stats['quarantined']}`\n"
                    f"🔴 Disabled: `{stats['disabled']}`\n"
                    f"🌐 Total Registered: `{stats['total']}`\n"
                    f"⭐ Ever Validated Working: `{stats['ever_working']}`\n"
                    f"📈 Avg Quality Score: `{avg_q}/100`"
                )
                await query.message.edit_text(text, reply_markup=self.platform_subpanel_markup(p, enabled))

            elif data.startswith("addfile_"):
                p = data.split("_", 1)[1]
                self.pending_file_platform[query.from_user.id] = p
                await query.answer(f"Send a proxy file for {p.title()} now.")
                await query.message.reply_text(
                    f"📥 Send a proxy list file for **{p.title()}** now (any common format — .txt/.csv/.json, "
                    f"or even no extension). It will be fast-tracked and tested only against {p.title()}, "
                    f"with live progress shown here."
                )

            elif data.startswith("exp_"):
                p = data.split("_")[1]
                await query.answer("Generating export...")
                export_bytes, diff = await self.reports.export_working(p)
                t_path = f"/tmp/{p}_working_{int(time.time())}.txt"
                with open(t_path, "wb") as fh:
                    fh.write(export_bytes)
                caption = (
                    f"Verified {p.title()} Working Proxies (Best Quality First, Geo-Distributed)\n"
                    f"Δ vs last export: +{diff['added']} / -{diff['removed']} (total {diff['total']})"
                )
                await query.message.reply_document(t_path, caption=caption)
                if os.path.exists(t_path):
                    os.remove(t_path)

            elif data.startswith("toggle_"):
                p = data.split("_")[1]
                cur = await self.db.get_config(f"{p}_validation_enabled", True)
                await self.db.set_config(f"{p}_validation_enabled", not cur)
                await query.answer(f"{p.title()} validation toggled.")
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
                col = self.db.get_col(p)
                await col.update_many(
                    {f"platform_status.{p}.state": PlatformState.WORKING},
                    {"$set": {f"platform_status.{p}.next_check_at": now_utc()}},
                )
                await query.message.reply_text(f"♻️ Immediate revalidation scheduled for all active {p.title()} proxies.")

            elif data == "btn_digest":
                digest = await self.reports.generate_daily_digest()
                await query.message.edit_text(digest, reply_markup=self.main_dashboard_markup())

            elif data == "btn_sources":
                sources = await self.db.get_sources()
                lines = [f"📁 Configured Sources ({len(sources)}):"]
                for s in sources[:20]:
                    status = "🟢" if s.get("enabled") else "⏸"
                    disc = " [Auto]" if s.get("discovered") else ""
                    lines.append(f"{status} {s.get('name')}{disc} (Yield: {s.get('yield_working_count', 0)})")
                await query.message.edit_text("\n".join(lines), reply_markup=self.main_dashboard_markup())

            elif data == "btn_manual_prompt":
                await query.message.reply_text(
                    "Send `/addproxy <url>` or paste a block of proxies directly into chat."
                )

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
        return web.json_response({"service": "proxy-worker-bot-v5", "status": "running"})

    async def handle_health(self, _) -> web.Response:
        db_ok = await self.db.ping()
        status = 200 if db_ok else 503
        stats = {}
        for p in ALL_PLATFORMS:
            stats[p] = await self.db.get_platform_stats(p)
            stats[p]["breaker_state"] = self.scheduler.breakers[p].state
            stats[p]["archived"] = await self.db.count_archived(p)
            stats[p]["active_workers"] = len([t for t in self.scheduler.worker_tasks[p] if not t.done()])

        return web.json_response(
            {
                "status": "ok" if db_ok else "degraded",
                "mongo": db_ok,
                "active_tests": self.scheduler.active_tests,
                "platform_stats": stats,
                "active_collections": Config.COLLECTION_NAMES,
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
        self.reports.scheduler = self.scheduler
        self.health_server = HealthServer(self.db, self.scheduler)

    async def start(self) -> None:
        Config.validate()
        await self.db.connect()
        # Release any stale leases from previous runs
        await self.db.release_expired_leases()
        await self.sources.start()
        await self.admin_ui.setup()
        await self.admin_ui.start()
        await self.scheduler.start()
        await self.health_server.start()

        start_msg = (
            "🚀 **Proxy Worker Bot v5 Online**\n"
            "• Platforms: YouTube, Instagram, TikTok (fully independent state machines)\n"
            f"• Active collections: {', '.join(Config.COLLECTION_NAMES.values())}\n"
            "• Legacy migration system fully removed\n"
            "• Per-proxy-only 429 cooldowns — no platform-wide freezes\n"
            "• Adaptive multi-worker validation pools per platform\n"
            "• Quality-ranked claim_proxy() selection + <=2h non-destructive requeue\n"
            "• Per-platform \"Add File\" fast-track with live progress"
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
