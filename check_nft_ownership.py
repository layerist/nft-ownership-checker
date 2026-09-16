#!/usr/bin/env python3
"""
Robust high-throughput ERC-721 ownership checker (JSON-RPC batch edition) v4.

Checks whether every wallet owns at least one token in any supplied ERC-721
contract by calling balanceOf(address) through batched eth_call requests.

Major properties:
- Multiple RPC endpoints with chain-id validation, batch-capability probing,
  latency/failure scoring, real cooldown enforcement, optional per-node
  concurrency caps, Retry-After support, and optional lag filtering.
- Thread-local requests.Session objects with keep-alive and explicit global
  cleanup at shutdown.
- Selective JSON-RPC batch retries: only missing/retryable calls are resent;
  successful and deterministic-error items are preserved.
- Automatic batch splitting when providers reject oversized batches.
- Incomplete checks are never converted to confirmed false unless explicitly
  requested with --allow-partial-false.
- Contract bytecode validation is fail-open: uncertain contracts remain in the
  scan rather than being silently dropped.
- Resume safety fingerprint prevents reusing results from a different chain,
  contract set, block tag, or partial-false policy.
- Dedicated buffered CSV writer, bounded in-flight work, crash attribution,
  graceful interruption, and optional fsync durability.
- Failed CSV is reset on normal resume because failed rows are retried; with
  --resume-failed it is retained and treated as completed work.

Default input files:
- input_addresses.txt
- nft_contracts.txt

Default output files:
- nft_owners.csv
- nft_owners_failed.csv
- nft_owners.csv.state.json

Dependencies:
    python -m pip install requests

Optional:
    python -m pip install web3   # enables EIP-55 checksum validation/output
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import logging
import os
import queue
import random
import signal
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence
from urllib.parse import urlsplit
from concurrent.futures import CancelledError, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
try:
    from web3 import Web3  # type: ignore
except ImportError:  # web3 is optional; raw JSON-RPC does not require it
    Web3 = None  # type: ignore[assignment]


VERSION = "4.0"
BALANCE_OF_SELECTOR = "70a08231"
WRITE_SENTINEL = object()
STATE_SCHEMA = 1
RETRYABLE_HTTP_STATUSES = {408, 425, 429, 500, 502, 503, 504}

STOP_EVENT = threading.Event()
THREAD_LOCAL = threading.local()
SESSION_REGISTRY: set[requests.Session] = set()
SESSION_REGISTRY_LOCK = threading.Lock()


# ==========================================================
# CONFIG / DATA TYPES
# ==========================================================


@dataclass(frozen=True)
class Config:
    rpc_urls: tuple[str, ...]
    input_file: Path
    contracts_file: Path
    output_file: Path
    failed_file: Path
    state_file: Path
    log_file: Path
    log_level: str
    threads: int
    max_inflight: int
    max_retries: int
    health_retries: int
    base_delay: float
    max_delay: float
    max_retry_after: float
    request_timeout: float
    connect_timeout: float
    pool_connections: int
    pool_maxsize: int
    rpc_concurrency_per_node: int
    contract_batch_size: int
    writer_batch_size: int
    writer_flush_seconds: float
    progress_every: int
    skip_contract_validation: bool
    skip_batch_probe: bool
    expected_chain_id: Optional[int]
    max_rpc_lag_blocks: int
    block_tag: str
    allow_partial_false: bool
    resume_failed: bool
    fresh: bool
    force_resume_mismatch: bool
    fsync_writes: bool
    user_agent: str


@dataclass(frozen=True)
class CheckResult:
    address: str
    owns_nft: Optional[bool]
    checked_contracts: int
    failed_contracts: int
    total_contracts: int
    error: str = ""


@dataclass
class Stats:
    confirmed: int = 0
    owners: int = 0
    uncertain: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, result: CheckResult) -> tuple[int, int, int, int]:
        with self.lock:
            if result.owns_nft is None:
                self.uncertain += 1
            else:
                self.confirmed += 1
                if result.owns_nft:
                    self.owners += 1
            done = self.confirmed + self.uncertain
            return done, self.confirmed, self.owners, self.uncertain


@dataclass
class RpcMetrics:
    http_requests: int = 0
    rpc_calls_sent: int = 0
    retried_rpc_calls: int = 0
    throttle_events: int = 0
    request_failures: int = 0
    missing_responses: int = 0
    retryable_rpc_errors: int = 0
    batch_splits: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add_http_request(self, rpc_calls: int, *, retry: bool) -> None:
        with self.lock:
            self.http_requests += 1
            self.rpc_calls_sent += rpc_calls
            if retry:
                self.retried_rpc_calls += rpc_calls

    def add_failure(self, *, throttled: bool) -> None:
        with self.lock:
            self.request_failures += 1
            if throttled:
                self.throttle_events += 1

    def add_semantic_retry(self, *, missing: int, retryable_errors: int) -> None:
        with self.lock:
            self.missing_responses += missing
            self.retryable_rpc_errors += retryable_errors

    def add_split(self) -> None:
        with self.lock:
            self.batch_splits += 1

    def reset(self) -> None:
        with self.lock:
            self.http_requests = 0
            self.rpc_calls_sent = 0
            self.retried_rpc_calls = 0
            self.throttle_events = 0
            self.request_failures = 0
            self.missing_responses = 0
            self.retryable_rpc_errors = 0
            self.batch_splits = 0

    def snapshot(self) -> dict[str, int]:
        with self.lock:
            return {
                "http_requests": self.http_requests,
                "rpc_calls_sent": self.rpc_calls_sent,
                "retried_rpc_calls": self.retried_rpc_calls,
                "throttle_events": self.throttle_events,
                "request_failures": self.request_failures,
                "missing_responses": self.missing_responses,
                "retryable_rpc_errors": self.retryable_rpc_errors,
                "batch_splits": self.batch_splits,
            }


@dataclass
class RpcNode:
    url: str
    max_concurrency: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    cooldown_until: float = 0.0
    failures: int = 0
    latency_ema: float = 0.30
    consecutive_throttles: int = 0
    disabled_reason: str = ""
    chain_id: Optional[int] = None
    block_number: Optional[int] = None
    inflight: int = 0

    def is_active(self) -> bool:
        with self.lock:
            return not self.disabled_reason

    def score(self, now: float) -> float:
        with self.lock:
            if self.disabled_reason:
                return float("inf")
            if self.cooldown_until > now:
                return float("inf")
            if self.max_concurrency > 0 and self.inflight >= self.max_concurrency:
                return float("inf")
            load_penalty = 1.0 + 0.20 * self.inflight
            return (
                self.latency_ema * load_penalty
                + self.failures * 0.30
                + self.consecutive_throttles * 0.75
            )

    def try_acquire(self, now: float) -> bool:
        with self.lock:
            if self.disabled_reason:
                return False
            if self.cooldown_until > now:
                return False
            if self.max_concurrency > 0 and self.inflight >= self.max_concurrency:
                return False
            self.inflight += 1
            return True

    def release(self) -> None:
        with self.lock:
            self.inflight = max(0, self.inflight - 1)

    def next_ready_delay(self, now: float) -> float:
        with self.lock:
            if self.disabled_reason:
                return float("inf")
            if self.cooldown_until > now:
                return self.cooldown_until - now
            if self.max_concurrency > 0 and self.inflight >= self.max_concurrency:
                return 0.05
            return 0.0


class StopRequested(RuntimeError):
    pass


class RpcRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        throttled: bool = False,
        retry_after: Optional[float] = None,
        batch_too_large: bool = False,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.throttled = throttled
        self.retry_after = retry_after
        self.batch_too_large = batch_too_large


class RpcPool:
    def __init__(self, urls: Sequence[str], *, max_concurrency_per_node: int) -> None:
        if not urls:
            raise ValueError("RPC URL list is empty")
        self.nodes = [
            RpcNode(url=url, max_concurrency=max(0, max_concurrency_per_node))
            for url in urls
        ]
        self.metrics = RpcMetrics()
        self._rr = itertools.count()
        self._rr_lock = threading.Lock()

    def active_nodes(self) -> list[RpcNode]:
        return [node for node in self.nodes if node.is_active()]

    @contextmanager
    def lease(self) -> Iterator[RpcNode]:
        while True:
            if STOP_EVENT.is_set():
                raise StopRequested("stopped")

            active = self.active_nodes()
            if not active:
                reasons = "; ".join(
                    f"{redact_url(node.url)}: {node.disabled_reason or 'unavailable'}"
                    for node in self.nodes
                )
                raise RuntimeError(f"No active RPC nodes: {reasons}")

            now = time.monotonic()
            with self._rr_lock:
                offset = next(self._rr) % len(active)
            rotated = active[offset:] + active[:offset]
            candidates = sorted(rotated, key=lambda node: node.score(now))

            for node in candidates:
                if node.try_acquire(now):
                    try:
                        yield node
                    finally:
                        node.release()
                    return

            delays = [node.next_ready_delay(now) for node in active]
            finite_delays = [delay for delay in delays if delay != float("inf")]
            sleep_for = min(finite_delays) if finite_delays else 0.10
            STOP_EVENT.wait(min(max(sleep_for, 0.02), 0.50))

    @staticmethod
    def mark_success(node: RpcNode, latency: float) -> None:
        with node.lock:
            # Do not clear cooldown_until here: another concurrent request may
            # have just throttled this same node. Let that penalty expire
            # naturally instead of allowing a racing success to erase it.
            node.failures = max(0, node.failures - 1)
            node.consecutive_throttles = max(0, node.consecutive_throttles - 1)
            node.latency_ema = node.latency_ema * 0.85 + latency * 0.15

    @staticmethod
    def mark_failure(
        cfg: Config,
        node: RpcNode,
        *,
        throttled: bool,
        retry_after: Optional[float] = None,
    ) -> None:
        now = time.monotonic()
        with node.lock:
            node.failures = min(50, node.failures + 1)
            if throttled:
                node.consecutive_throttles = min(20, node.consecutive_throttles + 1)
                exponent = min(8, max(0, node.consecutive_throttles - 1))
                base = max(0.50, cfg.base_delay)
                cooldown = min(cfg.max_delay, base * (2**exponent))
            else:
                exponent = min(8, max(0, node.failures - 1))
                base = max(0.05, cfg.base_delay)
                cooldown = min(cfg.max_delay, base * (1.55**exponent))

            cooldown *= random.uniform(0.85, 1.15)
            if retry_after is not None:
                cooldown = max(cooldown, retry_after)
            node.cooldown_until = max(node.cooldown_until, now + cooldown)

    @staticmethod
    def disable(node: RpcNode, reason: str) -> None:
        with node.lock:
            node.disabled_reason = reason


RPC_POOL: Optional[RpcPool] = None


# ==========================================================
# LOGGING / SIGNALS
# ==========================================================


def setup_logging(log_file: Path, level: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def install_signal_handlers() -> None:
    def handler(signum: int, _frame: object) -> None:
        if not STOP_EVENT.is_set():
            logging.warning("Signal %s received; stopping cleanly...", signum)
            STOP_EVENT.set()

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handler)


# ==========================================================
# HELPERS
# ==========================================================


def require_rpc_pool() -> RpcPool:
    if RPC_POOL is None:
        raise RuntimeError("RPC pool is not initialized")
    return RPC_POOL


def redact_url(url: str) -> str:
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or "unknown"
        port = f":{parsed.port}" if parsed.port else ""
        raw_path = parsed.path.rstrip("/")
        path = "/…" if raw_path else ""
        scheme = parsed.scheme or "rpc"
        return f"{scheme}://{host}{port}{path}"
    except Exception:
        return "<rpc-url>"


def truncate_error(value: Any, limit: int = 500) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ")
    return text[:limit]


def backoff_delay(base: float, maximum: float, attempt: int) -> float:
    raw = min(maximum, max(base, 0.01) * (1.8 ** max(0, attempt - 1)))
    return raw * random.uniform(0.75, 1.25)


def interruptible_sleep(seconds: float) -> None:
    if seconds > 0:
        STOP_EVENT.wait(seconds)


def chunked(items: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_retry_after(value: Optional[str], maximum: float) -> Optional[float]:
    if not value:
        return None
    value = value.strip()
    try:
        seconds = float(value)
        return min(maximum, max(0.0, seconds))
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        seconds = (dt - datetime.now(timezone.utc)).total_seconds()
        return min(maximum, max(0.0, seconds))
    except (TypeError, ValueError, OverflowError):
        return None


def fsync_directory(path: Path) -> None:
    if os.name == "nt" or not hasattr(os, "O_DIRECTORY"):
        return
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# ==========================================================
# HTTP / JSON-RPC
# ==========================================================


def get_session(cfg: Config, url: str) -> requests.Session:
    sessions: dict[str, requests.Session]
    if not hasattr(THREAD_LOCAL, "sessions"):
        THREAD_LOCAL.sessions = {}
    sessions = THREAD_LOCAL.sessions

    session = sessions.get(url)
    if session is None:
        session = requests.Session()
        session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": cfg.user_agent,
            }
        )
        adapter = HTTPAdapter(
            pool_connections=cfg.pool_connections,
            pool_maxsize=cfg.pool_maxsize,
            max_retries=0,
            pool_block=True,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        sessions[url] = session
        with SESSION_REGISTRY_LOCK:
            SESSION_REGISTRY.add(session)
    return session


def discard_thread_session(url: str) -> None:
    sessions = getattr(THREAD_LOCAL, "sessions", None)
    if not sessions:
        return
    session = sessions.pop(url, None)
    if session is None:
        return
    with SESSION_REGISTRY_LOCK:
        SESSION_REGISTRY.discard(session)
    try:
        session.close()
    except Exception:
        pass


def close_all_sessions() -> None:
    with SESSION_REGISTRY_LOCK:
        sessions = list(SESSION_REGISTRY)
        SESSION_REGISTRY.clear()
    for session in sessions:
        try:
            session.close()
        except Exception:
            pass


def is_throttle_text(text: str) -> bool:
    msg = text.lower()
    return any(
        token in msg
        for token in (
            "429",
            "rate limit",
            "rate-limit",
            "too many requests",
            "request rate exceeded",
            "daily request count exceeded",
            "capacity exceeded",
            "compute units per second",
            "cu per second",
            "throughput limit",
        )
    )


def is_batch_limit_text(text: str) -> bool:
    msg = text.lower()
    return any(
        token in msg
        for token in (
            "batch too large",
            "batch size",
            "too many batch",
            "too many requests in batch",
            "request entity too large",
            "payload too large",
            "content length",
            "maximum batch",
            "max batch",
            "413",
        )
    )


def is_retryable_text(text: str) -> bool:
    msg = text.lower()
    return is_throttle_text(msg) or any(
        token in msg
        for token in (
            "timeout",
            "timed out",
            "temporarily unavailable",
            "connection",
            "server error",
            "bad gateway",
            "gateway timeout",
            "service unavailable",
            "internal error",
            "header not found",
            "missing trie node",
            "econnreset",
            "connection reset",
            "try again",
            "408",
            "425",
            "429",
            "500",
            "502",
            "503",
            "504",
            "-32005",
            "-32603",
        )
    )


def json_rpc_error_text(item: Mapping[str, Any]) -> str:
    error = item.get("error")
    if isinstance(error, Mapping):
        code = error.get("code")
        message = error.get("message", "")
        data = error.get("data", "")
        return truncate_error(f"RPC {code}: {message} {data}".strip())
    return truncate_error(error)


def make_rpc_call(call_id: int, method: str, params: list[Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": call_id, "method": method, "params": params}


def index_rpc_responses(
    responses: Iterable[dict[str, Any]],
) -> tuple[dict[int, dict[str, Any]], set[int]]:
    mapped: dict[int, dict[str, Any]] = {}
    duplicates: set[int] = set()
    for item in responses:
        try:
            call_id = item.get("id")
            if isinstance(call_id, bool) or call_id is None:
                continue
            numeric_id = int(call_id)
        except (TypeError, ValueError):
            continue

        if numeric_id in mapped:
            duplicates.add(numeric_id)
        else:
            mapped[numeric_id] = item

    for duplicate_id in duplicates:
        mapped.pop(duplicate_id, None)
    return mapped, duplicates


def response_by_id(responses: Iterable[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    mapped, _duplicates = index_rpc_responses(responses)
    return mapped


def rpc_request_to_node(
    cfg: Config,
    node: RpcNode,
    payload: dict[str, Any] | list[dict[str, Any]],
    *,
    expect_batch: bool,
    retry_request: bool = False,
) -> tuple[dict[str, Any] | list[dict[str, Any]], float]:
    if STOP_EVENT.is_set():
        raise StopRequested("stopped")

    session = get_session(cfg, node.url)
    rpc_call_count = len(payload) if isinstance(payload, list) else 1
    require_rpc_pool().metrics.add_http_request(rpc_call_count, retry=retry_request)

    started = time.monotonic()
    try:
        response = session.post(
            node.url,
            json=payload,
            timeout=(cfg.connect_timeout, cfg.request_timeout),
        )
    except RequestException as exc:
        # Force a fresh pool/connection on the next attempt. This helps with
        # broken keep-alive sockets, stale DNS, and provider-side disconnects.
        discard_thread_session(node.url)
        text = truncate_error(exc)
        raise RpcRequestError(
            f"transport error: {text}",
            retryable=True,
            throttled=is_throttle_text(text),
        ) from exc

    latency = time.monotonic() - started

    if response.status_code != 200:
        body = truncate_error(response.text)
        text = f"HTTP {response.status_code}: {body}"
        retry_after = parse_retry_after(response.headers.get("Retry-After"), cfg.max_retry_after)
        throttled = response.status_code == 429 or is_throttle_text(text)
        batch_too_large = response.status_code == 413 or is_batch_limit_text(text)
        retryable = response.status_code in RETRYABLE_HTTP_STATUSES or throttled or batch_too_large or is_retryable_text(text)
        raise RpcRequestError(
            text,
            retryable=retryable,
            throttled=throttled,
            retry_after=retry_after,
            batch_too_large=batch_too_large,
        )

    try:
        data = response.json()
    except ValueError as exc:
        text = truncate_error(response.text)
        raise RpcRequestError(
            f"invalid JSON response: {text}",
            retryable=True,
        ) from exc

    if expect_batch and not isinstance(data, list):
        error_text = json_rpc_error_text(data) if isinstance(data, Mapping) and "error" in data else truncate_error(data)
        raise RpcRequestError(
            f"RPC returned non-batch response: {error_text}",
            retryable=is_retryable_text(error_text),
            throttled=is_throttle_text(error_text),
            batch_too_large=is_batch_limit_text(error_text),
        )

    if not expect_batch and not isinstance(data, dict):
        raise RpcRequestError(
            f"RPC returned invalid single response: {truncate_error(data)}",
            retryable=True,
        )

    return data, latency


def synthetic_rpc_error(call_id: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": call_id,
        "error": {
            "code": -32098,
            "message": truncate_error(message),
        },
    }


def _merge_batch_results(
    original_payload: list[dict[str, Any]],
    *response_groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[int, dict[str, Any]] = {}
    for group in response_groups:
        mapped, _duplicates = index_rpc_responses(group)
        merged.update(mapped)

    ordered: list[dict[str, Any]] = []
    for item in original_payload:
        call_id = int(item["id"])
        ordered.append(merged.get(call_id, synthetic_rpc_error(call_id, "missing result after batch split")))
    return ordered


def rpc_batch(
    cfg: Config,
    payload: list[dict[str, Any]],
    *,
    _split_depth: int = 0,
) -> list[dict[str, Any]]:
    """Execute a JSON-RPC batch with per-item retries and provider failover.

    Successful items and deterministic per-call errors are retained immediately.
    Only missing responses and retryable per-call errors are resent. If every
    provider rejects the batch size, the unresolved subset is split recursively.

    On exhausted retries, unresolved calls are returned as synthetic per-call RPC
    errors instead of discarding successful items from the same batch.
    """
    if not payload:
        return []
    if STOP_EVENT.is_set():
        raise StopRequested("stopped")

    call_ids: list[int] = []
    seen_ids: set[int] = set()
    for item in payload:
        if "id" not in item:
            raise ValueError("Every JSON-RPC batch item must contain an id")
        call_id = int(item["id"])
        if call_id in seen_ids:
            raise ValueError(f"Duplicate JSON-RPC request id: {call_id}")
        seen_ids.add(call_id)
        call_ids.append(call_id)

    remaining: dict[int, dict[str, Any]] = {
        int(item["id"]): item for item in payload
    }
    final: dict[int, dict[str, Any]] = {}
    last_errors: dict[int, str] = {}
    attempted_nodes: set[str] = set()
    batch_limit_nodes: set[str] = set()
    pool = require_rpc_pool()

    for attempt in range(1, cfg.max_retries + 1):
        if not remaining:
            break
        if STOP_EVENT.is_set():
            raise StopRequested("stopped")

        current_payload = list(remaining.values())

        try:
            with pool.lease() as node:
                attempted_nodes.add(node.url)
                try:
                    raw, latency = rpc_request_to_node(
                        cfg,
                        node,
                        current_payload,
                        expect_batch=True,
                        retry_request=attempt > 1,
                    )
                except RpcRequestError as exc:
                    pool.metrics.add_failure(throttled=exc.throttled)
                    RpcPool.mark_failure(
                        cfg,
                        node,
                        throttled=exc.throttled,
                        retry_after=exc.retry_after,
                    )
                    error_text = truncate_error(exc)
                    for call_id in remaining:
                        last_errors[call_id] = error_text

                    if exc.batch_too_large and len(current_payload) > 1:
                        batch_limit_nodes.add(node.url)
                        active_count = max(1, len(pool.active_nodes()))
                        if len(batch_limit_nodes) >= active_count:
                            break

                    if not exc.retryable and not exc.batch_too_large and len(pool.active_nodes()) <= 1:
                        break
                    continue

                assert isinstance(raw, list)
                data = [item for item in raw if isinstance(item, dict)]
                mapped, duplicates = index_rpc_responses(data)

                missing_count = 0
                retryable_error_count = 0
                throttled = False

                for call_id in list(remaining):
                    if call_id in duplicates:
                        last_errors[call_id] = "duplicate JSON-RPC response id"
                        missing_count += 1
                        continue

                    item = mapped.get(call_id)
                    if item is None:
                        last_errors[call_id] = "missing JSON-RPC batch response"
                        missing_count += 1
                        continue

                    if "error" in item:
                        error_text = json_rpc_error_text(item)
                        if is_retryable_text(error_text):
                            last_errors[call_id] = error_text
                            retryable_error_count += 1
                            throttled = throttled or is_throttle_text(error_text)
                            continue

                    final[call_id] = item
                    remaining.pop(call_id, None)
                    last_errors.pop(call_id, None)

                if missing_count or retryable_error_count:
                    pool.metrics.add_semantic_retry(
                        missing=missing_count,
                        retryable_errors=retryable_error_count,
                    )
                    pool.metrics.add_failure(throttled=throttled)
                    RpcPool.mark_failure(cfg, node, throttled=throttled)
                else:
                    RpcPool.mark_success(node, latency)

        except StopRequested:
            raise
        except RuntimeError as exc:
            # This mainly covers an unexpectedly unavailable pool. Preserve any
            # already-completed batch items and mark the unresolved subset later.
            error_text = truncate_error(exc)
            for call_id in remaining:
                last_errors[call_id] = error_text
            break

    if remaining and batch_limit_nodes and len(remaining) > 1:
        unresolved_payload = list(remaining.values())
        midpoint = len(unresolved_payload) // 2
        if midpoint > 0:
            pool.metrics.add_split()
            left_payload = unresolved_payload[:midpoint]
            right_payload = unresolved_payload[midpoint:]
            logging.debug(
                "Splitting rejected RPC batch: size=%d -> %d + %d | depth=%d",
                len(unresolved_payload),
                len(left_payload),
                len(right_payload),
                _split_depth + 1,
            )
            left = rpc_batch(cfg, left_payload, _split_depth=_split_depth + 1)
            right = rpc_batch(cfg, right_payload, _split_depth=_split_depth + 1)
            split_mapped = response_by_id(left + right)
            for call_id in list(remaining):
                if call_id in split_mapped:
                    final[call_id] = split_mapped[call_id]
                    remaining.pop(call_id, None)

    if remaining:
        attempted = ", ".join(sorted(redact_url(url) for url in attempted_nodes)) or "none"
        for call_id in list(remaining):
            detail = last_errors.get(call_id, "RPC retries exhausted")
            final[call_id] = synthetic_rpc_error(
                call_id,
                f"{detail}; attempted RPC nodes: {attempted}",
            )
            remaining.pop(call_id, None)

    return [
        final.get(call_id, synthetic_rpc_error(call_id, "internal batch result missing"))
        for call_id in call_ids
    ]


def rpc_single_direct(
    cfg: Config,
    node: RpcNode,
    method: str,
    params: list[Any],
    *,
    call_id: int = 1,
) -> dict[str, Any]:
    payload = make_rpc_call(call_id, method, params)
    last_error = "unknown RPC error"

    for attempt in range(1, cfg.health_retries + 1):
        if STOP_EVENT.is_set():
            raise StopRequested("stopped")
        try:
            raw, latency = rpc_request_to_node(
                cfg,
                node,
                payload,
                expect_batch=False,
                retry_request=attempt > 1,
            )
            assert isinstance(raw, dict)
            if "error" in raw:
                error_text = json_rpc_error_text(raw)
                if is_retryable_text(error_text) and attempt < cfg.health_retries:
                    last_error = error_text
                    interruptible_sleep(backoff_delay(cfg.base_delay, cfg.max_delay, attempt))
                    continue
            with node.lock:
                node.latency_ema = node.latency_ema * 0.50 + latency * 0.50
            return raw
        except RpcRequestError as exc:
            last_error = truncate_error(exc)
            if attempt >= cfg.health_retries or not exc.retryable:
                break
            delay = exc.retry_after if exc.retry_after is not None else backoff_delay(cfg.base_delay, cfg.max_delay, attempt)
            interruptible_sleep(delay)

    raise RuntimeError(last_error)


def parse_quantity(value: Any, *, field_name: str) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"bad {field_name} result: {value!r}")
    return int(value, 16)


def probe_batch_support(cfg: Config, node: RpcNode, expected_chain_id: int) -> None:
    payload = [
        make_rpc_call(101, "eth_chainId", []),
        make_rpc_call(102, "eth_chainId", []),
    ]
    last_error = "batch probe failed"

    for attempt in range(1, cfg.health_retries + 1):
        try:
            raw, latency = rpc_request_to_node(
                cfg,
                node,
                payload,
                expect_batch=True,
                retry_request=attempt > 1,
            )
            assert isinstance(raw, list)
            mapped, duplicates = index_rpc_responses(
                item for item in raw if isinstance(item, dict)
            )
            if duplicates or set(mapped) != {101, 102}:
                raise RuntimeError("batch probe returned missing/duplicate ids")
            for call_id in (101, 102):
                item = mapped[call_id]
                if "error" in item:
                    raise RuntimeError(json_rpc_error_text(item))
                chain_id = parse_quantity(item.get("result"), field_name="eth_chainId")
                if chain_id != expected_chain_id:
                    raise RuntimeError(
                        f"batch probe chain_id={chain_id}, expected {expected_chain_id}"
                    )
            with node.lock:
                node.latency_ema = node.latency_ema * 0.50 + latency * 0.50
            return
        except (RpcRequestError, RuntimeError, ValueError) as exc:
            last_error = truncate_error(exc)
            retryable = isinstance(exc, RpcRequestError) and exc.retryable
            if attempt >= cfg.health_retries or not retryable:
                break
            delay = exc.retry_after if isinstance(exc, RpcRequestError) and exc.retry_after is not None else backoff_delay(cfg.base_delay, cfg.max_delay, attempt)
            interruptible_sleep(delay)

    raise RuntimeError(last_error)


def validate_rpc_nodes(cfg: Config) -> int:
    pool = require_rpc_pool()
    logging.info("Validating %d RPC node(s)...", len(pool.nodes))
    healthy_chain_ids: set[int] = set()

    for node in pool.nodes:
        try:
            response = rpc_single_direct(cfg, node, "eth_chainId", [])
            if "error" in response:
                raise RuntimeError(json_rpc_error_text(response))
            chain_id = parse_quantity(response.get("result"), field_name="eth_chainId")
            node.chain_id = chain_id

            if not cfg.skip_batch_probe:
                probe_batch_support(cfg, node, chain_id)

            block_response = rpc_single_direct(cfg, node, "eth_blockNumber", [])
            if "error" in block_response:
                raise RuntimeError(json_rpc_error_text(block_response))
            node.block_number = parse_quantity(block_response.get("result"), field_name="eth_blockNumber")

            healthy_chain_ids.add(chain_id)
            logging.info(
                "RPC OK: %s | chain_id=%d | block=%d | batch=%s",
                redact_url(node.url),
                chain_id,
                node.block_number,
                "unchecked" if cfg.skip_batch_probe else "yes",
            )
        except Exception as exc:
            reason = f"health check failed: {truncate_error(exc)}"
            RpcPool.disable(node, reason)
            logging.warning("RPC disabled: %s | %s", redact_url(node.url), reason)

    if not healthy_chain_ids:
        raise RuntimeError("All RPC nodes failed health validation")

    if cfg.expected_chain_id is None:
        if len(healthy_chain_ids) != 1:
            values = ", ".join(str(value) for value in sorted(healthy_chain_ids))
            raise RuntimeError(
                "RPC nodes report multiple chain IDs "
                f"({values}). Set --expected-chain-id explicitly to avoid scanning the wrong chain."
            )
        target_chain_id = next(iter(healthy_chain_ids))
    else:
        target_chain_id = cfg.expected_chain_id

    for node in pool.active_nodes():
        if node.chain_id != target_chain_id:
            reason = f"wrong chain_id={node.chain_id}; expected {target_chain_id}"
            RpcPool.disable(node, reason)
            logging.warning("RPC disabled: %s | %s", redact_url(node.url), reason)

    active = pool.active_nodes()
    if not active:
        raise RuntimeError(f"No RPC nodes remain for chain_id={target_chain_id}")

    if cfg.max_rpc_lag_blocks > 0:
        known_blocks = [node.block_number for node in active if node.block_number is not None]
        if known_blocks:
            best_block = max(known_blocks)
            for node in list(active):
                if node.block_number is None:
                    continue
                lag = best_block - node.block_number
                if lag > cfg.max_rpc_lag_blocks:
                    reason = (
                        f"lagging by {lag} blocks; allowed {cfg.max_rpc_lag_blocks}"
                    )
                    RpcPool.disable(node, reason)
                    logging.warning("RPC disabled: %s | %s", redact_url(node.url), reason)

    active = pool.active_nodes()
    if not active:
        raise RuntimeError("All matching RPC nodes were removed by lag filtering")

    logging.info(
        "RPC pool ready: active=%d/%d | chain_id=%d",
        len(active),
        len(pool.nodes),
        target_chain_id,
    )
    return target_chain_id


# ==========================================================
# INPUT / CSV / STATE
# ==========================================================


def iter_clean_lines(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8-sig") as file:
        for raw in file:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            yield line


def unique_preserve_order(items: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(items))


def extract_first_token(line: str) -> str:
    earliest = len(line)
    for delimiter in (",", ";", "\t", " "):
        index = line.find(delimiter)
        if index >= 0:
            earliest = min(earliest, index)
    return line[:earliest].strip().strip('"').strip("'")


def normalize_evm_address(value: str) -> Optional[str]:
    token = value.strip()
    if Web3 is not None:
        try:
            if Web3.is_address(token):
                return Web3.to_checksum_address(token)
            return None
        except (TypeError, ValueError):
            return None

    clean = token[2:] if token.lower().startswith("0x") else token
    if len(clean) != 40:
        return None
    if any(char not in "0123456789abcdefABCDEF" for char in clean):
        return None
    return "0x" + clean.lower()


def validate_addresses(items: Iterable[str], *, label: str) -> list[str]:
    valid: list[str] = []
    invalid_samples: list[str] = []
    invalid_count = 0

    for item in items:
        token = extract_first_token(item)
        normalized = normalize_evm_address(token)
        if normalized is not None:
            valid.append(normalized)
        else:
            invalid_count += 1
            if len(invalid_samples) < 5:
                invalid_samples.append(token)

    deduplicated = unique_preserve_order(valid)
    if invalid_count:
        logging.warning(
            "Skipped invalid %s line(s): %d | samples: %s",
            label,
            invalid_count,
            ", ".join(invalid_samples),
        )
    if len(valid) != len(deduplicated):
        logging.info(
            "Removed %d duplicate %s line(s)",
            len(valid) - len(deduplicated),
            label,
        )
    return deduplicated


def ensure_csv_header(path: Path, header: Sequence[str], *, fsync_writes: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as file:
                existing = next(csv.reader(file), None)
        except (OSError, csv.Error) as exc:
            raise RuntimeError(f"Cannot validate CSV header in {path}: {exc}") from exc
        if existing != list(header):
            raise RuntimeError(
                f"Unexpected CSV header in {path}: {existing!r}; expected {list(header)!r}"
            )
        return

    with path.open("w", encoding="utf-8-sig", newline="") as file:
        csv.writer(file).writerow(header)
        file.flush()
        if fsync_writes:
            os.fsync(file.fileno())
    if fsync_writes:
        fsync_directory(path.parent)


def _parse_result_counters(row: Mapping[str, Any]) -> Optional[tuple[int, int, int]]:
    try:
        checked = int(row.get("checked_contracts") or "")
        failed = int(row.get("failed_contracts") or "")
        total = int(row.get("total_contracts") or "")
    except (TypeError, ValueError):
        return None
    if checked < 0 or failed < 0 or total < 0:
        return None
    if checked > total or failed > total or checked + failed > total:
        return None
    return checked, failed, total


def load_addresses_from_csv(path: Path, *, kind: str) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()

    if kind == "output":
        expected_header = [
            "address",
            "owns_nft",
            "checked_contracts",
            "failed_contracts",
            "total_contracts",
        ]
    elif kind == "failed":
        expected_header = [
            "address",
            "checked_contracts",
            "failed_contracts",
            "total_contracts",
            "error",
        ]
    else:
        raise ValueError(f"unknown CSV kind: {kind}")

    completed: set[str] = set()
    malformed_rows = 0
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            if reader.fieldnames != expected_header:
                raise RuntimeError(
                    f"Unexpected CSV header in {path}: {reader.fieldnames!r}; expected {expected_header!r}"
                )

            for row in reader:
                address = normalize_evm_address((row.get("address") or "").strip())
                counters = _parse_result_counters(row)
                row_valid = address is not None and counters is not None

                if kind == "output":
                    status = (row.get("owns_nft") or "").strip().lower()
                    row_valid = row_valid and status in {"true", "false"}
                else:
                    # A failed row should carry a reason. Requiring it also keeps
                    # a torn/partial final CSV line from becoming resumable work.
                    error_text = (row.get("error") or "").strip()
                    row_valid = row_valid and bool(error_text)

                if row_valid and address is not None:
                    completed.add(address)
                else:
                    malformed_rows += 1
    except (OSError, csv.Error) as exc:
        raise RuntimeError(f"Cannot read resume file {path}: {exc}") from exc

    if malformed_rows:
        logging.warning(
            "Ignored %d malformed/torn row(s) while reading %s",
            malformed_rows,
            path,
        )
    return completed


def contracts_fingerprint(
    chain_id: int,
    contracts: Sequence[str],
    *,
    block_tag: str,
    allow_partial_false: bool,
) -> str:
    payload = {
        "chain_id": chain_id,
        "contracts": sorted(address.lower() for address in contracts),
        "block_tag": block_tag,
        "allow_partial_false": allow_partial_false,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_state(path: Path) -> Optional[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, dict):
            return data
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        logging.warning("Cannot read state file %s: %s", path, exc)
    return None


def atomic_write_json(path: Path, data: Mapping[str, Any], *, fsync_writes: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, sort_keys=True)
            file.write("\n")
            file.flush()
            if fsync_writes:
                os.fsync(file.fileno())
        os.replace(temp_path, path)
        if fsync_writes:
            fsync_directory(path.parent)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def prepare_resume(
    cfg: Config,
    *,
    chain_id: int,
    contracts: Sequence[str],
) -> set[str]:
    if cfg.fresh:
        logging.warning("Fresh mode: clearing prior output, failed, and state files")
        for path in (cfg.output_file, cfg.failed_file, cfg.state_file):
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                raise RuntimeError(f"Cannot clear {path}: {exc}") from exc

    output_addresses = load_addresses_from_csv(cfg.output_file, kind="output")
    failed_addresses = load_addresses_from_csv(cfg.failed_file, kind="failed")
    existing_rows = bool(output_addresses or failed_addresses)

    signature = contracts_fingerprint(
        chain_id,
        contracts,
        block_tag=cfg.block_tag,
        allow_partial_false=cfg.allow_partial_false,
    )
    state = read_state(cfg.state_file)

    if existing_rows:
        state_signature = state.get("signature") if state else None
        if state_signature != signature:
            details = (
                "existing CSV results do not have a matching scan state fingerprint"
                if state
                else "existing CSV results have no scan state fingerprint"
            )
            if not cfg.force_resume_mismatch:
                raise RuntimeError(
                    f"Unsafe resume refused: {details}. Use --fresh to start over, "
                    "or --force-resume-mismatch only if you intentionally accept stale/mismatched results."
                )
            logging.warning("FORCED unsafe resume: %s", details)

    created_at = state.get("created_at") if state else None
    state_payload = {
        "schema": STATE_SCHEMA,
        "checker_version": VERSION,
        "signature": signature,
        "chain_id": chain_id,
        "contract_count": len(contracts),
        "block_tag": cfg.block_tag,
        "allow_partial_false": cfg.allow_partial_false,
        "created_at": created_at or utc_now_iso(),
        "last_started_at": utc_now_iso(),
    }
    atomic_write_json(cfg.state_file, state_payload, fsync_writes=cfg.fsync_writes)

    completed = set(output_addresses)
    if cfg.resume_failed:
        completed |= failed_addresses
    else:
        # Failed rows are work-to-retry. Reset the file so it represents failures
        # from this invocation instead of accumulating stale failures forever.
        if cfg.failed_file.exists() and cfg.failed_file.stat().st_size > 0:
            logging.info("Resetting failed CSV because failed rows will be retried")
            cfg.failed_file.unlink()

    return completed


# ==========================================================
# CONTRACT / BALANCE LOGIC
# ==========================================================


def balance_of_calldata(wallet: str) -> str:
    clean = wallet.lower().removeprefix("0x")
    return "0x" + BALANCE_OF_SELECTOR + clean.rjust(64, "0")


def parse_uint256_hex(value: Any) -> int:
    # eth_call returns ABI DATA, not an Ethereum quantity. A standard
    # balanceOf(address) return is exactly one 32-byte uint256. Treat empty
    # data ("0x"), short quantities ("0x0"), malformed hex, and extra bytes as
    # uncertain instead of silently interpreting them as zero.
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"bad uint256 ABI result: {value!r}")
    encoded = value[2:]
    if len(encoded) != 64:
        raise ValueError(f"uint256 ABI result must be 32 bytes, got {len(encoded) // 2} byte(s)")
    if any(char not in "0123456789abcdefABCDEF" for char in encoded):
        raise ValueError("uint256 ABI result contains non-hex characters")
    return int(encoded, 16)


def has_contract_code(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"bad eth_getCode result: {value!r}")
    encoded = value[2:]
    if not encoded:
        return False
    if len(encoded) % 2 != 0:
        raise ValueError("eth_getCode returned odd-length hex data")
    if any(char not in "0123456789abcdefABCDEF" for char in encoded):
        raise ValueError("eth_getCode returned non-hex data")
    # 0x00 is still one byte of contract code (STOP), not an EOA.
    return True


def filter_contracts(cfg: Config, contracts: list[str]) -> list[str]:
    if cfg.skip_contract_validation:
        logging.info("Contract bytecode validation skipped")
        return contracts

    status: dict[str, str] = {}
    logging.info("Checking contract bytecode: %d candidate(s)", len(contracts))

    for batch in chunked(contracts, cfg.contract_batch_size):
        payload = [
            make_rpc_call(i, "eth_getCode", [address, cfg.block_tag])
            for i, address in enumerate(batch)
        ]
        try:
            responses = response_by_id(rpc_batch(cfg, payload))
        except StopRequested:
            raise
        except Exception as exc:
            logging.warning(
                "Contract validation batch failed; keeping %d uncertain contract(s): %s",
                len(batch),
                exc,
            )
            for address in batch:
                status[address] = "uncertain"
            continue

        for i, address in enumerate(batch):
            item = responses.get(i)
            if not item:
                status[address] = "uncertain"
                logging.warning("No eth_getCode response for %s; keeping it", address)
                continue
            if "error" in item:
                status[address] = "uncertain"
                logging.warning(
                    "eth_getCode error for %s; keeping it: %s",
                    address,
                    json_rpc_error_text(item),
                )
                continue
            try:
                if has_contract_code(item.get("result")):
                    status[address] = "valid"
                else:
                    status[address] = "empty"
                    logging.warning("EOA/empty address skipped from contract list: %s", address)
            except ValueError as exc:
                status[address] = "uncertain"
                logging.warning("Malformed eth_getCode result for %s; keeping it: %s", address, exc)

    result = [address for address in contracts if status.get(address) in {"valid", "uncertain"}]
    verified = sum(1 for value in status.values() if value == "valid")
    uncertain = sum(1 for value in status.values() if value == "uncertain")
    empty = sum(1 for value in status.values() if value == "empty")
    logging.info(
        "Contract validation done: usable=%d/%d | verified=%d | uncertain=%d | skipped_empty=%d",
        len(result),
        len(contracts),
        verified,
        uncertain,
        empty,
    )
    return result


def format_error_samples(errors: Sequence[str], total_failures: int) -> str:
    text = " | ".join(errors)
    remaining = max(0, total_failures - len(errors))
    if remaining:
        suffix = f" | +{remaining} more failure(s)"
        text = (text + suffix) if text else suffix.lstrip(" |")
    return text


def check_wallet(cfg: Config, wallet: str, contracts: Sequence[str]) -> CheckResult:
    checked = 0
    failed = 0
    errors: list[str] = []
    calldata = balance_of_calldata(wallet)

    for batch in chunked(contracts, cfg.contract_batch_size):
        if STOP_EVENT.is_set():
            return CheckResult(wallet, None, checked, failed, len(contracts), "stopped")

        payload = [
            make_rpc_call(
                i,
                "eth_call",
                [{"to": contract, "data": calldata}, cfg.block_tag],
            )
            for i, contract in enumerate(batch)
        ]

        try:
            responses = response_by_id(rpc_batch(cfg, payload))
        except StopRequested:
            return CheckResult(wallet, None, checked, failed, len(contracts), "stopped")
        except Exception as exc:
            failed += len(batch)
            if len(errors) < 3:
                errors.append(truncate_error(exc))
            continue

        for i, contract in enumerate(batch):
            item = responses.get(i)
            if not item:
                failed += 1
                if len(errors) < 3:
                    errors.append(f"{contract}: missing RPC response")
                continue

            if "error" in item:
                failed += 1
                if len(errors) < 3:
                    errors.append(f"{contract}: {json_rpc_error_text(item)}")
                continue

            try:
                balance = parse_uint256_hex(item.get("result"))
            except (TypeError, ValueError) as exc:
                failed += 1
                if len(errors) < 3:
                    errors.append(f"{contract}: {truncate_error(exc)}")
                continue

            checked += 1
            if balance > 0:
                # Positive ownership is conclusive even if another contract in the
                # same or an earlier batch failed.
                return CheckResult(wallet, True, checked, failed, len(contracts))

    error_text = format_error_samples(errors, failed)
    if failed and not cfg.allow_partial_false:
        return CheckResult(
            wallet,
            None,
            checked,
            failed,
            len(contracts),
            error_text or "incomplete check",
        )

    if checked == 0:
        return CheckResult(
            wallet,
            None,
            checked,
            failed,
            len(contracts),
            error_text or "no confirmed checks",
        )

    return CheckResult(wallet, False, checked, failed, len(contracts), error_text)


# ==========================================================
# WRITER / PROGRESS
# ==========================================================


def append_csv_rows(path: Path, rows: list[list[object]], *, fsync_writes: bool) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8-sig", newline="") as file:
        csv.writer(file).writerows(rows)
        file.flush()
        if fsync_writes:
            os.fsync(file.fileno())


def writer_loop(
    cfg: Config,
    work_queue: "queue.Queue[CheckResult | object]",
    error_box: list[BaseException],
) -> None:
    try:
        ensure_csv_header(
            cfg.output_file,
            ["address", "owns_nft", "checked_contracts", "failed_contracts", "total_contracts"],
            fsync_writes=cfg.fsync_writes,
        )
        ensure_csv_header(
            cfg.failed_file,
            ["address", "checked_contracts", "failed_contracts", "total_contracts", "error"],
            fsync_writes=cfg.fsync_writes,
        )

        ok_buffer: list[list[object]] = []
        failed_buffer: list[list[object]] = []
        last_flush = time.monotonic()

        def flush() -> None:
            nonlocal ok_buffer, failed_buffer, last_flush
            append_csv_rows(cfg.output_file, ok_buffer, fsync_writes=cfg.fsync_writes)
            append_csv_rows(cfg.failed_file, failed_buffer, fsync_writes=cfg.fsync_writes)
            ok_buffer = []
            failed_buffer = []
            last_flush = time.monotonic()

        while True:
            timeout = max(0.1, cfg.writer_flush_seconds - (time.monotonic() - last_flush))
            try:
                item = work_queue.get(timeout=timeout)
            except queue.Empty:
                flush()
                continue

            try:
                if item is WRITE_SENTINEL:
                    flush()
                    return

                if not isinstance(item, CheckResult):
                    raise TypeError(f"unexpected writer item: {type(item)!r}")

                if item.owns_nft is None:
                    failed_buffer.append(
                        [
                            item.address,
                            item.checked_contracts,
                            item.failed_contracts,
                            item.total_contracts,
                            item.error,
                        ]
                    )
                else:
                    ok_buffer.append(
                        [
                            item.address,
                            str(item.owns_nft).lower(),
                            item.checked_contracts,
                            item.failed_contracts,
                            item.total_contracts,
                        ]
                    )

                if len(ok_buffer) + len(failed_buffer) >= cfg.writer_batch_size:
                    flush()
            finally:
                work_queue.task_done()
    except BaseException as exc:
        error_box.append(exc)
        STOP_EVENT.set()
        logging.exception("CSV writer crashed")


def enqueue_writer_result(
    work_queue: "queue.Queue[CheckResult | object]",
    result: CheckResult,
    *,
    writer: threading.Thread,
    writer_errors: list[BaseException],
) -> None:
    while True:
        if writer_errors:
            raise RuntimeError("CSV writer failed") from writer_errors[0]
        if not writer.is_alive():
            raise RuntimeError("CSV writer stopped unexpectedly")
        try:
            work_queue.put(result, timeout=0.25)
            return
        except queue.Full:
            continue


def log_progress(
    cfg: Config,
    stats: Stats,
    result: CheckResult,
    work_queue: "queue.Queue[CheckResult | object]",
    writer: threading.Thread,
    writer_errors: list[BaseException],
    total: int,
    started: float,
) -> None:
    enqueue_writer_result(
        work_queue,
        result,
        writer=writer,
        writer_errors=writer_errors,
    )
    done, confirmed, owners, uncertain = stats.add(result)

    if done % cfg.progress_every != 0 and done != total:
        return

    elapsed = max(0.001, time.monotonic() - started)
    speed = done / elapsed
    eta = max(0.0, (total - done) / speed) if speed else 0.0
    rpc = require_rpc_pool().metrics.snapshot()
    logging.info(
        "Progress %d/%d (%.2f%%) | %.1f wallet/s | confirmed=%d | owners=%d | uncertain=%d "
        "| HTTP=%d | RPC calls=%d | retried=%d | throttles=%d | ETA %.1fs",
        done,
        total,
        done / total * 100,
        speed,
        confirmed,
        owners,
        uncertain,
        rpc["http_requests"],
        rpc["rpc_calls_sent"],
        rpc["retried_rpc_calls"],
        rpc["throttle_events"],
        eta,
    )


# ==========================================================
# ARGUMENTS / MAIN
# ==========================================================


def parse_rpc_urls(raw_values: Iterable[str]) -> tuple[str, ...]:
    urls: list[str] = []
    for raw in raw_values:
        for part in raw.replace(";", ",").split(","):
            url = part.strip()
            if url and url.lower() not in {"none", "null", "changeme", "your_rpc_url"}:
                urls.append(url)
    return tuple(unique_preserve_order(urls))


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def parse_optional_int(value: str) -> Optional[int]:
    value = value.strip()
    if not value:
        return None
    return int(value, 0)


def validate_block_tag(value: str) -> str:
    text = value.strip().lower()
    if text in {"latest", "safe", "finalized", "pending", "earliest"}:
        return text
    if text.startswith("0x"):
        int(text, 16)
        return text
    raise argparse.ArgumentTypeError(
        "block tag must be latest/safe/finalized/pending/earliest or a 0x-prefixed block number"
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    env_rpc = (
        os.getenv("RPC_URLS", "").strip()
        or os.getenv("RPC_URL", "").strip()
        or os.getenv("INFURA_URL", "").strip()
    )

    parser = argparse.ArgumentParser(
        description="Robust batch ERC-721 ownership checker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--rpc-url",
        action="append",
        default=[env_rpc] if env_rpc else [],
        help="Repeatable or comma-separated RPC URL",
    )
    parser.add_argument("--input", default=os.getenv("INPUT_FILE", "input_addresses.txt"))
    parser.add_argument("--contracts", default=os.getenv("CONTRACTS_FILE", "nft_contracts.txt"))
    parser.add_argument("--output", default=os.getenv("OUTPUT_FILE", "nft_owners.csv"))
    parser.add_argument("--failed", default=os.getenv("FAILED_FILE", "nft_owners_failed.csv"))
    parser.add_argument("--state", default=os.getenv("STATE_FILE", ""), help="Resume fingerprint state file")
    parser.add_argument("--log", default=os.getenv("LOG_FILE", "nft_checker.log"))
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"), choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--threads", type=int, default=int(os.getenv("NUM_THREADS", "32")))
    parser.add_argument("--max-inflight", type=int, default=int(os.getenv("MAX_INFLIGHT", "0")))
    parser.add_argument("--max-retries", type=int, default=int(os.getenv("MAX_RETRIES", "5")))
    parser.add_argument("--health-retries", type=int, default=int(os.getenv("HEALTH_RETRIES", "3")))
    parser.add_argument("--base-delay", type=float, default=float(os.getenv("BASE_DELAY", "0.25")))
    parser.add_argument("--max-delay", type=float, default=float(os.getenv("MAX_DELAY", "10")))
    parser.add_argument("--max-retry-after", type=float, default=float(os.getenv("MAX_RETRY_AFTER", "120")))
    parser.add_argument("--request-timeout", type=float, default=float(os.getenv("RPC_TIMEOUT", "20")))
    parser.add_argument("--connect-timeout", type=float, default=float(os.getenv("CONNECT_TIMEOUT", "5")))
    parser.add_argument("--pool-connections", type=int, default=int(os.getenv("POOL_CONNECTIONS", "8")))
    parser.add_argument("--pool-maxsize", type=int, default=int(os.getenv("POOL_MAXSIZE", "8")))
    parser.add_argument(
        "--rpc-concurrency-per-node",
        type=int,
        default=int(os.getenv("RPC_CONCURRENCY_PER_NODE", "0")),
        help="0 means unlimited; useful for providers with strict concurrent-request caps",
    )
    parser.add_argument(
        "--contract-batch-size",
        type=int,
        default=int(os.getenv("CONTRACT_BATCH_SIZE", "80")),
    )
    parser.add_argument(
        "--writer-batch-size",
        type=int,
        default=int(os.getenv("WRITER_BATCH_SIZE", os.getenv("BATCH_SIZE", "250"))),
    )
    parser.add_argument(
        "--writer-flush-seconds",
        type=float,
        default=float(os.getenv("WRITER_FLUSH_SECONDS", "2")),
    )
    parser.add_argument("--progress-every", type=int, default=int(os.getenv("PROGRESS_EVERY", "200")))
    parser.add_argument(
        "--expected-chain-id",
        default=os.getenv("EXPECTED_CHAIN_ID", ""),
        help="Decimal or 0x-prefixed chain id",
    )
    parser.add_argument(
        "--max-rpc-lag-blocks",
        type=int,
        default=int(os.getenv("MAX_RPC_LAG_BLOCKS", "0")),
        help="Disable RPCs lagging this many blocks behind the best node; 0 disables the check",
    )
    parser.add_argument(
        "--block-tag",
        type=validate_block_tag,
        default=validate_block_tag(os.getenv("BLOCK_TAG", "latest")),
        help="Block tag used for eth_call and eth_getCode",
    )
    parser.add_argument(
        "--skip-contract-validation",
        action="store_true",
        default=env_bool("SKIP_CONTRACT_VALIDATION"),
    )
    parser.add_argument(
        "--skip-batch-probe",
        action="store_true",
        default=env_bool("SKIP_BATCH_PROBE"),
        help="Do not verify that each RPC supports JSON-RPC batches at startup",
    )
    parser.add_argument(
        "--allow-partial-false",
        action="store_true",
        default=env_bool("ALLOW_PARTIAL_FALSE"),
        help="UNSAFE: allow false even if some contract calls failed",
    )
    parser.add_argument(
        "--resume-failed",
        action="store_true",
        default=env_bool("RESUME_FAILED"),
        help="Treat addresses already present in failed CSV as completed",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        default=env_bool("FRESH_RUN"),
        help="Delete prior output/failed/state and start a new scan",
    )
    parser.add_argument(
        "--force-resume-mismatch",
        action="store_true",
        default=env_bool("FORCE_RESUME_MISMATCH"),
        help="UNSAFE: reuse existing CSV rows even when scan fingerprint differs",
    )
    parser.add_argument(
        "--fsync-writes",
        action="store_true",
        default=env_bool("FSYNC_WRITES"),
        help="fsync CSV/state writes for stronger crash durability at some throughput cost",
    )
    parser.add_argument(
        "--user-agent",
        default=os.getenv("USER_AGENT", f"erc721-ownership-checker/{VERSION}"),
    )
    args = parser.parse_args(argv)

    rpc_urls = parse_rpc_urls(args.rpc_url)
    if not rpc_urls:
        parser.error("RPC URL is empty. Set RPC_URLS/RPC_URL/INFURA_URL or pass --rpc-url")

    threads = max(1, args.threads)
    max_inflight = args.max_inflight if args.max_inflight > 0 else threads * 4
    base_delay = max(0.0, args.base_delay)
    max_delay = max(base_delay, args.max_delay)

    output_file = Path(args.output)
    failed_file = Path(args.failed)
    state_file = Path(args.state) if str(args.state).strip() else Path(str(output_file) + ".state.json")

    if output_file == failed_file:
        parser.error("--output and --failed must point to different files")
    if args.contract_batch_size > 1000:
        logging.warning("Very large RPC batches may be rejected by providers")

    try:
        expected_chain_id = parse_optional_int(args.expected_chain_id)
    except ValueError as exc:
        parser.error(f"invalid --expected-chain-id: {exc}")

    return Config(
        rpc_urls=rpc_urls,
        input_file=Path(args.input),
        contracts_file=Path(args.contracts),
        output_file=output_file,
        failed_file=failed_file,
        state_file=state_file,
        log_file=Path(args.log),
        log_level=str(args.log_level),
        threads=threads,
        max_inflight=max(1, max_inflight),
        max_retries=max(1, args.max_retries),
        health_retries=max(1, args.health_retries),
        base_delay=base_delay,
        max_delay=max_delay,
        max_retry_after=max(0.0, args.max_retry_after),
        request_timeout=max(0.1, args.request_timeout),
        connect_timeout=max(0.1, args.connect_timeout),
        pool_connections=max(1, args.pool_connections),
        pool_maxsize=max(1, args.pool_maxsize),
        rpc_concurrency_per_node=max(0, args.rpc_concurrency_per_node),
        contract_batch_size=max(1, args.contract_batch_size),
        writer_batch_size=max(1, args.writer_batch_size),
        writer_flush_seconds=max(0.1, args.writer_flush_seconds),
        progress_every=max(1, args.progress_every),
        skip_contract_validation=bool(args.skip_contract_validation),
        skip_batch_probe=bool(args.skip_batch_probe),
        expected_chain_id=expected_chain_id,
        max_rpc_lag_blocks=max(0, args.max_rpc_lag_blocks),
        block_tag=str(args.block_tag),
        allow_partial_false=bool(args.allow_partial_false),
        resume_failed=bool(args.resume_failed),
        fresh=bool(args.fresh),
        force_resume_mismatch=bool(args.force_resume_mismatch),
        fsync_writes=bool(args.fsync_writes),
        user_agent=str(args.user_agent),
    )


def validate_paths(cfg: Config) -> None:
    if not cfg.input_file.is_file():
        raise FileNotFoundError(f"Input file not found: {cfg.input_file}")
    if not cfg.contracts_file.is_file():
        raise FileNotFoundError(f"Contracts file not found: {cfg.contracts_file}")

    output_paths = [
        cfg.output_file.resolve(),
        cfg.failed_file.resolve(),
        cfg.state_file.resolve(),
        cfg.log_file.resolve(),
    ]
    if len(set(output_paths)) != len(output_paths):
        raise ValueError("Output, failed, state, and log files must all be different")

    resolved_outputs = set(output_paths)
    if cfg.input_file.resolve() in resolved_outputs or cfg.contracts_file.resolve() in resolved_outputs:
        raise ValueError("Input/contract file must not be reused as output/state/log file")


def main(argv: Optional[Sequence[str]] = None) -> int:
    global RPC_POOL

    cfg = parse_args(argv)
    setup_logging(cfg.log_file, cfg.log_level)
    install_signal_handlers()
    validate_paths(cfg)
    RPC_POOL = RpcPool(
        cfg.rpc_urls,
        max_concurrency_per_node=cfg.rpc_concurrency_per_node,
    )

    logging.info("ERC-721 ownership checker v%s", VERSION)
    chain_id = validate_rpc_nodes(cfg)

    addresses = validate_addresses(iter_clean_lines(cfg.input_file), label="wallet address")
    contracts = validate_addresses(iter_clean_lines(cfg.contracts_file), label="contract address")

    if not addresses:
        logging.info("No valid wallet addresses found")
        return 0
    if not contracts:
        logging.info("No valid contract addresses found")
        return 0

    completed = prepare_resume(cfg, chain_id=chain_id, contracts=contracts)
    if completed:
        before = len(addresses)
        addresses = [address for address in addresses if address not in completed]
        logging.info("Resume mode: skipped %d completed wallet(s)", before - len(addresses))

    if not addresses:
        logging.info("Nothing to do")
        return 0

    contracts = filter_contracts(cfg, contracts)
    if not contracts:
        raise RuntimeError("No usable contract addresses remain")

    # Startup probes and contract validation are useful operational work but
    # should not distort the wallet-scan throughput/retry counters.
    require_rpc_pool().metrics.reset()

    logging.info(
        "Start | chain_id=%d | wallets=%d | contracts=%d | rpc_nodes=%d | threads=%d | "
        "max_inflight=%d | batch=%d | block_tag=%s | per_rpc_concurrency=%s",
        chain_id,
        len(addresses),
        len(contracts),
        len(require_rpc_pool().active_nodes()),
        cfg.threads,
        cfg.max_inflight,
        cfg.contract_batch_size,
        cfg.block_tag,
        cfg.rpc_concurrency_per_node or "unlimited",
    )
    if cfg.allow_partial_false:
        logging.warning("ALLOW_PARTIAL_FALSE is enabled: incomplete checks may be written as false")
    if cfg.resume_failed:
        logging.warning("RESUME_FAILED is enabled: previous uncertain rows will be skipped")

    started = time.monotonic()
    total = len(addresses)
    stats = Stats()
    write_queue: "queue.Queue[CheckResult | object]" = queue.Queue(
        maxsize=max(4, cfg.writer_batch_size * 4)
    )
    writer_errors: list[BaseException] = []
    writer = threading.Thread(
        target=writer_loop,
        args=(cfg, write_queue, writer_errors),
        name="csv-writer",
        daemon=False,
    )
    writer.start()

    address_iter = iter(addresses)
    pending: dict[Future[CheckResult], str] = {}

    def submit_next(executor: ThreadPoolExecutor) -> bool:
        if STOP_EVENT.is_set():
            return False
        try:
            wallet = next(address_iter)
        except StopIteration:
            return False
        future = executor.submit(check_wallet, cfg, wallet, contracts)
        pending[future] = wallet
        return True

    try:
        with ThreadPoolExecutor(
            max_workers=cfg.threads,
            thread_name_prefix="wallet",
        ) as executor:
            while len(pending) < min(cfg.max_inflight, total) and submit_next(executor):
                pass

            while pending:
                if writer_errors:
                    raise RuntimeError("CSV writer failed") from writer_errors[0]

                done, _ = wait(
                    tuple(pending),
                    timeout=0.5,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    if STOP_EVENT.is_set():
                        for future in pending:
                            future.cancel()
                    continue

                for future in done:
                    wallet = pending.pop(future)
                    if future.cancelled():
                        continue
                    try:
                        result = future.result()
                    except CancelledError:
                        continue
                    except Exception as exc:
                        logging.exception("Worker crashed for %s", wallet)
                        result = CheckResult(
                            wallet,
                            None,
                            0,
                            len(contracts),
                            len(contracts),
                            f"worker_crashed: {truncate_error(exc)}",
                        )

                    log_progress(
                        cfg,
                        stats,
                        result,
                        write_queue,
                        writer,
                        writer_errors,
                        total,
                        started,
                    )
                    submit_next(executor)

                if STOP_EVENT.is_set():
                    for future in pending:
                        future.cancel()
    finally:
        if writer.is_alive() and not writer_errors:
            while True:
                try:
                    write_queue.put(WRITE_SENTINEL, timeout=0.25)
                    break
                except queue.Full:
                    if writer_errors or not writer.is_alive():
                        break
        writer.join(timeout=30)
        if writer.is_alive():
            logging.error("CSV writer did not stop cleanly")
        close_all_sessions()

    if writer_errors:
        raise RuntimeError("CSV writer failed") from writer_errors[0]

    elapsed = max(0.001, time.monotonic() - started)
    with stats.lock:
        done_count = stats.confirmed + stats.uncertain
        confirmed = stats.confirmed
        owners = stats.owners
        uncertain = stats.uncertain

    rpc = require_rpc_pool().metrics.snapshot()
    logging.info(
        "DONE | processed=%d/%d | confirmed=%d | owners=%d (%.2f%% of confirmed) | uncertain=%d | "
        "%.2fs | %.1f wallet/s | HTTP=%d | RPC calls=%d | retried=%d | throttles=%d | "
        "missing=%d | retryable_rpc_errors=%d | splits=%d",
        done_count,
        total,
        confirmed,
        owners,
        owners / confirmed * 100 if confirmed else 0.0,
        uncertain,
        elapsed,
        done_count / elapsed,
        rpc["http_requests"],
        rpc["rpc_calls_sent"],
        rpc["retried_rpc_calls"],
        rpc["throttle_events"],
        rpc["missing_responses"],
        rpc["retryable_rpc_errors"],
        rpc["batch_splits"],
    )

    if STOP_EVENT.is_set() and done_count < total:
        logging.warning("Stopped early; rerun with the same scan definition to resume")
        return 130
    if uncertain:
        logging.warning(
            "Uncertain rows were written to %s and were not treated as false",
            cfg.failed_file,
        )
    return 0


if __name__ == "__main__":
    started_at = time.monotonic()
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        STOP_EVENT.set()
        raise SystemExit(130)
    except Exception:
        logging.exception("Fatal error")
        raise SystemExit(1)
    finally:
        close_all_sessions()
        print(f"Finished in {time.monotonic() - started_at:.2f}s")
