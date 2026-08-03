#!/usr/bin/env python3
"""
Robust high-throughput ERC-721 ownership checker (JSON-RPC batch edition).

Checks whether every wallet owns at least one token in any supplied ERC-721
contract by calling balanceOf(address) through batched eth_call requests.

Key properties:
- Multiple RPC endpoints with chain-id validation, health scoring and cooldowns.
- Thread-local keep-alive sessions.
- Retries HTTP failures and retryable JSON-RPC batch failures.
- Never converts an incomplete check into a confirmed false by default.
- Bounded worker queue and dedicated CSV writer.
- Correct wallet attribution if a worker crashes.
- Resume support and graceful interruption.

Default input files:
- input_addresses.txt
- nft_contracts.txt

Default output files:
- nft_owners.csv
- nft_owners_failed.csv
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import logging
import os
import queue
import random
import signal
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from web3 import Web3

BALANCE_OF_SELECTOR = "70a08231"
WRITE_SENTINEL = object()

STOP_EVENT = threading.Event()
THREAD_LOCAL = threading.local()


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
    log_file: Path
    threads: int
    max_inflight: int
    max_retries: int
    base_delay: float
    max_delay: float
    request_timeout: float
    connect_timeout: float
    pool_connections: int
    pool_maxsize: int
    contract_batch_size: int
    writer_batch_size: int
    writer_flush_seconds: float
    progress_every: int
    skip_contract_validation: bool
    expected_chain_id: Optional[int]
    allow_partial_false: bool
    resume_failed: bool
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
class RpcNode:
    url: str
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    cooldown_until: float = 0.0
    failures: int = 0
    latency_ema: float = 0.30
    consecutive_throttles: int = 0
    disabled_reason: str = ""
    chain_id: Optional[int] = None

    def snapshot(self) -> tuple[float, int, float, int, str]:
        with self.lock:
            return (
                self.cooldown_until,
                self.failures,
                self.latency_ema,
                self.consecutive_throttles,
                self.disabled_reason,
            )

    def score(self, now: float) -> float:
        cooldown_until, failures, latency, throttles, disabled = self.snapshot()
        if disabled:
            return float("inf")
        if cooldown_until > now:
            return 10_000.0 + cooldown_until - now
        return latency + failures * 0.25 + throttles * 0.50


class RpcPool:
    def __init__(self, urls: Sequence[str]) -> None:
        if not urls:
            raise ValueError("RPC URL list is empty")
        self.nodes = [RpcNode(url=url) for url in urls]
        self._rr = itertools.count()
        self._rr_lock = threading.Lock()

    def active_nodes(self) -> list[RpcNode]:
        return [node for node in self.nodes if not node.snapshot()[4]]

    def choose(self) -> RpcNode:
        active = self.active_nodes()
        if not active:
            reasons = "; ".join(
                f"{redact_url(n.url)}: {n.snapshot()[4] or 'unavailable'}" for n in self.nodes
            )
            raise RuntimeError(f"No active RPC nodes: {reasons}")

        now = time.monotonic()
        with self._rr_lock:
            offset = next(self._rr) % len(active)
        rotated = active[offset:] + active[:offset]
        node = min(rotated, key=lambda item: item.score(now))

        cooldown_until = node.snapshot()[0]
        if cooldown_until > now:
            STOP_EVENT.wait(min(cooldown_until - now, 1.0))
            if STOP_EVENT.is_set():
                raise RuntimeError("stopped")
        return node

    @staticmethod
    def mark_success(node: RpcNode, latency: float) -> None:
        with node.lock:
            node.failures = max(0, node.failures - 1)
            node.consecutive_throttles = 0
            node.cooldown_until = 0.0
            node.latency_ema = node.latency_ema * 0.85 + latency * 0.15

    @staticmethod
    def mark_failure(node: RpcNode, *, throttled: bool) -> None:
        now = time.monotonic()
        with node.lock:
            node.failures = min(50, node.failures + 1)
            if throttled:
                node.consecutive_throttles = min(20, node.consecutive_throttles + 1)
                cooldown = min(60.0, 0.75 * (2 ** min(6, node.consecutive_throttles)))
            else:
                cooldown = min(15.0, 0.35 * node.failures)
            node.cooldown_until = max(node.cooldown_until, now + cooldown)

    @staticmethod
    def disable(node: RpcNode, reason: str) -> None:
        with node.lock:
            node.disabled_reason = reason


RPC_POOL: RpcPool


# ==========================================================
# LOGGING / SIGNALS
# ==========================================================


def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
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


def redact_url(url: str) -> str:
    try:
        parsed = requests.utils.urlparse(url)
        host = parsed.hostname or "unknown"
        port = f":{parsed.port}" if parsed.port else ""
        path = parsed.path.rstrip("/")
        if len(path) > 18:
            path = path[:8] + "…" + path[-6:]
        return f"{parsed.scheme}://{host}{port}{path}"
    except Exception:
        return "<rpc-url>"


def truncate_error(value: Any, limit: int = 500) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ")
    return text[:limit]


def backoff_delay(base: float, maximum: float, attempt: int) -> float:
    raw = min(maximum, base * (1.8 ** max(0, attempt - 1)))
    return raw * random.uniform(0.75, 1.25)


def interruptible_sleep(seconds: float) -> None:
    if seconds > 0:
        STOP_EVENT.wait(seconds)


def chunked(items: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


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
    return session


def close_thread_sessions() -> None:
    sessions = getattr(THREAD_LOCAL, "sessions", None)
    if not sessions:
        return
    for session in sessions.values():
        try:
            session.close()
        except Exception:
            pass
    sessions.clear()


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
            "502",
            "503",
            "504",
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


def should_retry_batch_response(data: list[dict[str, Any]], expected_count: int) -> tuple[bool, str]:
    if not data:
        return True, "empty JSON-RPC batch response"

    errors = [json_rpc_error_text(item) for item in data if isinstance(item, dict) and "error" in item]
    if len(errors) == expected_count and errors:
        combined = " | ".join(errors[:3])
        return is_retryable_text(combined), combined
    return False, ""


def rpc_request_to_node(
    cfg: Config,
    node: RpcNode,
    payload: dict[str, Any] | list[dict[str, Any]],
    *,
    expect_batch: bool,
) -> dict[str, Any] | list[dict[str, Any]]:
    session = get_session(cfg, node.url)
    started = time.monotonic()
    response = session.post(
        node.url,
        json=payload,
        timeout=(cfg.connect_timeout, cfg.request_timeout),
    )
    latency = time.monotonic() - started

    if response.status_code != 200:
        body = truncate_error(response.text)
        raise RuntimeError(f"HTTP {response.status_code}: {body}")

    try:
        data = response.json()
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid JSON response: {truncate_error(response.text)}") from exc

    if expect_batch and not isinstance(data, list):
        raise RuntimeError(f"RPC returned non-batch response: {truncate_error(data)}")
    if not expect_batch and not isinstance(data, dict):
        raise RuntimeError(f"RPC returned invalid single response: {truncate_error(data)}")

    RPC_POOL.mark_success(node, latency)
    return data


def rpc_batch(cfg: Config, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not payload:
        return []

    last_error = "unknown RPC error"
    attempted_nodes: set[str] = set()

    for attempt in range(1, cfg.max_retries + 1):
        if STOP_EVENT.is_set():
            raise RuntimeError("stopped")

        node = RPC_POOL.choose()
        attempted_nodes.add(node.url)

        try:
            raw = rpc_request_to_node(cfg, node, payload, expect_batch=True)
            assert isinstance(raw, list)
            data = [item for item in raw if isinstance(item, dict)]

            retry, batch_error = should_retry_batch_response(data, len(payload))
            if retry:
                raise RuntimeError(batch_error)
            return data

        except (RequestException, RuntimeError) as exc:
            last_error = truncate_error(exc)
            throttled = is_throttle_text(last_error)
            RPC_POOL.mark_failure(node, throttled=throttled)

            if attempt >= cfg.max_retries or not is_retryable_text(last_error):
                break

            logging.debug(
                "RPC batch retry %d/%d after %s from %s",
                attempt,
                cfg.max_retries,
                last_error,
                redact_url(node.url),
            )
            interruptible_sleep(backoff_delay(cfg.base_delay, cfg.max_delay, attempt))

    nodes = ", ".join(redact_url(url) for url in attempted_nodes)
    raise RuntimeError(f"{last_error}; attempted RPC nodes: {nodes}")


def rpc_single(cfg: Config, node: RpcNode, method: str, params: list[Any]) -> dict[str, Any]:
    payload = make_rpc_call(1, method, params)
    raw = rpc_request_to_node(cfg, node, payload, expect_batch=False)
    assert isinstance(raw, dict)
    return raw


def make_rpc_call(call_id: int, method: str, params: list[Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": call_id, "method": method, "params": params}


def response_by_id(responses: Iterable[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    mapped: dict[int, dict[str, Any]] = {}
    for item in responses:
        try:
            call_id = item.get("id")
            if isinstance(call_id, bool) or call_id is None:
                continue
            mapped[int(call_id)] = item
        except (TypeError, ValueError):
            continue
    return mapped


def validate_rpc_nodes(cfg: Config) -> int:
    logging.info("Validating %d RPC node(s)...", len(RPC_POOL.nodes))
    detected_chain_ids: list[int] = []

    for node in RPC_POOL.nodes:
        try:
            response = rpc_single(cfg, node, "eth_chainId", [])
            if "error" in response:
                raise RuntimeError(json_rpc_error_text(response))
            raw_chain_id = response.get("result")
            if not isinstance(raw_chain_id, str):
                raise RuntimeError(f"bad eth_chainId result: {raw_chain_id!r}")
            chain_id = int(raw_chain_id, 16)
            node.chain_id = chain_id
            detected_chain_ids.append(chain_id)
            logging.info("RPC OK: %s | chain_id=%d", redact_url(node.url), chain_id)
        except Exception as exc:
            reason = f"health check failed: {truncate_error(exc)}"
            RPC_POOL.disable(node, reason)
            logging.warning("RPC disabled: %s | %s", redact_url(node.url), reason)

    if not detected_chain_ids:
        raise RuntimeError("All RPC nodes failed health validation")

    target_chain_id = cfg.expected_chain_id
    if target_chain_id is None:
        counts: dict[int, int] = {}
        for chain_id in detected_chain_ids:
            counts[chain_id] = counts.get(chain_id, 0) + 1
        target_chain_id = max(counts, key=counts.get)

    for node in RPC_POOL.active_nodes():
        if node.chain_id != target_chain_id:
            reason = f"wrong chain_id={node.chain_id}; expected {target_chain_id}"
            RPC_POOL.disable(node, reason)
            logging.warning("RPC disabled: %s | %s", redact_url(node.url), reason)

    if not RPC_POOL.active_nodes():
        raise RuntimeError(f"No RPC nodes remain for chain_id={target_chain_id}")

    logging.info(
        "RPC pool ready: active=%d/%d | chain_id=%d",
        len(RPC_POOL.active_nodes()),
        len(RPC_POOL.nodes),
        target_chain_id,
    )
    return target_chain_id


# ==========================================================
# INPUT / CSV
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
    for delimiter in (",", ";", "\t", " "):
        if delimiter in line:
            line = line.split(delimiter, 1)[0]
    return line.strip().strip('"').strip("'")


def validate_addresses(items: Iterable[str], *, label: str) -> list[str]:
    valid: list[str] = []
    invalid_samples: list[str] = []
    invalid_count = 0

    for item in items:
        token = extract_first_token(item)
        if Web3.is_address(token):
            valid.append(Web3.to_checksum_address(token))
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
        logging.info("Removed %d duplicate %s line(s)", len(valid) - len(deduplicated), label)
    return deduplicated


def ensure_csv_header(path: Path, header: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        csv.writer(file).writerow(header)


def load_addresses_from_csv(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()

    completed: set[str] = set()
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            if not reader.fieldnames or "address" not in reader.fieldnames:
                logging.warning("Cannot resume from %s: missing address column", path)
                return set()
            for row in reader:
                address = (row.get("address") or "").strip()
                if Web3.is_address(address):
                    completed.add(Web3.to_checksum_address(address))
    except (OSError, csv.Error) as exc:
        logging.warning("Cannot read resume file %s: %s", path, exc)
    return completed


# ==========================================================
# CONTRACT / BALANCE LOGIC
# ==========================================================


def balance_of_calldata(wallet: str) -> str:
    clean = wallet.lower().removeprefix("0x")
    return "0x" + BALANCE_OF_SELECTOR + clean.rjust(64, "0")


def parse_uint256_hex(value: Any) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"bad hex result: {value!r}")
    if value in ("0x", "0x0"):
        return 0
    number = int(value, 16)
    if number < 0 or number >= 2**256:
        raise ValueError("uint256 result is out of range")
    return number


def filter_contracts(cfg: Config, contracts: list[str]) -> list[str]:
    if cfg.skip_contract_validation:
        logging.info("Contract bytecode validation skipped")
        return contracts

    valid: list[str] = []
    uncertain: list[str] = []
    logging.info("Checking contract bytecode: %d candidate(s)", len(contracts))

    for batch in chunked(contracts, cfg.contract_batch_size):
        payload = [make_rpc_call(i, "eth_getCode", [address, "latest"]) for i, address in enumerate(batch)]
        try:
            responses = response_by_id(rpc_batch(cfg, payload))
        except Exception as exc:
            logging.warning("Contract validation batch failed; keeping %d uncertain contract(s): %s", len(batch), exc)
            uncertain.extend(batch)
            continue

        for i, address in enumerate(batch):
            item = responses.get(i)
            if not item:
                uncertain.append(address)
                logging.warning("No eth_getCode response for %s; keeping it", address)
                continue
            if "error" in item:
                uncertain.append(address)
                logging.warning("eth_getCode error for %s; keeping it: %s", address, json_rpc_error_text(item))
                continue
            code = item.get("result")
            if isinstance(code, str) and code.lower() not in ("0x", "0x0", "0x00"):
                valid.append(address)
            else:
                logging.warning("EOA/empty address skipped from contract list: %s", address)

    result = unique_preserve_order(valid + uncertain)
    logging.info(
        "Contract validation done: usable=%d/%d | verified=%d | uncertain=%d",
        len(result),
        len(contracts),
        len(valid),
        len(uncertain),
    )
    return result


def check_wallet(cfg: Config, wallet: str, contracts: Sequence[str]) -> CheckResult:
    checked = 0
    failed = 0
    errors: list[str] = []
    calldata = balance_of_calldata(wallet)

    try:
        for batch in chunked(contracts, cfg.contract_batch_size):
            if STOP_EVENT.is_set():
                return CheckResult(wallet, None, checked, failed, len(contracts), "stopped")

            payload = [
                make_rpc_call(i, "eth_call", [{"to": contract, "data": calldata}, "latest"])
                for i, contract in enumerate(batch)
            ]

            try:
                responses = response_by_id(rpc_batch(cfg, payload))
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
                    return CheckResult(wallet, True, checked, failed, len(contracts))

        error_text = " | ".join(errors)
        if failed and not cfg.allow_partial_false:
            return CheckResult(wallet, None, checked, failed, len(contracts), error_text or "incomplete check")

        if checked == 0:
            return CheckResult(wallet, None, checked, failed, len(contracts), error_text or "no confirmed checks")

        return CheckResult(wallet, False, checked, failed, len(contracts), error_text)
    finally:
        # Sessions stay alive for the worker thread and are closed when the pool exits.
        pass


# ==========================================================
# WRITER / PROGRESS
# ==========================================================


def append_csv_rows(path: Path, rows: list[list[object]]) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8-sig", newline="") as file:
        csv.writer(file).writerows(rows)
        file.flush()


def writer_loop(cfg: Config, work_queue: "queue.Queue[CheckResult | object]", error_box: list[BaseException]) -> None:
    try:
        ensure_csv_header(
            cfg.output_file,
            ["address", "owns_nft", "checked_contracts", "failed_contracts", "total_contracts"],
        )
        ensure_csv_header(
            cfg.failed_file,
            ["address", "checked_contracts", "failed_contracts", "total_contracts", "error"],
        )

        ok_buffer: list[list[object]] = []
        failed_buffer: list[list[object]] = []
        last_flush = time.monotonic()

        def flush() -> None:
            nonlocal ok_buffer, failed_buffer, last_flush
            append_csv_rows(cfg.output_file, ok_buffer)
            append_csv_rows(cfg.failed_file, failed_buffer)
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
                        [item.address, item.checked_contracts, item.failed_contracts, item.total_contracts, item.error]
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
    except BaseException as exc:  # writer failure must be visible to main thread
        error_box.append(exc)
        STOP_EVENT.set()
        logging.exception("CSV writer crashed")


def log_progress(
    cfg: Config,
    stats: Stats,
    result: CheckResult,
    work_queue: "queue.Queue[CheckResult | object]",
    total: int,
    started: float,
) -> None:
    work_queue.put(result)
    done, confirmed, owners, uncertain = stats.add(result)

    if done % cfg.progress_every != 0 and done != total:
        return

    elapsed = max(0.001, time.monotonic() - started)
    speed = done / elapsed
    eta = max(0.0, (total - done) / speed) if speed else 0.0
    logging.info(
        "Progress %d/%d (%.2f%%) | %.1f wallet/s | confirmed=%d | owners=%d | uncertain=%d | ETA %.1fs",
        done,
        total,
        done / total * 100,
        speed,
        confirmed,
        owners,
        uncertain,
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


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    env_rpc = os.getenv("RPC_URLS", "").strip() or os.getenv("RPC_URL", "").strip() or os.getenv("INFURA_URL", "").strip()

    parser = argparse.ArgumentParser(description="Robust batch ERC-721 ownership checker")
    parser.add_argument("--rpc-url", action="append", default=[env_rpc] if env_rpc else [], help="Repeatable or comma-separated RPC URL")
    parser.add_argument("--input", default=os.getenv("INPUT_FILE", "input_addresses.txt"))
    parser.add_argument("--contracts", default=os.getenv("CONTRACTS_FILE", "nft_contracts.txt"))
    parser.add_argument("--output", default=os.getenv("OUTPUT_FILE", "nft_owners.csv"))
    parser.add_argument("--failed", default=os.getenv("FAILED_FILE", "nft_owners_failed.csv"))
    parser.add_argument("--log", default=os.getenv("LOG_FILE", "nft_checker.log"))
    parser.add_argument("--threads", type=int, default=int(os.getenv("NUM_THREADS", "32")))
    parser.add_argument("--max-inflight", type=int, default=int(os.getenv("MAX_INFLIGHT", "0")))
    parser.add_argument("--max-retries", type=int, default=int(os.getenv("MAX_RETRIES", "5")))
    parser.add_argument("--base-delay", type=float, default=float(os.getenv("BASE_DELAY", "0.25")))
    parser.add_argument("--max-delay", type=float, default=float(os.getenv("MAX_DELAY", "10")))
    parser.add_argument("--request-timeout", type=float, default=float(os.getenv("RPC_TIMEOUT", "20")))
    parser.add_argument("--connect-timeout", type=float, default=float(os.getenv("CONNECT_TIMEOUT", "5")))
    parser.add_argument("--pool-connections", type=int, default=int(os.getenv("POOL_CONNECTIONS", "64")))
    parser.add_argument("--pool-maxsize", type=int, default=int(os.getenv("POOL_MAXSIZE", "64")))
    parser.add_argument("--contract-batch-size", type=int, default=int(os.getenv("CONTRACT_BATCH_SIZE", "80")))
    parser.add_argument("--writer-batch-size", type=int, default=int(os.getenv("WRITER_BATCH_SIZE", os.getenv("BATCH_SIZE", "250"))))
    parser.add_argument("--writer-flush-seconds", type=float, default=float(os.getenv("WRITER_FLUSH_SECONDS", "2")))
    parser.add_argument("--progress-every", type=int, default=int(os.getenv("PROGRESS_EVERY", "200")))
    parser.add_argument("--expected-chain-id", default=os.getenv("EXPECTED_CHAIN_ID", ""), help="Decimal or 0x-prefixed chain id")
    parser.add_argument("--skip-contract-validation", action="store_true", default=env_bool("SKIP_CONTRACT_VALIDATION"))
    parser.add_argument("--allow-partial-false", action="store_true", default=env_bool("ALLOW_PARTIAL_FALSE"), help="Unsafe: allow false even if some calls failed")
    parser.add_argument("--resume-failed", action="store_true", default=env_bool("RESUME_FAILED"), help="Skip addresses already present in failed CSV")
    parser.add_argument("--user-agent", default=os.getenv("USER_AGENT", "erc721-ownership-checker/2.0"))
    args = parser.parse_args(argv)

    rpc_urls = parse_rpc_urls(args.rpc_url)
    if not rpc_urls:
        parser.error("RPC URL is empty. Set RPC_URLS/RPC_URL/INFURA_URL or pass --rpc-url")

    threads = max(1, args.threads)
    max_inflight = args.max_inflight if args.max_inflight > 0 else threads * 4

    if args.output == args.failed:
        parser.error("--output and --failed must point to different files")
    if args.contract_batch_size > 1000:
        logging.warning("Very large RPC batches may be rejected by providers")

    return Config(
        rpc_urls=rpc_urls,
        input_file=Path(args.input),
        contracts_file=Path(args.contracts),
        output_file=Path(args.output),
        failed_file=Path(args.failed),
        log_file=Path(args.log),
        threads=threads,
        max_inflight=max(1, max_inflight),
        max_retries=max(1, args.max_retries),
        base_delay=max(0.0, args.base_delay),
        max_delay=max(0.0, args.max_delay),
        request_timeout=max(0.1, args.request_timeout),
        connect_timeout=max(0.1, args.connect_timeout),
        pool_connections=max(1, args.pool_connections),
        pool_maxsize=max(1, args.pool_maxsize),
        contract_batch_size=max(1, args.contract_batch_size),
        writer_batch_size=max(1, args.writer_batch_size),
        writer_flush_seconds=max(0.1, args.writer_flush_seconds),
        progress_every=max(1, args.progress_every),
        skip_contract_validation=bool(args.skip_contract_validation),
        expected_chain_id=parse_optional_int(args.expected_chain_id),
        allow_partial_false=bool(args.allow_partial_false),
        resume_failed=bool(args.resume_failed),
        user_agent=str(args.user_agent),
    )


def validate_paths(cfg: Config) -> None:
    if not cfg.input_file.is_file():
        raise FileNotFoundError(f"Input file not found: {cfg.input_file}")
    if not cfg.contracts_file.is_file():
        raise FileNotFoundError(f"Contracts file not found: {cfg.contracts_file}")

    resolved_outputs = {cfg.output_file.resolve(), cfg.failed_file.resolve(), cfg.log_file.resolve()}
    if cfg.input_file.resolve() in resolved_outputs or cfg.contracts_file.resolve() in resolved_outputs:
        raise ValueError("Input/contract file must not be reused as output or log file")


def main(argv: Optional[Sequence[str]] = None) -> int:
    global RPC_POOL

    cfg = parse_args(argv)
    setup_logging(cfg.log_file)
    install_signal_handlers()
    validate_paths(cfg)
    RPC_POOL = RpcPool(cfg.rpc_urls)

    chain_id = validate_rpc_nodes(cfg)

    addresses = validate_addresses(iter_clean_lines(cfg.input_file), label="wallet address")
    contracts = validate_addresses(iter_clean_lines(cfg.contracts_file), label="contract address")

    if not addresses:
        logging.info("No valid wallet addresses found")
        return 0
    if not contracts:
        logging.info("No valid contract addresses found")
        return 0

    completed = load_addresses_from_csv(cfg.output_file)
    if cfg.resume_failed:
        completed |= load_addresses_from_csv(cfg.failed_file)

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

    logging.info(
        "Start | chain_id=%d | wallets=%d | contracts=%d | rpc_nodes=%d | threads=%d | max_inflight=%d | batch=%d",
        chain_id,
        len(addresses),
        len(contracts),
        len(RPC_POOL.active_nodes()),
        cfg.threads,
        cfg.max_inflight,
        cfg.contract_batch_size,
    )
    if cfg.allow_partial_false:
        logging.warning("ALLOW_PARTIAL_FALSE is enabled: incomplete checks may be written as false")

    started = time.monotonic()
    total = len(addresses)
    stats = Stats()
    write_queue: "queue.Queue[CheckResult | object]" = queue.Queue(maxsize=max(4, cfg.writer_batch_size * 4))
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
        with ThreadPoolExecutor(max_workers=cfg.threads, thread_name_prefix="wallet") as executor:
            while len(pending) < min(cfg.max_inflight, total) and submit_next(executor):
                pass

            while pending:
                if writer_errors:
                    raise RuntimeError("CSV writer failed") from writer_errors[0]

                done, _ = wait(tuple(pending), timeout=0.5, return_when=FIRST_COMPLETED)
                if not done:
                    if STOP_EVENT.is_set():
                        for future in pending:
                            future.cancel()
                    continue

                for future in done:
                    wallet = pending.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:  # defensive boundary
                        logging.exception("Worker crashed for %s", wallet)
                        result = CheckResult(wallet, None, 0, len(contracts), len(contracts), f"worker_crashed: {truncate_error(exc)}")

                    log_progress(cfg, stats, result, write_queue, total, started)
                    submit_next(executor)

                if STOP_EVENT.is_set():
                    for future in pending:
                        future.cancel()
    finally:
        # Sentinel is queued only after all produced rows, preserving write order.
        while writer.is_alive():
            try:
                write_queue.put(WRITE_SENTINEL, timeout=0.5)
                break
            except queue.Full:
                if writer_errors:
                    break
        writer.join(timeout=30)
        if writer.is_alive():
            logging.error("CSV writer did not stop within 30 seconds")
        close_thread_sessions()

    if writer_errors:
        raise RuntimeError("CSV writer failed") from writer_errors[0]

    elapsed = max(0.001, time.monotonic() - started)
    with stats.lock:
        done = stats.confirmed + stats.uncertain
        confirmed = stats.confirmed
        owners = stats.owners
        uncertain = stats.uncertain

    logging.info(
        "DONE | processed=%d/%d | confirmed=%d | owners=%d (%.2f%% of confirmed) | uncertain=%d | %.2fs | %.1f wallet/s",
        done,
        total,
        confirmed,
        owners,
        owners / confirmed * 100 if confirmed else 0.0,
        uncertain,
        elapsed,
        done / elapsed,
    )

    if STOP_EVENT.is_set() and done < total:
        logging.warning("Stopped early; rerun to resume from confirmed output")
        return 130
    if uncertain:
        logging.warning("Uncertain rows were written to %s and were not treated as false", cfg.failed_file)
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
        print(f"Finished in {time.monotonic() - started_at:.2f}s")
