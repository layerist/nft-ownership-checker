#!/usr/bin/env python3
"""
High-performance ERC-721 ownership checker, batch JSON-RPC edition.

Reads wallet addresses and ERC-721 contract addresses, then checks whether each
wallet owns at least one NFT from any supplied contract.

Why this version is faster/safer than a naive balanceOf loop:
- Uses JSON-RPC batch eth_call instead of one HTTP request per balanceOf.
- Supports multiple RPC URLs with simple health scoring and cooldowns.
- Keeps one HTTP session per thread per RPC URL.
- Bounded in-flight wallet tasks, so huge inputs do not eat RAM.
- Dedicated CSV writer thread.
- Separates confirmed false from uncertain failures.
- Resumable output.

Input files by default:
- input_addresses.txt
- nft_contracts.txt

Output files by default:
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
from typing import Any, Iterable, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, Timeout
from web3 import Web3

BALANCE_OF_SELECTOR = "70a08231"  # balanceOf(address)
WRITE_SENTINEL = object()

stop_event = threading.Event()
thread_local = threading.local()

stats_lock = threading.Lock()
checked_count = 0
owned_count = 0
failed_count = 0


# ==========================================================
# CONFIG / DATA TYPES
# ==========================================================

@dataclass(frozen=True)
class Config:
    rpc_urls: list[str]
    input_file: Path
    contracts_file: Path
    output_file: Path
    failed_file: Path
    log_file: Path
    threads: int
    max_inflight: int
    max_retries: int
    base_delay: float
    request_timeout: float
    pool_connections: int
    pool_maxsize: int
    contract_batch_size: int
    writer_batch_size: int
    progress_every: int
    min_confirmed_contracts: int
    skip_contract_validation: bool


@dataclass(frozen=True)
class CheckResult:
    address: str
    owns_nft: Optional[bool]
    checked_contracts: int
    failed_contracts: int
    error: str = ""


@dataclass
class RpcNode:
    url: str
    lock: threading.Lock = field(default_factory=threading.Lock)
    cooldown_until: float = 0.0
    failures: int = 0
    latency_ema: float = 0.30
    consecutive_throttles: int = 0

    def score(self, now: float) -> float:
        if self.cooldown_until > now:
            return 10_000 + (self.cooldown_until - now)
        return self.latency_ema + self.failures * 0.25 + self.consecutive_throttles * 0.50


class RpcPool:
    def __init__(self, urls: list[str]) -> None:
        self.nodes = [RpcNode(url=u) for u in urls]
        self._rr = itertools.count()

    def choose(self) -> RpcNode:
        now = time.time()
        # Add tiny round-robin jitter to avoid all threads hammering the same best node.
        offset = next(self._rr) % max(1, len(self.nodes))
        rotated = self.nodes[offset:] + self.nodes[:offset]
        return min(rotated, key=lambda n: n.score(now))

    @staticmethod
    def mark_success(node: RpcNode, latency: float) -> None:
        with node.lock:
            node.failures = max(0, node.failures - 1)
            node.consecutive_throttles = 0
            node.latency_ema = node.latency_ema * 0.85 + latency * 0.15

    @staticmethod
    def mark_failure(node: RpcNode, *, throttled: bool) -> None:
        with node.lock:
            node.failures = min(20, node.failures + 1)
            if throttled:
                node.consecutive_throttles = min(20, node.consecutive_throttles + 1)
                cooldown = min(30.0, 0.75 * (2 ** min(5, node.consecutive_throttles)))
            else:
                cooldown = min(10.0, 0.25 * node.failures)
            node.cooldown_until = max(node.cooldown_until, time.time() + cooldown)


RPC_POOL: RpcPool
CFG: Config


# ==========================================================
# LOGGING / SIGNALS
# ==========================================================

def setup_logging(log_file: Path) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def install_signal_handlers() -> None:
    def handler(signum: int, _frame: object) -> None:
        logging.warning("Signal %s received; stopping after in-flight tasks...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handler)


# ==========================================================
# HTTP / JSON-RPC
# ==========================================================

def get_session(cfg: Config, url: str) -> requests.Session:
    if not hasattr(thread_local, "sessions"):
        thread_local.sessions = {}

    sessions: dict[str, requests.Session] = thread_local.sessions
    session = sessions.get(url)
    if session is None:
        session = requests.Session()
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


def is_throttle_text(text: str) -> bool:
    msg = text.lower()
    return any(
        token in msg
        for token in (
            "429",
            "rate limit",
            "rate-limit",
            "too many requests",
            "project id request rate exceeded",
            "daily request count exceeded",
            "capacity exceeded",
        )
    )


def is_retryable_text(text: str) -> bool:
    msg = text.lower()
    return is_throttle_text(msg) or any(
        token in msg
        for token in (
            "timeout",
            "temporarily unavailable",
            "connection",
            "server error",
            "bad gateway",
            "gateway timeout",
            "503",
            "502",
            "500",
            "econnreset",
            "read timed out",
        )
    )


def rpc_batch(cfg: Config, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not payload:
        return []

    delay = cfg.base_delay
    last_error = "unknown RPC error"

    for attempt in range(1, cfg.max_retries + 1):
        if stop_event.is_set():
            raise RuntimeError("stopped")

        node = RPC_POOL.choose()
        session = get_session(cfg, node.url)
        started = time.time()

        try:
            response = session.post(node.url, json=payload, timeout=cfg.request_timeout)
            latency = time.time() - started

            if response.status_code != 200:
                body = response.text[:500]
                last_error = f"HTTP {response.status_code}: {body}"
                throttled = response.status_code == 429 or is_throttle_text(body)
                RPC_POOL.mark_failure(node, throttled=throttled)
                raise RuntimeError(last_error)

            data = response.json()
            if not isinstance(data, list):
                last_error = f"RPC returned non-batch response: {str(data)[:500]}"
                RPC_POOL.mark_failure(node, throttled=is_throttle_text(last_error))
                raise RuntimeError(last_error)

            RPC_POOL.mark_success(node, latency)
            return data

        except (RequestException, Timeout, json.JSONDecodeError, RuntimeError) as exc:
            last_error = str(exc)[:500]
            throttled = is_throttle_text(last_error)
            RPC_POOL.mark_failure(node, throttled=throttled)

            if attempt >= cfg.max_retries or not is_retryable_text(last_error):
                raise RuntimeError(last_error) from exc

            time.sleep(delay * (1.0 + random.random() * 0.45))
            delay *= 1.7

    raise RuntimeError(last_error)


def make_rpc_call(call_id: int, method: str, params: list[Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": call_id, "method": method, "params": params}


def response_by_id(responses: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    mapped: dict[int, dict[str, Any]] = {}
    for item in responses:
        try:
            mapped[int(item.get("id"))] = item
        except Exception:
            continue
    return mapped


# ==========================================================
# INPUT / CSV
# ==========================================================

def iter_clean_lines(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            yield line


def unique_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def validate_addresses(items: Iterable[str], *, label: str) -> list[str]:
    valid: list[str] = []
    invalid = 0

    for item in items:
        # Allow comma/semicolon separated accidental exports: take the first token.
        token = item.replace(";", ",").split(",", 1)[0].strip()
        if Web3.is_address(token):
            valid.append(Web3.to_checksum_address(token))
        else:
            invalid += 1

    valid = unique_preserve_order(valid)
    if invalid:
        logging.warning("Skipped invalid %s line(s): %d", label, invalid)
    return valid


def ensure_csv_header(path: Path, header: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow(header)


def load_completed(output_file: Path) -> set[str]:
    if not output_file.exists() or output_file.stat().st_size == 0:
        return set()

    completed: set[str] = set()
    with output_file.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        address_col = header.index("address") if header and "address" in header else 0

        for row in reader:
            if len(row) <= address_col:
                continue
            address = row[address_col].strip()
            if Web3.is_address(address):
                completed.add(Web3.to_checksum_address(address))
    return completed


def chunked(items: list[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ==========================================================
# CONTRACT / BALANCE LOGIC
# ==========================================================

def balance_of_calldata(wallet: str) -> str:
    clean = wallet.lower().removeprefix("0x")
    return "0x" + BALANCE_OF_SELECTOR + clean.rjust(64, "0")


def parse_uint256_hex(value: Any) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"bad hex result: {value!r}")
    if value == "0x":
        return 0
    return int(value, 16)


def filter_contracts(cfg: Config, contracts: list[str]) -> list[str]:
    if cfg.skip_contract_validation:
        logging.info("Contract bytecode validation skipped")
        return contracts

    valid: list[str] = []
    logging.info("Checking contract bytecode: %d candidate(s)", len(contracts))

    for batch in chunked(contracts, cfg.contract_batch_size):
        payload = [make_rpc_call(i, "eth_getCode", [addr, "latest"]) for i, addr in enumerate(batch)]
        try:
            responses = response_by_id(rpc_batch(cfg, payload))
        except Exception as exc:
            logging.warning("Contract validation batch failed; keeping batch as uncertain-valid: %s", exc)
            valid.extend(batch)
            continue

        for i, addr in enumerate(batch):
            item = responses.get(i)
            if not item:
                logging.warning("No eth_getCode response for %s; keeping as uncertain-valid", addr)
                valid.append(addr)
                continue
            if "error" in item:
                logging.warning("eth_getCode error for %s; keeping as uncertain-valid: %s", addr, item["error"])
                valid.append(addr)
                continue
            code = item.get("result")
            if isinstance(code, str) and code not in ("0x", "0x0"):
                valid.append(addr)
            else:
                logging.warning("Not a contract, skipped: %s", addr)

    valid = unique_preserve_order(valid)
    logging.info("Verified/kept contracts: %d/%d", len(valid), len(contracts))
    return valid


def check_wallet(cfg: Config, wallet: str, contracts: list[str]) -> CheckResult:
    checked = 0
    failed = 0
    first_error = ""
    calldata = balance_of_calldata(wallet)

    for batch in chunked(contracts, cfg.contract_batch_size):
        if stop_event.is_set():
            return CheckResult(wallet, None, checked, failed, "stopped")

        payload = [
            make_rpc_call(
                i,
                "eth_call",
                [{"to": contract, "data": calldata}, "latest"],
            )
            for i, contract in enumerate(batch)
        ]

        try:
            responses = response_by_id(rpc_batch(cfg, payload))
        except Exception as exc:
            failed += len(batch)
            if not first_error:
                first_error = str(exc)[:500]
            continue

        # If the provider returned rate-limit style errors inside every response, retrying
        # the whole request would have happened at HTTP level. Here we mark individual
        # contract calls as failed instead of silently treating them as zero balances.
        for i, _contract in enumerate(batch):
            item = responses.get(i)
            if not item:
                failed += 1
                if not first_error:
                    first_error = "missing RPC response item"
                continue

            if "error" in item:
                failed += 1
                if not first_error:
                    first_error = str(item["error"])[:500]
                continue

            try:
                balance = parse_uint256_hex(item.get("result"))
            except Exception as exc:
                failed += 1
                if not first_error:
                    first_error = str(exc)[:500]
                continue

            checked += 1
            if balance > 0:
                return CheckResult(wallet, True, checked, failed, "")

    if checked < cfg.min_confirmed_contracts and failed > 0:
        return CheckResult(wallet, None, checked, failed, first_error or "too few confirmed checks")

    if checked == 0 and failed > 0:
        return CheckResult(wallet, None, checked, failed, first_error or "all checks failed")

    return CheckResult(wallet, False, checked, failed, first_error if failed else "")


# ==========================================================
# WRITER / PROGRESS
# ==========================================================

def writer_loop(cfg: Config, q: "queue.Queue[CheckResult | object]") -> None:
    ensure_csv_header(cfg.output_file, ["address", "owns_nft", "checked_contracts", "failed_contracts"])
    ensure_csv_header(cfg.failed_file, ["address", "checked_contracts", "failed_contracts", "error"])

    ok_buffer: list[list[object]] = []
    failed_buffer: list[list[object]] = []

    def flush() -> None:
        nonlocal ok_buffer, failed_buffer
        if ok_buffer:
            with cfg.output_file.open("a", encoding="utf-8", newline="") as f:
                csv.writer(f).writerows(ok_buffer)
            ok_buffer = []
        if failed_buffer:
            with cfg.failed_file.open("a", encoding="utf-8", newline="") as f:
                csv.writer(f).writerows(failed_buffer)
            failed_buffer = []

    while True:
        item = q.get()
        try:
            if item is WRITE_SENTINEL:
                flush()
                return

            assert isinstance(item, CheckResult)
            if item.owns_nft is None:
                failed_buffer.append([item.address, item.checked_contracts, item.failed_contracts, item.error])
            else:
                ok_buffer.append([item.address, str(item.owns_nft).lower(), item.checked_contracts, item.failed_contracts])

            if len(ok_buffer) + len(failed_buffer) >= cfg.writer_batch_size:
                flush()
        finally:
            q.task_done()


def handle_result(result: CheckResult, q: "queue.Queue[CheckResult | object]", total: int, start: float) -> None:
    global checked_count, owned_count, failed_count

    q.put(result)

    with stats_lock:
        if result.owns_nft is None:
            failed_count += 1
        else:
            checked_count += 1
            owned_count += int(result.owns_nft)

        done = checked_count + failed_count
        checked = checked_count
        owners = owned_count
        failed = failed_count

    if done % cfg_progress_every() == 0 or done == total:
        elapsed = max(0.001, time.time() - start)
        speed = done / elapsed
        eta = (total - done) / speed if speed else 0.0
        logging.info(
            "Progress %d/%d (%.2f%%) | %.1f wallet/s | checked=%d | owners=%d | failed=%d | ETA %.1fs",
            done,
            total,
            done / total * 100,
            speed,
            checked,
            owners,
            failed,
            eta,
        )


def cfg_progress_every() -> int:
    return max(1, CFG.progress_every)


# ==========================================================
# ARGUMENTS / MAIN
# ==========================================================

def parse_rpc_urls(raw_values: list[str]) -> list[str]:
    urls: list[str] = []
    for raw in raw_values:
        for part in raw.split(","):
            url = part.strip()
            if url:
                urls.append(url)
    return unique_preserve_order(urls)


def parse_args() -> Config:
    default_rpc = os.getenv("RPC_URLS", "").strip() or os.getenv("INFURA_URL", "").strip()

    parser = argparse.ArgumentParser(description="Batch ERC-721 ownership checker")
    parser.add_argument("--rpc-url", action="append", default=[default_rpc] if default_rpc else [], help="RPC URL. Can be repeated or comma-separated. Env: RPC_URLS or INFURA_URL")
    parser.add_argument("--input", default=os.getenv("INPUT_FILE", "input_addresses.txt"))
    parser.add_argument("--contracts", default=os.getenv("CONTRACTS_FILE", "nft_contracts.txt"))
    parser.add_argument("--output", default=os.getenv("OUTPUT_FILE", "nft_owners.csv"))
    parser.add_argument("--failed", default=os.getenv("FAILED_FILE", "nft_owners_failed.csv"))
    parser.add_argument("--log", default=os.getenv("LOG_FILE", "nft_checker.log"))
    parser.add_argument("--threads", type=int, default=int(os.getenv("NUM_THREADS", "32")))
    parser.add_argument("--max-inflight", type=int, default=int(os.getenv("MAX_INFLIGHT", "0")))
    parser.add_argument("--max-retries", type=int, default=int(os.getenv("MAX_RETRIES", "5")))
    parser.add_argument("--base-delay", type=float, default=float(os.getenv("BASE_DELAY", "0.25")))
    parser.add_argument("--request-timeout", type=float, default=float(os.getenv("RPC_TIMEOUT", "20")))
    parser.add_argument("--pool-connections", type=int, default=int(os.getenv("POOL_CONNECTIONS", "128")))
    parser.add_argument("--pool-maxsize", type=int, default=int(os.getenv("POOL_MAXSIZE", "128")))
    parser.add_argument("--contract-batch-size", type=int, default=int(os.getenv("CONTRACT_BATCH_SIZE", "80")))
    parser.add_argument("--writer-batch-size", type=int, default=int(os.getenv("WRITER_BATCH_SIZE", os.getenv("BATCH_SIZE", "500"))))
    parser.add_argument("--progress-every", type=int, default=int(os.getenv("PROGRESS_EVERY", "200")))
    parser.add_argument("--min-confirmed-contracts", type=int, default=int(os.getenv("MIN_CONFIRMED_CONTRACTS", "1")))
    parser.add_argument("--skip-contract-validation", action="store_true", default=os.getenv("SKIP_CONTRACT_VALIDATION", "0") == "1")
    args = parser.parse_args()

    rpc_urls = parse_rpc_urls(args.rpc_url)
    if not rpc_urls:
        raise EnvironmentError("RPC URL is empty. Set RPC_URLS/INFURA_URL or pass --rpc-url")

    threads = max(1, args.threads)
    max_inflight = args.max_inflight if args.max_inflight > 0 else threads * 4

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
        request_timeout=max(1.0, args.request_timeout),
        pool_connections=max(1, args.pool_connections),
        pool_maxsize=max(1, args.pool_maxsize),
        contract_batch_size=max(1, args.contract_batch_size),
        writer_batch_size=max(1, args.writer_batch_size),
        progress_every=max(1, args.progress_every),
        min_confirmed_contracts=max(0, args.min_confirmed_contracts),
        skip_contract_validation=bool(args.skip_contract_validation),
    )


def main() -> None:
    global CFG, RPC_POOL

    CFG = parse_args()
    RPC_POOL = RpcPool(CFG.rpc_urls)
    setup_logging(CFG.log_file)
    install_signal_handlers()

    if not CFG.input_file.exists():
        raise FileNotFoundError(f"Input file not found: {CFG.input_file}")
    if not CFG.contracts_file.exists():
        raise FileNotFoundError(f"Contracts file not found: {CFG.contracts_file}")

    addresses = validate_addresses(iter_clean_lines(CFG.input_file), label="wallet address")
    contracts = validate_addresses(iter_clean_lines(CFG.contracts_file), label="contract address")

    if not addresses:
        logging.info("No valid wallet addresses found.")
        return
    if not contracts:
        logging.info("No valid contract addresses found.")
        return

    completed = load_completed(CFG.output_file)
    if completed:
        before = len(addresses)
        addresses = [a for a in addresses if a not in completed]
        logging.info("Resume mode: skipped %d completed wallet(s)", before - len(addresses))

    if not addresses:
        logging.info("Nothing to do.")
        return

    contracts = filter_contracts(CFG, contracts)
    if not contracts:
        raise RuntimeError("No usable contract addresses left. Check RPC/network/contract list.")

    logging.info(
        "Start | wallets=%d | contracts=%d | rpc_nodes=%d | threads=%d | max_inflight=%d | contract_batch_size=%d",
        len(addresses),
        len(contracts),
        len(CFG.rpc_urls),
        CFG.threads,
        CFG.max_inflight,
        CFG.contract_batch_size,
    )

    start = time.time()
    total = len(addresses)
    write_queue: "queue.Queue[CheckResult | object]" = queue.Queue(maxsize=CFG.writer_batch_size * 4)
    writer = threading.Thread(target=writer_loop, args=(CFG, write_queue), name="csv-writer", daemon=True)
    writer.start()

    address_iter = iter(addresses)
    pending: set[Future[CheckResult]] = set()

    def submit_next(executor: ThreadPoolExecutor) -> bool:
        try:
            wallet = next(address_iter)
        except StopIteration:
            return False
        pending.add(executor.submit(check_wallet, CFG, wallet, contracts))
        return True

    try:
        with ThreadPoolExecutor(max_workers=CFG.threads) as executor:
            while len(pending) < min(CFG.max_inflight, total):
                if not submit_next(executor):
                    break

            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        result = future.result()
                    except Exception as exc:  # noqa: BLE001
                        logging.exception("Worker crashed: %s", exc)
                        result = CheckResult("", None, 0, 0, f"worker_crashed:{str(exc)[:500]}")
                    handle_result(result, write_queue, total, start)

                    if not stop_event.is_set():
                        submit_next(executor)
    finally:
        write_queue.put(WRITE_SENTINEL)
        write_queue.join()
        writer.join(timeout=30)

    elapsed = max(0.001, time.time() - start)
    done = checked_count + failed_count
    logging.info(
        "DONE | done=%d/%d | checked=%d | owners=%d (%.2f%% of checked) | failed=%d | time=%.2fs | %.1f wallet/s",
        done,
        total,
        checked_count,
        owned_count,
        owned_count / checked_count * 100 if checked_count else 0.0,
        failed_count,
        elapsed,
        done / elapsed,
    )

    if failed_count:
        logging.warning("Uncertain rows were written separately, not as false: %s", CFG.failed_file)


if __name__ == "__main__":
    t0 = time.time()
    try:
        main()
    finally:
        print(f"Finished in {time.time() - t0:.2f}s")
