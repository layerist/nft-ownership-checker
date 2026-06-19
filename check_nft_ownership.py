#!/usr/bin/env python3
"""
Fast ERC-721 ownership checker.

What it does:
- Reads wallet addresses from input_addresses.txt by default.
- Reads ERC-721 contract addresses from nft_contracts.txt by default.
- Checks whether each wallet owns at least one NFT from any supplied contract.
- Writes resumable CSV output to nft_owners.csv by default.

Main improvements over the pasted version:
- Thread-local Web3 AND thread-local Contract cache.
- Bounded in-flight futures instead of submitting everything at once.
- Dedicated writer thread, so RPC workers are not blocked by disk I/O.
- Safer retry logic with adaptive throttle penalty and decay.
- Distinguishes a real "no NFT" from RPC failure: uncertain rows go to a separate failed CSV.
- Deduplicates input while preserving order.
- CLI arguments with env defaults.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import queue
import random
import signal
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, Timeout
from web3 import Web3, exceptions
from web3.contract import Contract


# ==========================================================
# ABI
# ==========================================================

ERC721_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]


# ==========================================================
# CONFIG / STATE
# ==========================================================

@dataclass(frozen=True)
class Config:
    rpc_url: str
    input_file: Path
    contracts_file: Path
    output_file: Path
    failed_file: Path
    log_file: Path
    threads: int
    max_retries: int
    base_delay: float
    rpc_timeout: float
    pool_connections: int
    pool_maxsize: int
    writer_batch_size: int
    progress_every: int
    max_inflight: int
    max_rpc_penalty: float
    min_confirmed_contracts: int


@dataclass(frozen=True)
class CheckResult:
    address: str
    owns_nft: Optional[bool]
    checked_contracts: int
    failed_contracts: int
    error: str = ""


thread_local = threading.local()
stop_event = threading.Event()

code_cache: dict[str, bool] = {}
code_lock = threading.Lock()

rpc_penalty = 0.0
rpc_penalty_lock = threading.Lock()

stats_lock = threading.Lock()
checked_count = 0
owned_count = 0
failed_count = 0

WRITE_SENTINEL = object()


# ==========================================================
# LOGGING
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


# ==========================================================
# WEB3 / HTTP
# ==========================================================

def build_session(cfg: Config) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=cfg.pool_connections,
        pool_maxsize=cfg.pool_maxsize,
        max_retries=0,
        pool_block=True,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_w3(cfg: Config) -> Web3:
    if not hasattr(thread_local, "w3"):
        thread_local.w3 = Web3(
            Web3.HTTPProvider(
                cfg.rpc_url,
                request_kwargs={
                    "timeout": cfg.rpc_timeout,
                    "session": build_session(cfg),
                },
            )
        )
    return thread_local.w3


def get_contract(cfg: Config, address: str) -> Contract:
    # Important: Contract is bound to the Web3 instance that created it.
    # Keep contract cache thread-local, not global.
    if not hasattr(thread_local, "contract_cache"):
        thread_local.contract_cache = {}

    cache: dict[str, Contract] = thread_local.contract_cache
    contract = cache.get(address)
    if contract is None:
        contract = get_w3(cfg).eth.contract(address=address, abi=ERC721_ABI)
        cache[address] = contract
    return contract


# ==========================================================
# RETRIES / THROTTLE
# ==========================================================

def is_throttle_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
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


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, (RequestException, Timeout, ConnectionError, exceptions.TimeExhausted)):
        return True

    # web3.py often wraps JSON-RPC errors into ValueError with a dict/string payload.
    if isinstance(exc, ValueError):
        msg = str(exc).lower()
        return any(
            token in msg
            for token in (
                "timeout",
                "temporarily unavailable",
                "429",
                "rate",
                "too many requests",
                "connection",
                "server error",
                "bad gateway",
                "gateway timeout",
                "503",
                "502",
                "500",
            )
        )

    return False


def apply_rpc_penalty(cfg: Config, throttled: bool) -> None:
    global rpc_penalty
    with rpc_penalty_lock:
        if throttled:
            rpc_penalty = min(cfg.max_rpc_penalty, max(0.02, rpc_penalty * 1.35 + 0.02))
        else:
            rpc_penalty = max(0.0, rpc_penalty * 0.985 - 0.001)


def sleep_before_rpc() -> None:
    with rpc_penalty_lock:
        penalty = rpc_penalty
    if penalty > 0:
        time.sleep(penalty)


def rpc_call(cfg: Config, fn, *args, **kwargs):
    delay = cfg.base_delay
    last_exc: Optional[BaseException] = None

    for attempt in range(1, cfg.max_retries + 1):
        if stop_event.is_set():
            raise RuntimeError("stopped")

        try:
            sleep_before_rpc()
            result = fn(*args, **kwargs)
            apply_rpc_penalty(cfg, throttled=False)
            return result
        except exceptions.ContractLogicError:
            # balanceOf can revert on broken/non-standard contracts.
            # This is not usually fixed by retrying.
            raise
        except Exception as exc:  # noqa: BLE001 - we classify below
            last_exc = exc
            throttled = is_throttle_error(exc)
            retryable = is_retryable_error(exc) or throttled
            apply_rpc_penalty(cfg, throttled=throttled)

            if not retryable or attempt >= cfg.max_retries:
                raise

            jitter = 1.0 + random.random() * 0.45
            time.sleep(delay * jitter)
            delay *= 1.7

    assert last_exc is not None
    raise last_exc


# ==========================================================
# IO HELPERS
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
        if Web3.is_address(item):
            valid.append(Web3.to_checksum_address(item))
        else:
            invalid += 1

    valid = unique_preserve_order(valid)

    if invalid:
        logging.warning("Skipped invalid %s lines: %d", label, invalid)

    return valid


def load_completed(output_file: Path) -> set[str]:
    if not output_file.exists():
        return set()

    completed: set[str] = set()
    with output_file.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        address_col = 0
        if header and "address" in header:
            address_col = header.index("address")

        for row in reader:
            if not row or len(row) <= address_col:
                continue
            address = row[address_col].strip()
            if Web3.is_address(address):
                completed.add(Web3.to_checksum_address(address))

    return completed


def ensure_csv_header(path: Path, header: list[str]) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow(header)


# ==========================================================
# CONTRACT CHECKS
# ==========================================================

def is_contract(cfg: Config, address: str) -> bool:
    with code_lock:
        cached = code_cache.get(address)
    if cached is not None:
        return cached

    code = rpc_call(cfg, get_w3(cfg).eth.get_code, address)
    result = code not in (b"", b"\x00")

    with code_lock:
        code_cache[address] = result
    return result


def filter_contracts(cfg: Config, contracts: list[str]) -> list[str]:
    if not contracts:
        return []

    valid: list[str] = []
    logging.info("Checking contract bytecode: %d candidate(s)", len(contracts))

    # Contract list is usually much smaller than wallet list.
    # Keep this bounded and fail-loud enough to avoid silently checking nothing.
    with ThreadPoolExecutor(max_workers=min(cfg.threads, max(1, len(contracts)))) as executor:
        future_to_contract = {executor.submit(is_contract, cfg, c): c for c in contracts}
        for future in wait_iter(future_to_contract):
            contract = future_to_contract[future]
            try:
                if future.result():
                    valid.append(contract)
                else:
                    logging.warning("Not a contract, skipped: %s", contract)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Could not verify contract %s: %s", contract, exc)

    return valid


# ==========================================================
# CHECK LOGIC
# ==========================================================

def balance_of(cfg: Config, contract: Contract, wallet: str) -> int:
    return int(rpc_call(cfg, contract.functions.balanceOf(wallet).call))


def check_wallet(cfg: Config, wallet: str, contracts: list[str]) -> CheckResult:
    checked = 0
    failed = 0
    first_error = ""

    for contract_address in contracts:
        if stop_event.is_set():
            return CheckResult(wallet, None, checked, failed, "stopped")

        try:
            contract = get_contract(cfg, contract_address)
            if balance_of(cfg, contract, wallet) > 0:
                return CheckResult(wallet, True, checked + 1, failed, "")
            checked += 1
        except exceptions.ContractLogicError as exc:
            # Broken/non-standard contract for this call. Count it as failed, not as no-balance.
            failed += 1
            if not first_error:
                first_error = f"contract_logic_error:{exc}"
        except Exception as exc:  # noqa: BLE001
            failed += 1
            if not first_error:
                first_error = str(exc)[:500]

    # Avoid false negatives when most/all contract checks failed.
    if checked < cfg.min_confirmed_contracts and failed > 0:
        return CheckResult(wallet, None, checked, failed, first_error or "no confirmed contract checks")

    if checked == 0 and failed > 0:
        return CheckResult(wallet, None, checked, failed, first_error or "all contract checks failed")

    return CheckResult(wallet, False, checked, failed, first_error if failed else "")


# ==========================================================
# WRITER
# ==========================================================

def writer_loop(cfg: Config, q: "queue.Queue[CheckResult | object]") -> None:
    ensure_csv_header(cfg.output_file, ["address", "owns_nft"])
    ensure_csv_header(
        cfg.failed_file,
        ["address", "checked_contracts", "failed_contracts", "error"],
    )

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
                failed_buffer.append(
                    [item.address, item.checked_contracts, item.failed_contracts, item.error]
                )
            else:
                ok_buffer.append([item.address, str(item.owns_nft).lower()])

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
        owners = owned_count
        failed = failed_count

    if done % max(1, CFG.progress_every) == 0 or done == total:
        elapsed = max(0.001, time.time() - start)
        speed = done / elapsed
        eta = (total - done) / speed if speed else 0.0
        with rpc_penalty_lock:
            penalty = rpc_penalty
        logging.info(
            "Progress %d/%d (%.2f%%) | %.1f addr/s | owners=%d | failed=%d | penalty=%.3fs | ETA %.1fs",
            done,
            total,
            done / total * 100,
            speed,
            owners,
            failed,
            penalty,
            eta,
        )


def wait_iter(future_map: dict[Future, object]) -> Iterator[Future]:
    pending = set(future_map)
    while pending:
        done, pending = wait(pending, return_when=FIRST_COMPLETED)
        yield from done


# This global is only used to avoid passing cfg into a tiny progress helper.
# It is assigned in main().
CFG: Config


# ==========================================================
# MAIN
# ==========================================================

def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Fast ERC-721 ownership checker")
    parser.add_argument("--rpc-url", default=os.getenv("INFURA_URL", "").strip())
    parser.add_argument("--input", default=os.getenv("INPUT_FILE", "input_addresses.txt"))
    parser.add_argument("--contracts", default=os.getenv("CONTRACTS_FILE", "nft_contracts.txt"))
    parser.add_argument("--output", default=os.getenv("OUTPUT_FILE", "nft_owners.csv"))
    parser.add_argument("--failed", default=os.getenv("FAILED_FILE", "nft_owners_failed.csv"))
    parser.add_argument("--log", default=os.getenv("LOG_FILE", "nft_checker.log"))
    parser.add_argument("--threads", type=int, default=int(os.getenv("NUM_THREADS", "32")))
    parser.add_argument("--max-retries", type=int, default=int(os.getenv("MAX_RETRIES", "6")))
    parser.add_argument("--base-delay", type=float, default=float(os.getenv("BASE_DELAY", "0.35")))
    parser.add_argument("--rpc-timeout", type=float, default=float(os.getenv("RPC_TIMEOUT", "20")))
    parser.add_argument("--pool-connections", type=int, default=int(os.getenv("POOL_CONNECTIONS", "200")))
    parser.add_argument("--pool-maxsize", type=int, default=int(os.getenv("POOL_MAXSIZE", "200")))
    parser.add_argument("--writer-batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "500")))
    parser.add_argument("--progress-every", type=int, default=int(os.getenv("PROGRESS_EVERY", "200")))
    parser.add_argument("--max-inflight", type=int, default=int(os.getenv("MAX_INFLIGHT", "0")))
    parser.add_argument("--max-rpc-penalty", type=float, default=float(os.getenv("MAX_RPC_PENALTY", "1.5")))
    parser.add_argument("--min-confirmed-contracts", type=int, default=int(os.getenv("MIN_CONFIRMED_CONTRACTS", "1")))
    args = parser.parse_args()

    if not args.rpc_url:
        raise EnvironmentError("RPC URL is empty. Set INFURA_URL or pass --rpc-url")

    threads = max(1, args.threads)
    max_inflight = args.max_inflight if args.max_inflight > 0 else threads * 4

    return Config(
        rpc_url=args.rpc_url,
        input_file=Path(args.input),
        contracts_file=Path(args.contracts),
        output_file=Path(args.output),
        failed_file=Path(args.failed),
        log_file=Path(args.log),
        threads=threads,
        max_retries=max(1, args.max_retries),
        base_delay=max(0.0, args.base_delay),
        rpc_timeout=max(1.0, args.rpc_timeout),
        pool_connections=max(1, args.pool_connections),
        pool_maxsize=max(1, args.pool_maxsize),
        writer_batch_size=max(1, args.writer_batch_size),
        progress_every=max(1, args.progress_every),
        max_inflight=max(1, max_inflight),
        max_rpc_penalty=max(0.0, args.max_rpc_penalty),
        min_confirmed_contracts=max(0, args.min_confirmed_contracts),
    )


def install_signal_handlers() -> None:
    def sig_handler(signum, _frame):
        logging.warning("Signal %s received, stopping after in-flight tasks...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, sig_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, sig_handler)


def main() -> None:
    global CFG
    CFG = parse_args()
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
        logging.info("Resume mode: skipped %d completed address(es)", before - len(addresses))

    if not addresses:
        logging.info("Nothing to do.")
        return

    contracts = filter_contracts(CFG, contracts)
    if not contracts:
        raise RuntimeError("No verified contract addresses left. Check RPC, network, or contract list.")

    total = len(addresses)
    logging.info(
        "Start | addresses=%d | contracts=%d | threads=%d | max_inflight=%d",
        total,
        len(contracts),
        CFG.threads,
        CFG.max_inflight,
    )

    start = time.time()
    write_queue: "queue.Queue[CheckResult | object]" = queue.Queue(maxsize=CFG.writer_batch_size * 4)
    writer = threading.Thread(target=writer_loop, args=(CFG, write_queue), name="csv-writer", daemon=True)
    writer.start()

    address_iter = iter(addresses)
    pending: set[Future] = set()

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
                        # This should be rare because check_wallet returns failed rows.
                        result = CheckResult("", None, 0, 0, f"worker_crashed:{exc}")
                        logging.exception("Worker crashed: %s", exc)
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
        "DONE | done=%d/%d | checked=%d | owners=%d (%.2f%% of checked) | failed=%d | time=%.2fs | %.1f addr/s",
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
        logging.warning(
            "Some addresses were not written as false because RPC/contract checks were uncertain. See: %s",
            CFG.failed_file,
        )


if __name__ == "__main__":
    t0 = time.time()
    try:
        main()
    finally:
        print(f"Finished in {time.time() - t0:.2f}s")
