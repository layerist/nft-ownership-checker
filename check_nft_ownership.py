#!/usr/bin/env python3
"""
Production-grade multithreaded ERC-721 ownership checker.

Improvements:
- Strict thread-local Web3 providers
- Safe contract factory per-thread (no cached cross-thread instances)
- Deterministic exponential backoff with jitter
- Cooperative + enforced shutdown (SIGINT cancels pending futures)
- Memory-safe task scheduling (no massive future dict)
- Atomic CSV writes without pandas overhead
- Explicit RPC/network retry coverage
"""

from __future__ import annotations

import os
import time
import json
import csv
import signal
import random
import logging
from pathlib import Path
from typing import Iterable, List, Tuple, Sequence, Callable
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_COMPLETED
from threading import Lock, Event, local

from web3 import Web3, exceptions
from web3.contract import Contract
from requests.exceptions import RequestException

# ======================================================
# Configuration
# ======================================================

INFURA_URL = os.getenv("INFURA_URL", "").strip()
if not INFURA_URL:
    raise EnvironmentError("INFURA_URL is not set")

ABI_FILE = Path("erc721_abi.json")
INPUT_FILE = Path("input_addresses.txt")
CONTRACTS_FILE = Path("nft_contracts.txt")
OUTPUT_FILE = Path("nft_owners.csv")
LOG_FILE = Path("nft_checker.log")

NUM_THREADS = int(os.getenv("NUM_THREADS", "12"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
BASE_DELAY = float(os.getenv("BASE_DELAY", "1.5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
RPC_TIMEOUT = int(os.getenv("RPC_TIMEOUT", "20"))

# ======================================================
# Logging
# ======================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# ======================================================
# Thread-local Web3
# ======================================================

_thread_ctx = local()


def get_web3() -> Web3:
    """Thread-local Web3 instance."""
    if not hasattr(_thread_ctx, "web3"):
        provider = Web3.HTTPProvider(
            INFURA_URL,
            request_kwargs={"timeout": RPC_TIMEOUT},
        )
        w3 = Web3(provider)

        if not w3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum RPC")

        _thread_ctx.web3 = w3

    return _thread_ctx.web3


def get_contract(address: str, abi: Sequence[dict]) -> Contract:
    """Create contract bound to the thread-local Web3."""
    w3 = get_web3()
    return w3.eth.contract(
        address=w3.to_checksum_address(address),
        abi=abi,
    )


# ======================================================
# Retry Decorator
# ======================================================

def retry_on_failure(
    *,
    max_retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    retry_exceptions: tuple[type, ...] = (
        exceptions.TimeExhausted,
        exceptions.BadFunctionCallOutput,
        exceptions.ContractLogicError,
        RequestException,
        IOError,
        ConnectionError,
    ),
) -> Callable:

    def decorator(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            delay = base_delay

            for attempt in range(1, max_retries + 1):
                try:
                    return fn(*args, **kwargs)

                except retry_exceptions as exc:
                    if attempt == max_retries:
                        logging.error(
                            "%s failed after %d attempts: %s",
                            fn.__name__,
                            attempt,
                            exc,
                        )
                        raise

                    jitter = random.uniform(0.0, 0.3)
                    sleep_time = delay * (1.0 + jitter)

                    logging.warning(
                        "%s retry %d/%d — sleeping %.2fs (%s)",
                        fn.__name__,
                        attempt,
                        max_retries,
                        sleep_time,
                        exc,
                    )

                    time.sleep(sleep_time)
                    delay *= 2

        return wrapper

    return decorator


# ======================================================
# Utilities
# ======================================================

def load_lines(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(path)

    with path.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    logging.info("Loaded %d entries from %s", len(lines), path)
    return lines


def load_abi(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8") as f:
        abi = json.load(f)

    if not isinstance(abi, list):
        raise ValueError("ABI JSON must be a list")

    return abi


def validate_addresses(addresses: Iterable[str]) -> List[str]:
    w3 = get_web3()
    valid: List[str] = []

    for addr in addresses:
        if w3.is_address(addr):
            valid.append(w3.to_checksum_address(addr))
        else:
            logging.warning("Invalid address skipped: %s", addr)

    return valid


# ======================================================
# NFT Logic
# ======================================================

@retry_on_failure()
def has_erc721_balance(address: str, contract: Contract) -> bool:
    return contract.functions.balanceOf(address).call() > 0


def check_nft_ownership(
    address: str,
    contract_addresses: Sequence[str],
    abi: Sequence[dict],
    stop_event: Event,
) -> Tuple[str, bool]:

    if stop_event.is_set():
        return address, False

    for contract_address in contract_addresses:
        if stop_event.is_set():
            break

        contract = get_contract(contract_address, abi)

        if has_erc721_balance(address, contract):
            logging.info(
                "%s owns NFT in %s",
                address,
                contract_address,
            )
            return address, True

    return address, False


# ======================================================
# Persistence (Atomic CSV)
# ======================================================

def append_results(
    rows: List[Tuple[str, bool]],
    path: Path,
    lock: Lock,
) -> None:

    if not rows:
        return

    with lock:
        file_exists = path.exists()

        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            if not file_exists:
                writer.writerow(["address", "owns_nft"])

            writer.writerows(rows)
            f.flush()
            os.fsync(f.fileno())

    logging.info("Flushed %d rows", len(rows))


# ======================================================
# Main
# ======================================================

def main() -> None:
    addresses_raw = load_lines(INPUT_FILE)
    contracts_raw = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    addresses = validate_addresses(addresses_raw)
    if not addresses:
        raise RuntimeError("No valid addresses")

    stop_event = Event()
    write_lock = Lock()

    def on_sigint(_sig, _frame):
        logging.warning("SIGINT received — shutting down")
        stop_event.set()

    signal.signal(signal.SIGINT, on_sigint)

    logging.info(
        "Starting: %d addresses × %d contracts using %d threads",
        len(addresses),
        len(contracts_raw),
        NUM_THREADS,
    )

    buffer: List[Tuple[str, bool]] = []
    checked = owned = 0

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures: set[Future] = set()
        address_iter = iter(addresses)

        # Pre-fill worker queue
        for _ in range(min(NUM_THREADS, len(addresses))):
            addr = next(address_iter, None)
            if addr:
                futures.add(
                    executor.submit(
                        check_nft_ownership,
                        addr,
                        contracts_raw,
                        abi,
                        stop_event,
                    )
                )

        while futures and not stop_event.is_set():
            done, futures = wait(futures, return_when=FIRST_COMPLETED)

            for future in done:
                try:
                    address, owns = future.result()
                except Exception as exc:
                    logging.error("Unhandled worker error: %s", exc)
                    continue

                buffer.append((address, owns))
                checked += 1
                owned += int(owns)

                if len(buffer) >= BATCH_SIZE:
                    append_results(buffer, OUTPUT_FILE, write_lock)
                    buffer.clear()

                # Submit next task
                next_addr = next(address_iter, None)
                if next_addr:
                    futures.add(
                        executor.submit(
                            check_nft_ownership,
                            next_addr,
                            contracts_raw,
                            abi,
                            stop_event,
                        )
                    )

        # Cancel remaining tasks on shutdown
        for future in futures:
            future.cancel()

    if buffer:
        append_results(buffer, OUTPUT_FILE, write_lock)

    ratio = (owned / checked * 100) if checked else 0.0

    logging.info(
        "Completed: %d checked — %d owners (%.2f%%)",
        checked,
        owned,
        ratio,
    )


# ======================================================
# Entry
# ======================================================

if __name__ == "__main__":
    start = time.time()

    try:
        main()
    except Exception:
        logging.exception("Fatal error")
        raise
    finally:
        elapsed = time.time() - start
        logging.info("Finished in %.2fs", elapsed)
        print(f"Done in {elapsed:.2f}s")
