#!/usr/bin/env python3
"""
Production-grade multithreaded ERC-721 ownership checker.

Highlights:
- Per-thread Web3 providers (thread-safe)
- Deterministic retries with exponential backoff + jitter
- Cooperative SIGINT shutdown (no orphan futures)
- RPC short-circuiting (stop on first positive balance)
- Safe, flushed CSV appends
"""

from __future__ import annotations

import os
import time
import json
import logging
import signal
import random
from pathlib import Path
from typing import (
    Iterable,
    List,
    Tuple,
    Dict,
    Callable,
    Sequence,
)
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from functools import lru_cache, wraps
from threading import Lock, Event, local

import pandas as pd
from tqdm import tqdm
from web3 import Web3, exceptions
from web3.contract import Contract

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
SHOW_PROGRESS = True

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
    if not hasattr(_thread_ctx, "web3"):
        w3 = Web3(Web3.HTTPProvider(INFURA_URL, request_kwargs={"timeout": 20}))
        if not w3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum RPC")
        _thread_ctx.web3 = w3
    return _thread_ctx.web3


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
        IOError,
    ),
) -> Callable:
    """Retry decorator with exponential backoff and jitter."""

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
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
                    sleep_for = delay * (1.0 + random.random() * 0.3)
                    logging.warning(
                        "%s retry %d/%d (%s) — sleeping %.2fs",
                        fn.__name__,
                        attempt,
                        max_retries,
                        exc,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    delay *= 2

        return wrapper

    return decorator

# ======================================================
# File Utilities
# ======================================================
def load_lines(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(path)

    with path.open("r", encoding="utf-8") as f:
        data = [line.strip() for line in f if line.strip()]

    logging.info("Loaded %d entries from %s", len(data), path)
    return data


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
# Contract Handling
# ======================================================
@lru_cache(maxsize=512)
def init_contract(address: str, abi_json: tuple[dict, ...]) -> Contract:
    w3 = get_web3()
    return w3.eth.contract(
        address=w3.to_checksum_address(address),
        abi=abi_json,
    )


def load_contracts(addresses: Iterable[str], abi: List[dict]) -> List[Contract]:
    abi_tuple = tuple(abi)
    contracts: List[Contract] = []

    for addr in addresses:
        try:
            contracts.append(init_contract(addr, abi_tuple))
        except Exception as exc:
            logging.warning("Skipping contract %s: %s", addr, exc)

    if not contracts:
        raise RuntimeError("No valid ERC-721 contracts loaded")

    logging.info("Loaded %d contract(s)", len(contracts))
    return contracts

# ======================================================
# NFT Logic
# ======================================================
@retry_on_failure()
def has_erc721_balance(address: str, contract: Contract) -> bool:
    return contract.functions.balanceOf(address).call() > 0


def check_nft_ownership(
    address: str,
    contracts: Sequence[Contract],
    stop_event: Event,
) -> Tuple[str, bool]:
    if stop_event.is_set():
        return address, False

    for contract in contracts:
        if stop_event.is_set():
            break
        if has_erc721_balance(address, contract):
            logging.info(
                "%s owns ERC-721 in contract %s",
                address,
                contract.address,
            )
            return address, True

    return address, False

# ======================================================
# Persistence
# ======================================================
def append_results(
    rows: List[Tuple[str, bool]],
    path: Path,
    lock: Lock,
) -> None:
    if not rows:
        return

    df = pd.DataFrame(rows, columns=["address", "owns_nft"])

    with lock:
        with path.open("a", encoding="utf-8", newline="") as f:
            df.to_csv(f, header=f.tell() == 0, index=False)
            f.flush()
            os.fsync(f.fileno())

    logging.info("Flushed %d rows to disk", len(rows))

# ======================================================
# Main
# ======================================================
def main() -> None:
    addresses_raw = load_lines(INPUT_FILE)
    contracts_raw = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    addresses = validate_addresses(addresses_raw)
    contracts = load_contracts(contracts_raw, abi)

    stop_event = Event()
    write_lock = Lock()

    def on_sigint(_sig, _frame):
        logging.warning("SIGINT received — stopping gracefully")
        stop_event.set()

    signal.signal(signal.SIGINT, on_sigint)

    logging.info(
        "Starting: %d addresses × %d contracts using %d threads",
        len(addresses),
        len(contracts),
        NUM_THREADS,
    )

    buffer: List[Tuple[str, bool]] = []
    checked = owned = 0

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures: Dict[Future, str] = {
            executor.submit(check_nft_ownership, addr, contracts, stop_event): addr
            for addr in addresses
            if not stop_event.is_set()
        }

        with tqdm(total=len(futures), disable=not SHOW_PROGRESS, unit="addr") as pbar:
            for future in as_completed(futures):
                if stop_event.is_set():
                    break

                addr = futures[future]
                try:
                    address, owns = future.result()
                except Exception as exc:
                    logging.error("Unhandled error for %s: %s", addr, exc)
                    address, owns = addr, False

                buffer.append((address, owns))
                checked += 1
                owned += int(owns)

                if len(buffer) >= BATCH_SIZE:
                    append_results(buffer, OUTPUT_FILE, write_lock)
                    buffer.clear()

                pbar.update(1)

    if buffer:
        append_results(buffer, OUTPUT_FILE, write_lock)

    ratio = owned / checked if checked else 0.0
    logging.info(
        "Completed: %d checked — %d owners (%.2f%%)",
        checked,
        owned,
        ratio * 100,
    )

# ======================================================
# Entry Point
# ======================================================
if __name__ == "__main__":
    start_ts = time.time()
    try:
        main()
    except Exception:
        logging.exception("Fatal error")
        raise
    finally:
        elapsed = time.time() - start_ts
        logging.info("Finished in %.2fs", elapsed)
        print(f"Done in {elapsed:.2f}s")
