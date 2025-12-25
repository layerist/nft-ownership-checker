#!/usr/bin/env python3
"""
Robust multithreaded ERC-721 ownership checker.

Improvements:
- Stronger typing and structure
- Cleaner retry logic with explicit Web3 exceptions
- Early abort on shutdown with graceful executor drain
- Reduced per-call overhead
- Safer CSV writing
- Clear separation of concerns
"""

import os
import time
import json
import logging
import signal
import random
from pathlib import Path
from typing import Set, List, Tuple, Optional, Dict, Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache, wraps
from threading import Lock, Event

import pandas as pd
from tqdm import tqdm
from web3 import Web3, exceptions
from web3.contract import Contract

# ======================================================
# Configuration
# ======================================================
INFURA_URL = os.getenv("INFURA_URL", "https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID")

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
        logging.StreamHandler()
    ],
)

# ======================================================
# Web3 Initialization
# ======================================================
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    raise ConnectionError(f"Unable to connect to Infura: {INFURA_URL}")

logging.info("Connected to Ethereum mainnet")

# ======================================================
# Retry Decorator
# ======================================================
def retry_on_failure(
    *,
    max_retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    retry_exceptions: Tuple[type, ...] = (
        exceptions.TimeExhausted,
        exceptions.BadFunctionCallOutput,
        exceptions.ContractLogicError,
        IOError,
    ),
) -> Callable:
    """Retry decorator with exponential backoff + jitter."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    if attempt == max_retries:
                        logging.error(f"{func.__name__} failed after {attempt} attempts: {e}")
                        return False

                    sleep_time = delay * (1 + random.random() * 0.3)
                    logging.warning(
                        f"{func.__name__} retry {attempt}/{max_retries} "
                        f"({e}) — sleeping {sleep_time:.2f}s"
                    )
                    time.sleep(sleep_time)
                    delay *= 2
        return wrapper

    return decorator

# ======================================================
# File & Data Utilities
# ======================================================
def load_lines(path: Path) -> Set[str]:
    if not path.exists():
        logging.error(f"File not found: {path}")
        return set()

    with path.open("r", encoding="utf-8") as f:
        lines = {line.strip() for line in f if line.strip()}

    logging.info(f"Loaded {len(lines)} entries from {path}")
    return lines


def load_abi(path: Path) -> Optional[List[dict]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            abi = json.load(f)
        if not isinstance(abi, list):
            raise ValueError("ABI JSON must be a list")
        return abi
    except Exception as e:
        logging.exception(f"Failed to load ABI: {e}")
        return None


def validate_addresses(addresses: Iterable[str]) -> List[str]:
    valid: List[str] = []
    for addr in addresses:
        if web3.is_address(addr):
            valid.append(web3.to_checksum_address(addr))
        else:
            logging.warning(f"Invalid address skipped: {addr}")
    return valid

# ======================================================
# Contract Handling
# ======================================================
@lru_cache(maxsize=None)
def init_contract(address: str, abi: Tuple[dict, ...]) -> Optional[Contract]:
    try:
        return web3.eth.contract(
            address=web3.to_checksum_address(address),
            abi=abi,
        )
    except Exception as e:
        logging.warning(f"Contract init failed for {address}: {e}")
        return None


def load_contracts(addresses: Set[str], abi: List[dict]) -> List[Contract]:
    abi_tuple = tuple(abi)
    contracts = [
        c for addr in addresses
        if (c := init_contract(addr, abi_tuple)) is not None
    ]

    if not contracts:
        logging.error("No valid contracts loaded")
    else:
        logging.info(f"Loaded {len(contracts)} contract(s)")

    return contracts

# ======================================================
# NFT Checking Logic
# ======================================================
@retry_on_failure()
def has_erc721_balance(address: str, contract: Contract) -> bool:
    return contract.functions.balanceOf(address).call() > 0


def check_nft_ownership(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    for contract in contracts:
        if has_erc721_balance(address, contract):
            logging.info(f"{address} owns NFT in {contract.address}")
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

    df = pd.DataFrame(rows, columns=["Address", "Owns NFT"])
    with lock:
        df.to_csv(
            path,
            mode="a",
            header=not path.exists(),
            index=False,
        )
    logging.info(f"Saved {len(rows)} rows")

# ======================================================
# Main
# ======================================================
def main() -> None:
    raw_addresses = load_lines(INPUT_FILE)
    raw_contracts = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not raw_addresses or not raw_contracts or not abi:
        logging.error("Missing input data (addresses, contracts, or ABI)")
        return

    addresses = validate_addresses(raw_addresses)
    contracts = load_contracts(raw_contracts, abi)

    if not addresses or not contracts:
        return

    stop_event = Event()
    write_lock = Lock()

    def handle_sigint(_sig, _frame):
        logging.warning("SIGINT received — shutting down gracefully")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    logging.info(f"Processing {len(addresses)} addresses with {NUM_THREADS} threads")

    buffer: List[Tuple[str, bool]] = []
    results_total = 0
    owned_total = 0

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {
            executor.submit(check_nft_ownership, addr, contracts): addr
            for addr in addresses
        }

        with tqdm(total=len(futures), disable=not SHOW_PROGRESS, unit="addr") as pbar:
            for future in as_completed(futures):
                if stop_event.is_set():
                    break

                addr = futures[future]
                try:
                    address, owns = future.result()
                except Exception as e:
                    logging.error(f"Unhandled error for {addr}: {e}")
                    address, owns = addr, False

                buffer.append((address, owns))
                results_total += 1
                if owns:
                    owned_total += 1

                if len(buffer) >= BATCH_SIZE:
                    append_results(buffer, OUTPUT_FILE, write_lock)
                    buffer.clear()

                pbar.update(1)

    if buffer:
        append_results(buffer, OUTPUT_FILE, write_lock)

    pct = (owned_total / results_total) if results_total else 0
    logging.info(
        f"Done: {results_total} checked — "
        f"{owned_total} own NFTs ({pct:.2%})"
    )

# ======================================================
# Entry Point
# ======================================================
if __name__ == "__main__":
    start = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
    finally:
        elapsed = time.time() - start
        logging.info(f"Finished in {elapsed:.2f}s")
        print(f"Done in {elapsed:.2f}s")
