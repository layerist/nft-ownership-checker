#!/usr/bin/env python3
"""
Ultra High Performance ERC-721 Ownership Checker v4

Improvements:
- Thread-local Web3 instances (better parallelism)
- Minimal ABI (balanceOf only)
- Resume support (skip already checked)
- as_completed() processing
- Smarter retry/backoff
- Faster contract cache
- Adaptive rate limiter
- Better connection pooling
- Safer CSV writing
- Lower memory overhead
- Faster ETA/progress
"""

from __future__ import annotations

import os
import csv
import time
import signal
import random
import logging
import threading
from pathlib import Path
from typing import Iterable, List, Tuple, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from web3 import Web3, exceptions
from web3.contract import Contract

# ==========================================================
# CONFIG
# ==========================================================

INFURA_URL = os.getenv("INFURA_URL", "").strip()
if not INFURA_URL:
    raise EnvironmentError("INFURA_URL is not set")

INPUT_FILE = Path("input_addresses.txt")
CONTRACTS_FILE = Path("nft_contracts.txt")
OUTPUT_FILE = Path("nft_owners.csv")
LOG_FILE = Path("nft_checker.log")

NUM_THREADS = int(os.getenv("NUM_THREADS", "32"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))
BASE_DELAY = float(os.getenv("BASE_DELAY", "0.35"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
RPC_TIMEOUT = int(os.getenv("RPC_TIMEOUT", "20"))

POOL_CONNECTIONS = int(os.getenv("POOL_CONNECTIONS", "200"))
POOL_MAXSIZE = int(os.getenv("POOL_MAXSIZE", "200"))

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# ==========================================================
# MINIMAL ERC721 ABI
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
# GLOBALS
# ==========================================================

thread_local = threading.local()

contract_cache: Dict[str, Contract] = {}
contract_lock = Lock()

code_cache: Dict[str, bool] = {}
code_lock = Lock()

stop_event = Event()

csv_lock = Lock()

completed_count = 0
owned_count = 0
stats_lock = Lock()

# adaptive RPC delay
rpc_penalty = 0.0
rpc_penalty_lock = Lock()

# ==========================================================
# WEB3
# ==========================================================


def build_session() -> requests.Session:
    session = requests.Session()

    adapter = HTTPAdapter(
        pool_connections=POOL_CONNECTIONS,
        pool_maxsize=POOL_MAXSIZE,
        max_retries=0,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def get_w3() -> Web3:
    if not hasattr(thread_local, "w3"):
        session = build_session()

        thread_local.w3 = Web3(
            Web3.HTTPProvider(
                INFURA_URL,
                request_kwargs={
                    "timeout": RPC_TIMEOUT,
                    "session": session,
                },
            )
        )

    return thread_local.w3


# ==========================================================
# RETRY
# ==========================================================

def retry(fn):
    def wrapper(*args, **kwargs):

        delay = BASE_DELAY

        for attempt in range(MAX_RETRIES):

            if stop_event.is_set():
                return None

            try:
                with rpc_penalty_lock:
                    penalty = rpc_penalty

                if penalty > 0:
                    time.sleep(penalty)

                return fn(*args, **kwargs)

            except (
                RequestException,
                ConnectionError,
                exceptions.TimeExhausted,
                ValueError,
            ) as e:

                msg = str(e).lower()

                # detect Infura throttling
                throttled = (
                    "429" in msg
                    or "rate" in msg
                    or "too many requests" in msg
                )

                if throttled:
                    with rpc_penalty_lock:
                        globals()["rpc_penalty"] = min(
                            0.5,
                            rpc_penalty + 0.01,
                        )

                if attempt == MAX_RETRIES - 1:
                    raise

                sleep_time = (
                    delay
                    * (1.0 + random.random() * 0.5)
                )

                time.sleep(sleep_time)
                delay *= 1.7

    return wrapper


# ==========================================================
# IO
# ==========================================================

def load_lines(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8") as f:
        return [x.strip() for x in f if x.strip()]


def validate_addresses(
    addresses: Iterable[str],
) -> List[str]:
    result = []

    for a in addresses:
        if Web3.is_address(a):
            result.append(
                Web3.to_checksum_address(a)
            )

    return result


def load_completed() -> Set[str]:
    if not OUTPUT_FILE.exists():
        return set()

    completed = set()

    with OUTPUT_FILE.open(
        "r",
        encoding="utf-8",
        newline=""
    ) as f:

        reader = csv.reader(f)

        next(reader, None)

        for row in reader:
            if row:
                completed.add(row[0])

    return completed


# ==========================================================
# CONTRACT HELPERS
# ==========================================================

@retry
def is_contract(address: str) -> bool:

    cached = code_cache.get(address)
    if cached is not None:
        return cached

    w3 = get_w3()

    code = w3.eth.get_code(address)

    result = code not in (b"", b"\x00")

    with code_lock:
        code_cache[address] = result

    return result


def get_contract(address: str) -> Contract:

    contract = contract_cache.get(address)
    if contract:
        return contract

    with contract_lock:

        contract = contract_cache.get(address)
        if contract:
            return contract

        contract = get_w3().eth.contract(
            address=address,
            abi=ERC721_ABI,
        )

        contract_cache[address] = contract

        return contract


@retry
def owns_nft(
    contract: Contract,
    wallet: str,
) -> bool:

    return (
        contract.functions
        .balanceOf(wallet)
        .call() > 0
    )


# ==========================================================
# WORKER
# ==========================================================

def worker(
    addresses: List[str],
    contracts: List[str],
) -> List[Tuple[str, bool]]:

    results = []

    for address in addresses:

        if stop_event.is_set():
            break

        owns = False

        for contract_addr in contracts:

            try:
                contract = get_contract(contract_addr)

                if owns_nft(
                    contract,
                    address,
                ):
                    owns = True
                    break

            except exceptions.ContractLogicError:
                continue

            except Exception:
                continue

        results.append(
            (address, owns)
        )

    return results


# ==========================================================
# CSV
# ==========================================================

def write_rows(
    rows: List[Tuple[str, bool]]
):

    if not rows:
        return

    with csv_lock:

        file_exists = OUTPUT_FILE.exists()

        with OUTPUT_FILE.open(
            "a",
            newline="",
            encoding="utf-8",
        ) as f:

            writer = csv.writer(f)

            if not file_exists:
                writer.writerow(
                    [
                        "address",
                        "owns_nft",
                    ]
                )

            writer.writerows(rows)


# ==========================================================
# MAIN
# ==========================================================

def main():

    global completed_count
    global owned_count

    addresses = validate_addresses(
        load_lines(INPUT_FILE)
    )

    contracts = validate_addresses(
        load_lines(CONTRACTS_FILE)
    )

    completed = load_completed()

    if completed:
        logging.info(
            "Resume mode: skipping %d addresses",
            len(completed),
        )

        addresses = [
            x for x in addresses
            if x not in completed
        ]

    logging.info(
        "Checking contracts..."
    )

    contracts = [
        c for c in contracts
        if is_contract(c)
    ]

    logging.info(
        "Valid contracts: %d",
        len(contracts),
    )

    total = len(addresses)

    if total == 0:
        logging.info(
            "Nothing to do."
        )
        return

    start = time.time()

    def sig_handler(*_):
        logging.warning(
            "Stopping..."
        )
        stop_event.set()

    signal.signal(
        signal.SIGINT,
        sig_handler,
    )

    chunk_size = max(
        50,
        total // (NUM_THREADS * 4),
    )

    chunks = [
        addresses[
            i:i + chunk_size
        ]
        for i in range(
            0,
            total,
            chunk_size
        )
    ]

    logging.info(
        "Addresses: %d | Threads: %d | Chunks: %d",
        total,
        NUM_THREADS,
        len(chunks),
    )

    buffer = []

    with ThreadPoolExecutor(
        max_workers=NUM_THREADS
    ) as executor:

        futures = [
            executor.submit(
                worker,
                chunk,
                contracts,
            )
            for chunk in chunks
        ]

        for future in as_completed(
            futures
        ):

            if stop_event.is_set():
                break

            try:
                results = future.result()

            except Exception as e:
                logging.exception(
                    "Worker failed: %s",
                    e,
                )
                continue

            for address, owns in results:

                completed_count += 1
                owned_count += int(owns)

                buffer.append(
                    (address, owns)
                )

                if (
                    completed_count
                    % 200
                    == 0
                ):
                    elapsed = (
                        time.time()
                        - start
                    )

                    speed = (
                        completed_count
                        / elapsed
                    )

                    eta = (
                        (
                            total
                            - completed_count
                        )
                        / speed
                        if speed
                        else 0
                    )

                    logging.info(
                        "Progress %d/%d (%.2f%%) | %.1f addr/s | ETA %.1fs",
                        completed_count,
                        total,
                        completed_count
                        / total
                        * 100,
                        speed,
                        eta,
                    )

                if (
                    len(buffer)
                    >= BATCH_SIZE
                ):
                    write_rows(buffer)
                    buffer.clear()

    if buffer:
        write_rows(buffer)

    elapsed = (
        time.time() - start
    )

    logging.info(
        "DONE | checked=%d | owners=%d (%.2f%%) | time=%.2fs",
        completed_count,
        owned_count,
        (
            owned_count
            / completed_count
            * 100
        )
        if completed_count
        else 0,
        elapsed,
    )


if __name__ == "__main__":

    t0 = time.time()

    try:
        main()

    finally:
        print(
            f"Finished in "
            f"{time.time()-t0:.2f}s"
        )
