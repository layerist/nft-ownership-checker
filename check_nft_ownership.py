#!/usr/bin/env python3
"""
Ultra high-performance ERC-721 ownership checker (v3)

Key upgrades:
- Global contract pre-filter (massive RPC reduction)
- Shared contract/code cache across threads
- Adaptive global rate limiter (Infura-safe)
- Optimized retry with jitter
- Chunked worker processing (less overhead than 1 addr = 1 future)
- Streaming CSV write (crash-safe)
- Progress + ETA
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
from typing import Iterable, List, Tuple, Sequence
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Event

import requests
from web3 import Web3, exceptions
from web3.contract import Contract
from requests.exceptions import RequestException

# ======================================================
# CONFIG
# ======================================================

INFURA_URL = os.getenv("INFURA_URL", "").strip()
if not INFURA_URL:
    raise EnvironmentError("INFURA_URL is not set")

ABI_FILE = Path("erc721_abi.json")
INPUT_FILE = Path("input_addresses.txt")
CONTRACTS_FILE = Path("nft_contracts.txt")
OUTPUT_FILE = Path("nft_owners.csv")
LOG_FILE = Path("nft_checker.log")

NUM_THREADS = int(os.getenv("NUM_THREADS", "16"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BASE_DELAY = float(os.getenv("BASE_DELAY", "0.6"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
RPC_TIMEOUT = int(os.getenv("RPC_TIMEOUT", "15"))

# ======================================================
# LOGGING
# ======================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# ======================================================
# GLOBALS
# ======================================================

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=200, pool_maxsize=200)
session.mount("http://", adapter)
session.mount("https://", adapter)

w3 = Web3(Web3.HTTPProvider(INFURA_URL, request_kwargs={"timeout": RPC_TIMEOUT, "session": session}))

if not w3.is_connected():
    raise ConnectionError("RPC connection failed")

contract_cache: dict[str, Contract] = {}
code_cache: dict[str, bool] = {}

# Rate limiter
last_call = 0.0
rate_lock = Lock()
min_interval = 0.01  # ~100 req/sec max (adaptive)


def rate_limit():
    global last_call
    with rate_lock:
        now = time.time()
        delta = now - last_call
        if delta < min_interval:
            time.sleep(min_interval - delta)
        last_call = time.time()


# ======================================================
# RETRY
# ======================================================

def retry(fn):
    def wrapper(*args, **kwargs):
        delay = BASE_DELAY

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                rate_limit()
                return fn(*args, **kwargs)

            except (
                exceptions.TimeExhausted,
                RequestException,
                ConnectionError,
                ValueError,
            ) as e:

                if attempt == MAX_RETRIES:
                    raise

                sleep = delay * (1 + random.random() * 0.5)
                time.sleep(sleep)
                delay *= 1.8

    return wrapper


# ======================================================
# IO
# ======================================================

def load_lines(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8") as f:
        return [x.strip() for x in f if x.strip()]


def load_abi(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_addresses(addresses: Iterable[str]) -> List[str]:
    return [
        Web3.to_checksum_address(a)
        for a in addresses
        if Web3.is_address(a)
    ]


# ======================================================
# CONTRACT HELPERS
# ======================================================

@retry
def is_contract(address: str) -> bool:
    if address in code_cache:
        return code_cache[address]

    code = w3.eth.get_code(address)
    result = code not in (b"", b"\x00")
    code_cache[address] = result
    return result


def get_contract(address: str, abi: Sequence[dict]) -> Contract:
    if address not in contract_cache:
        contract_cache[address] = w3.eth.contract(address=address, abi=abi)
    return contract_cache[address]


@retry
def balance_of(contract: Contract, address: str) -> int:
    return contract.functions.balanceOf(address).call()


# ======================================================
# CORE
# ======================================================

def worker_chunk(
    addresses: List[str],
    contracts: List[str],
    abi: Sequence[dict],
    stop: Event,
) -> List[Tuple[str, bool]]:

    results = []
    get_c = get_contract

    for address in addresses:
        if stop.is_set():
            break

        owns = False

        for c_addr in contracts:
            if stop.is_set():
                break

            try:
                contract = get_c(c_addr, abi)

                if balance_of(contract, address) > 0:
                    owns = True
                    break

            except exceptions.ContractLogicError:
                continue
            except Exception:
                continue

        results.append((address, owns))

    return results


# ======================================================
# CSV
# ======================================================

def write_rows(rows: List[Tuple[str, bool]], lock: Lock):
    if not rows:
        return

    with lock:
        exists = OUTPUT_FILE.exists()

        with OUTPUT_FILE.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)

            if not exists:
                w.writerow(["address", "owns_nft"])

            w.writerows(rows)


# ======================================================
# MAIN
# ======================================================

def main():
    addresses = validate_addresses(load_lines(INPUT_FILE))
    contracts = validate_addresses(load_lines(CONTRACTS_FILE))
    abi = load_abi(ABI_FILE)

    stop = Event()
    lock = Lock()

    # 🔥 Pre-filter contracts ONCE
    logging.info("Filtering contracts...")
    contracts = [c for c in contracts if is_contract(c)]
    logging.info("Valid contracts: %d", len(contracts))

    total = len(addresses)
    checked = owned = 0
    start_time = time.time()

    def sigint(_, __):
        logging.warning("Stopping...")
        stop.set()

    signal.signal(signal.SIGINT, sigint)

    # Chunk addresses
    chunk_size = max(10, total // (NUM_THREADS * 4))
    chunks = [addresses[i:i + chunk_size] for i in range(0, total, chunk_size)]

    buffer: List[Tuple[str, bool]] = []

    with ThreadPoolExecutor(NUM_THREADS) as ex:
        futures = [ex.submit(worker_chunk, chunk, contracts, abi, stop) for chunk in chunks]

        for f in futures:
            if stop.is_set():
                break

            try:
                results = f.result()
            except Exception as e:
                logging.error("Worker error: %s", e)
                continue

            for addr, owns_flag in results:
                checked += 1
                owned += owns_flag
                buffer.append((addr, owns_flag))

                if checked % 200 == 0:
                    elapsed = time.time() - start_time
                    speed = checked / elapsed
                    eta = (total - checked) / speed if speed else 0

                    logging.info(
                        "Progress: %d/%d | %.2f%% | %.1f addr/s | ETA %.1fs",
                        checked,
                        total,
                        checked / total * 100,
                        speed,
                        eta,
                    )

                if len(buffer) >= BATCH_SIZE:
                    write_rows(buffer, lock)
                    buffer.clear()

    if buffer:
        write_rows(buffer, lock)

    logging.info(
        "DONE: %d checked | %d owners (%.2f%%)",
        checked,
        owned,
        owned / checked * 100 if checked else 0,
    )


# ======================================================
# ENTRY
# ======================================================

if __name__ == "__main__":
    t0 = time.time()
    try:
        main()
    finally:
        print(f"Finished in {time.time() - t0:.2f}s")
