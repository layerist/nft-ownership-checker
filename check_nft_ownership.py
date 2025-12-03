import os
import time
import json
import logging
import signal
import random
from pathlib import Path
from typing import Set, List, Tuple, Optional, Dict, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from functools import lru_cache, wraps
from threading import Lock

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
OUTPUT_FILE = Path("nft_owners.csv")
CONTRACTS_FILE = Path("nft_contracts.txt")
LOG_FILE = Path("nft_checker.log")

NUM_THREADS = int(os.getenv("NUM_THREADS", "12"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
BASE_DELAY = float(os.getenv("BASE_DELAY", "1.5"))
SHOW_PROGRESS = True

# ======================================================
# Logging
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ======================================================
# Web3
# ======================================================
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    raise ConnectionError(f"Unable to connect via Infura ({INFURA_URL})")

logging.info("Connected to Ethereum mainnet.")

# ======================================================
# Utilities
# ======================================================
def retry_on_failure(
    max_retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    retry_exceptions: Tuple[type, ...] = (Exception,),
    fatal_exceptions: Tuple[type, ...] = (exceptions.ContractLogicError,)
) -> Callable:
    """
    Retry decorator with exponential backoff + jitter.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except fatal_exceptions as e:
                    logging.debug(f"{func.__name__}: fatal error: {e}")
                    return False
                except retry_exceptions as e:
                    if attempt == max_retries:
                        logging.error(f"{func.__name__}: failed after {attempt} attempts: {e}")
                        return False

                    sleep_time = delay + random.uniform(0, delay * 0.3)
                    logging.warning(
                        f"{func.__name__}: retry {attempt}/{max_retries} after error: {e} "
                        f"(sleep {sleep_time:.2f}s)"
                    )
                    time.sleep(sleep_time)
                    delay *= 2
        return wrapper
    return decorator


def load_lines(path: Path) -> Set[str]:
    """Reads non-empty unique lines."""
    if not path.exists():
        logging.error(f"File not found: {path}")
        return set()
    try:
        with path.open("r", encoding="utf-8") as f:
            lines = {line.strip() for line in f if line.strip()}
        logging.info(f"Loaded {len(lines)} lines from {path}")
        return lines
    except Exception as e:
        logging.exception(f"Error reading {path}: {e}")
        return set()


def load_abi(path: Path) -> Optional[List[dict]]:
    """Load ABI from JSON file."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        logging.error("ABI must be a list.")
    except Exception as e:
        logging.exception(f"Failed to load ABI: {e}")
    return None


def validate_addresses(addresses: Set[str]) -> List[str]:
    """Validate/checksum addresses."""
    valid = []
    for addr in addresses:
        if web3.is_address(addr):
            valid.append(web3.to_checksum_address(addr))
        else:
            logging.warning(f"Invalid address: {addr}")
    return valid


@lru_cache(maxsize=None)
def init_contract(address: str, abi: Tuple[dict, ...]) -> Optional[Contract]:
    """Initialize contract (cached)."""
    try:
        checksum = web3.to_checksum_address(address)
        return web3.eth.contract(address=checksum, abi=abi)
    except Exception as e:
        logging.warning(f"Failed to init contract {address}: {e}")
        return None


def load_contracts(addresses: Set[str], abi: List[dict]) -> List[Contract]:
    """Load ERC721 contract objects."""
    abi_tuple = tuple(abi)
    contracts = [init_contract(addr, abi_tuple) for addr in addresses]
    contracts = [c for c in contracts if c]

    if not contracts:
        logging.error("No valid contracts loaded.")
    else:
        logging.info(f"Loaded {len(contracts)} contract(s).")

    return contracts


@retry_on_failure()
def check_balance(address: str, contract: Contract) -> bool:
    """Return True if address has ERC721 balance > 0."""
    return contract.functions.balanceOf(address).call() > 0


def check_nft_ownership(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    """Return (address, bool)."""
    for c in contracts:
        if check_balance(address, c):
            logging.info(f"{address} holds NFTs in {c.address}")
            return address, True
    return address, False


def save_results(df: pd.DataFrame, path: Path, lock: Lock) -> None:
    """Append results to CSV thread-safely."""
    with lock:
        file_exists = path.exists()
        df.to_csv(path, mode="a", header=not file_exists, index=False)
    logging.info(f"Saved {len(df)} results.")


# ======================================================
# Main
# ======================================================
def main() -> None:
    addresses_raw = load_lines(INPUT_FILE)
    contracts_raw = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses_raw or not contracts_raw or not abi:
        logging.error("Missing addresses, contracts or ABI.")
        return

    addresses = validate_addresses(addresses_raw)
    contracts = load_contracts(contracts_raw, abi)
    if not addresses or not contracts:
        return

    lock = Lock()
    interrupted = False
    results: List[Tuple[str, bool]] = []

    def handle_interrupt(_sig, _frame):
        nonlocal interrupted
        interrupted = True
        logging.warning("Interrupted — stopping...")

    signal.signal(signal.SIGINT, handle_interrupt)

    logging.info(f"Checking {len(addresses)} addresses using {NUM_THREADS} threads...")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_map = {
            executor.submit(check_nft_ownership, addr, contracts): addr for addr in addresses
        }

        batch = []
        with tqdm(total=len(future_map), disable=not SHOW_PROGRESS, unit="addr") as pbar:
            for future in as_completed(future_map):
                if interrupted:
                    break

                addr = future_map[future]
                try:
                    res = future.result(timeout=30)
                except Exception as e:
                    logging.error(f"Error for {addr}: {e}")
                    res = (addr, False)

                batch.append(res)
                pbar.update(1)

                if len(batch) >= 50:
                    save_results(pd.DataFrame(batch, columns=["Address", "Owns NFT"]), OUTPUT_FILE, lock)
                    results.extend(batch)
                    batch.clear()

        # final flush
        if batch:
            save_results(pd.DataFrame(batch, columns=["Address", "Owns NFT"]), OUTPUT_FILE, lock)
            results.extend(batch)

    total = len(results)
    owned = sum(1 for _, v in results if v)
    pct = owned / total if total > 0 else 0
    logging.info(f"Done: {total} checked — {owned} own NFTs ({pct:.2%}).")


if __name__ == "__main__":
    start = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")

    elapsed = time.time() - start
    logging.info(f"Finished in {elapsed:.2f}s")
    print(f"Done in {elapsed:.2f}s")
