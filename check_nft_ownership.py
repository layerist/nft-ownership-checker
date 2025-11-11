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

# ============================================
# Configuration
# ============================================
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

# ============================================
# Logging Setup
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ============================================
# Web3 Initialization
# ============================================
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    raise ConnectionError(f"Unable to connect to Ethereum via Infura ({INFURA_URL})")

logging.info("Connected to Ethereum mainnet.")

# ============================================
# Utility Functions
# ============================================
def retry_on_failure(
    max_retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    retry_exceptions: Tuple[type, ...] = (Exception,),
    fatal_exceptions: Tuple[type, ...] = (exceptions.ContractLogicError,)
) -> Callable:
    """Decorator with exponential backoff, jitter, and configurable retry behavior."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except fatal_exceptions as e:
                    logging.debug(f"Fatal contract logic error in {func.__name__}: {e}")
                    return False
                except retry_exceptions as e:
                    if attempt == max_retries:
                        logging.error(f"{func.__name__} failed after {attempt} attempts: {e}")
                        return False
                    jitter = random.uniform(0, delay * 0.3)
                    sleep_time = delay + jitter
                    logging.warning(f"{func.__name__} retry {attempt}/{max_retries}: {e} — sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    delay *= 2
        return wrapper
    return decorator


def load_lines(path: Path) -> Set[str]:
    """Read unique non-empty lines from a file."""
    if not path.exists():
        logging.error(f"File not found: {path}")
        return set()
    try:
        with path.open("r", encoding="utf-8") as f:
            lines = {line.strip() for line in f if line.strip()}
        logging.info(f"Loaded {len(lines)} lines from {path}")
        return lines
    except Exception as e:
        logging.exception(f"Failed to read {path}: {e}")
        return set()


def load_abi(path: Path) -> Optional[List[dict]]:
    """Load ERC721 ABI JSON."""
    try:
        with path.open("r", encoding="utf-8") as f:
            abi = json.load(f)
        if isinstance(abi, list):
            return abi
        logging.error("ABI format invalid — expected a list of dicts.")
    except Exception as e:
        logging.exception(f"Failed to load ABI: {e}")
    return None


@lru_cache(maxsize=None)
def init_contract(address: str, abi: Tuple[dict, ...]) -> Optional[Contract]:
    """Initialize and cache contract instance."""
    try:
        return web3.eth.contract(address=web3.to_checksum_address(address), abi=abi)
    except Exception as e:
        logging.warning(f"Contract init failed for {address}: {e}")
        return None


@retry_on_failure()
def has_nft(address: str, contract: Contract) -> bool:
    """Check if an address holds NFTs from this contract."""
    balance = contract.functions.balanceOf(address).call()
    return balance > 0


def check_nft_ownership(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    """Return (address, True) if address owns NFT in any given contract."""
    for contract in contracts:
        if has_nft(address, contract):
            logging.info(f"{address} owns NFT in {contract.address}")
            return address, True
    return address, False


def validate_addresses(addresses: Set[str]) -> List[str]:
    """Filter and checksum valid Ethereum addresses."""
    valid = []
    for addr in addresses:
        if web3.is_address(addr):
            valid.append(web3.to_checksum_address(addr))
        else:
            logging.warning(f"Invalid address skipped: {addr}")
    return valid


def load_contracts(contract_addresses: Set[str], abi: List[dict]) -> List[Contract]:
    """Load valid ERC721 contract objects."""
    contracts = [init_contract(addr, tuple(abi)) for addr in contract_addresses]
    valid_contracts = [c for c in contracts if c]
    if not valid_contracts:
        logging.error("No valid NFT contracts loaded.")
    else:
        logging.info(f"Loaded {len(valid_contracts)} valid contracts.")
    return valid_contracts


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame atomically (safe for interruption)."""
    tmp_path = path.with_suffix(".tmp")
    df.to_csv(tmp_path, index=False)
    tmp_path.replace(path)


def save_results_incremental(
    new_results: List[Tuple[str, bool]],
    path: Path,
    lock: Lock
) -> None:
    """Append results incrementally (thread-safe)."""
    if not new_results:
        return
    df = pd.DataFrame(new_results, columns=["Address", "Owns NFT"])
    with lock:
        if path.exists():
            df.to_csv(path, index=False, mode="a", header=False)
        else:
            df.to_csv(path, index=False)
    logging.info(f"Appended {len(new_results)} new results.")


# ============================================
# Main Logic
# ============================================
def main() -> None:
    addresses_raw = load_lines(INPUT_FILE)
    contracts_raw = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses_raw or not contracts_raw or not abi:
        logging.error("Missing required input files or ABI.")
        return

    addresses = validate_addresses(addresses_raw)
    contracts = load_contracts(contracts_raw, abi)
    if not addresses or not contracts:
        return

    results: List[Tuple[str, bool]] = []
    results_lock = Lock()
    interrupted = False

    def handle_interrupt(sig, frame):
        nonlocal interrupted
        interrupted = True
        logging.warning("KeyboardInterrupt detected — stopping gracefully...")

    signal.signal(signal.SIGINT, handle_interrupt)

    logging.info(f"Starting NFT ownership checks for {len(addresses)} addresses using {NUM_THREADS} threads...")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures: Dict[Future, str] = {executor.submit(check_nft_ownership, addr, contracts): addr for addr in addresses}

        with tqdm(total=len(futures), desc="Checking NFT ownership", unit="addr", disable=not SHOW_PROGRESS) as pbar:
            batch: List[Tuple[str, bool]] = []
            for future in as_completed(futures):
                if interrupted:
                    break
                addr = futures[future]
                try:
                    result = future.result(timeout=30)
                    batch.append(result)
                except Exception as e:
                    logging.error(f"Unhandled error for {addr}: {e}")
                    batch.append((addr, False))
                finally:
                    pbar.update(1)
                    if len(batch) >= 50:
                        save_results_incremental(batch, OUTPUT_FILE, results_lock)
                        results.extend(batch)
                        batch.clear()

            # Save remaining
            if batch:
                save_results_incremental(batch, OUTPUT_FILE, results_lock)
                results.extend(batch)

    total = len(results)
    owns = sum(1 for _, has in results if has)
    logging.info(f"Completed: {total} checked, {owns} own NFTs ({owns/total:.2%}).")


# ============================================
# Entry Point
# ============================================
if __name__ == "__main__":
    start = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
    finally:
        elapsed = time.time() - start
        logging.info(f"Completed in {elapsed:.2f} seconds.")
        print(f"Done in {elapsed:.2f} seconds.")
