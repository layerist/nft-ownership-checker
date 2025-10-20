import os
import time
import json
import logging
import signal
import random
from pathlib import Path
from typing import Set, List, Tuple, Optional, Dict, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
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

NUM_THREADS = 12
MAX_RETRIES = 4
BASE_DELAY = 1.5  # seconds
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
    raise ConnectionError("Unable to connect to Ethereum via Infura.")

# ============================================
# Utility Functions
# ============================================
def retry_on_failure(max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY) -> Callable:
    """Decorator with exponential backoff and jitter."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions.ContractLogicError:
                    logging.debug(f"Contract logic error in {func.__name__}, args={args}")
                    return False
                except Exception as e:
                    logging.warning(f"{func.__name__} failed (attempt {attempt}/{max_retries}): {e}")
                    if attempt < max_retries:
                        time.sleep(delay + random.uniform(0, delay * 0.3))
                        delay *= 2
            return False
        return wrapper
    return decorator


def load_lines(path: Path) -> Set[str]:
    """Read non-empty unique lines from a file."""
    if not path.exists():
        logging.error(f"File not found: {path}")
        return set()
    try:
        with path.open("r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except Exception as e:
        logging.exception(f"Failed to read {path}: {e}")
        return set()


def load_abi(path: Path) -> Optional[List[dict]]:
    """Load ERC721 ABI JSON."""
    if not path.exists():
        logging.error(f"Missing ABI file: {path}")
        return None
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
    """Cached contract initialization."""
    try:
        return web3.eth.contract(address=web3.to_checksum_address(address), abi=abi)
    except Exception as e:
        logging.warning(f"Contract init failed for {address}: {e}")
        return None


@retry_on_failure()
def has_nft(address: str, contract: Contract) -> bool:
    """Check if the address holds NFTs from this contract."""
    balance = contract.functions.balanceOf(address).call()
    return balance > 0


def check_nft_ownership(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    """Return (address, True) if user owns NFT in any contract."""
    try:
        for contract in contracts:
            if has_nft(address, contract):
                logging.info(f"{address} owns NFT in {contract.address}")
                return address, True
        return address, False
    except Exception as e:
        logging.warning(f"Error checking ownership for {address}: {e}")
        return address, False


def validate_addresses(addresses: Set[str]) -> List[str]:
    """Validate and checksum all Ethereum addresses."""
    valid = []
    for addr in addresses:
        if web3.is_address(addr):
            valid.append(web3.to_checksum_address(addr))
        else:
            logging.warning(f"Invalid address skipped: {addr}")
    return valid


def load_contracts(contract_addresses: Set[str], abi: List[dict]) -> List[Contract]:
    """Load all valid ERC721 contracts."""
    contracts = [init_contract(addr, tuple(abi)) for addr in contract_addresses]
    valid_contracts = [c for c in contracts if c]
    if not valid_contracts:
        logging.error("No valid NFT contracts loaded.")
    return valid_contracts


def save_results(results: List[Tuple[str, bool]], path: Path, lock: Optional[Lock] = None) -> None:
    """Save current progress to CSV (thread-safe)."""
    try:
        df = pd.DataFrame(sorted(results), columns=["Address", "Owns NFT"])
        if lock:
            with lock:
                df.to_csv(path, index=False)
        else:
            df.to_csv(path, index=False)
        logging.info(f"Saved {len(results)} results to {path}")
    except Exception as e:
        logging.exception(f"Failed to save CSV: {e}")


# ============================================
# Main Logic
# ============================================
def main() -> None:
    addresses_raw = load_lines(INPUT_FILE)
    contracts_raw = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses_raw:
        logging.error("No addresses provided.")
        return
    if not contracts_raw:
        logging.error("No contract addresses provided.")
        return
    if not abi:
        logging.error("ABI not loaded.")
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
        logging.warning("Interrupted by user — saving progress...")
        interrupted = True

    signal.signal(signal.SIGINT, handle_interrupt)

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {executor.submit(check_nft_ownership, addr, contracts): addr for addr in addresses}
        with tqdm(total=len(futures), desc="Checking NFT ownership", unit="addr", disable=not SHOW_PROGRESS) as pbar:
            for future in as_completed(futures):
                if interrupted:
                    break
                address = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logging.exception(f"Unhandled error for {address}: {e}")
                finally:
                    pbar.update(1)
                    if len(results) % 50 == 0:  # autosave every 50
                        save_results(results, OUTPUT_FILE, results_lock)

    save_results(results, OUTPUT_FILE, results_lock)
    logging.info(f"Checked {len(results)} addresses total.")


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
