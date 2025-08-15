import os
import time
import json
import logging
from pathlib import Path
from typing import Set, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm
from web3 import Web3, exceptions
from web3.contract import Contract

# ==============================
# Configuration
# ==============================
INFURA_URL = os.getenv("INFURA_URL", "https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID")
ABI_FILE = Path("erc721_abi.json")
INPUT_FILE = Path("input_addresses.txt")
OUTPUT_FILE = Path("nft_owners.csv")
CONTRACTS_FILE = Path("nft_contracts.txt")
LOG_FILE = Path("nft_checker.log")
NUM_THREADS = 10
MAX_RETRIES = 3
RETRY_DELAY = 1.5  # seconds between retries to avoid rate limits

# ==============================
# Logging
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ==============================
# Web3 Initialization
# ==============================
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    raise ConnectionError("Unable to connect to Ethereum via Infura.")

# ==============================
# File Loading Helpers
# ==============================
def load_lines(path: Path) -> Set[str]:
    if not path.exists():
        logging.error(f"File not found: {path}")
        return set()
    try:
        with path.open("r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except Exception as e:
        logging.exception(f"Failed to read file {path}: {e}")
        return set()

def load_abi(path: Path) -> Optional[List[dict]]:
    if not path.exists():
        logging.error(f"ABI file not found: {path}")
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            abi = json.load(f)
        if isinstance(abi, list):
            return abi
        logging.error("Invalid ABI format: expected list of dicts.")
    except Exception as e:
        logging.exception(f"Error loading ABI: {e}")
    return None

# ==============================
# Ethereum Contract Helpers
# ==============================
def init_contract(address: str, abi: List[dict]) -> Optional[Contract]:
    try:
        return web3.eth.contract(address=web3.to_checksum_address(address), abi=abi)
    except Exception as e:
        logging.warning(f"Contract init failed for {address}: {e}")
        return None

def has_nft(address: str, contract: Contract) -> bool:
    """Check if the address owns any NFTs in the given contract, with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            balance = contract.functions.balanceOf(address).call()
            return balance > 0
        except exceptions.ContractLogicError as e:
            logging.debug(f"Contract logic error for {contract.address} on {address}: {e}")
            return False
        except Exception as e:
            logging.warning(f"[Attempt {attempt}] Error checking {address} on {contract.address}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

def check_nft_ownership(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    """Return (address, owns) and stop checking after first positive match."""
    for contract in contracts:
        if has_nft(address, contract):
            logging.info(f"{address}: owns NFT")
            return address, True
    logging.info(f"{address}: does not own NFT")
    return address, False

# ==============================
# Data Saving
# ==============================
def save_results(results: List[Tuple[str, bool]], path: Path) -> None:
    try:
        if path.exists():
            logging.warning(f"Overwriting existing file: {path}")
        df = pd.DataFrame(results, columns=["Address", "Owns NFT"])
        df.to_csv(path, index=False)
        logging.info(f"Saved {len(results)} results to {path}")
    except Exception as e:
        logging.exception(f"Could not save CSV to {path}: {e}")

# ==============================
# Data Validation
# ==============================
def validate_addresses(addresses: Set[str]) -> List[str]:
    checksummed = []
    for addr in addresses:
        if web3.is_address(addr):
            checksummed.append(web3.to_checksum_address(addr))
        else:
            logging.warning(f"Invalid Ethereum address skipped: {addr}")
    return checksummed

def load_contracts(contract_addresses: Set[str], abi: List[dict]) -> List[Contract]:
    contracts = [init_contract(addr, abi) for addr in contract_addresses]
    valid = [c for c in contracts if c]
    if not valid:
        logging.error("No valid contracts loaded.")
    return valid

# ==============================
# Main Logic
# ==============================
def main() -> None:
    addresses_raw = load_lines(INPUT_FILE)
    contracts_raw = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses_raw:
        logging.error("No addresses found.")
        return
    if not contracts_raw:
        logging.error("No NFT contract addresses found.")
        return
    if abi is None:
        logging.error("ABI loading failed.")
        return

    addresses = validate_addresses(addresses_raw)
    if not addresses:
        logging.error("No valid addresses to check.")
        return

    contracts = load_contracts(contracts_raw, abi)
    if not contracts:
        return

    results: List[Tuple[str, bool]] = []
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {
            executor.submit(check_nft_ownership, address, contracts): address
            for address in addresses
        }
        with tqdm(total=len(futures), desc="Checking NFT ownership", unit="addr") as pbar:
            for future in as_completed(futures):
                address = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    logging.exception(f"Error checking {address}: {e}")
                finally:
                    pbar.update(1)

    save_results(results, OUTPUT_FILE)

# ==============================
# Entry Point
# ==============================
if __name__ == "__main__":
    start_time = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
    elapsed = time.time() - start_time
    logging.info(f"Completed in {elapsed:.2f} seconds.")
    print(f"Done in {elapsed:.2f} seconds.")
