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

# Configuration
INFURA_URL = os.getenv("INFURA_URL", "https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID")
ABI_FILE = Path("erc721_abi.json")
INPUT_FILE = Path("input_addresses.txt")
OUTPUT_FILE = Path("nft_owners.csv")
CONTRACTS_FILE = Path("nft_contracts.txt")
LOG_FILE = Path("nft_checker.log")
NUM_THREADS = 10

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Initialize Web3
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    raise ConnectionError("Failed to connect to Ethereum via Infura.")

def load_lines(file_path: Path) -> Set[str]:
    """Load unique, non-empty lines from a file."""
    if not file_path.exists():
        logging.error(f"Missing file: {file_path}")
        return set()
    with file_path.open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def load_abi(file_path: Path) -> Optional[List[dict]]:
    """Load a contract ABI from a JSON file."""
    if not file_path.exists():
        logging.error(f"ABI file not found: {file_path}")
        return None
    try:
        with file_path.open("r", encoding="utf-8") as f:
            abi = json.load(f)
        if not isinstance(abi, list):
            logging.error("Invalid ABI format. Expected a list.")
            return None
        return abi
    except Exception as e:
        logging.exception(f"Error loading ABI: {e}")
        return None

def init_contract(address: str, abi: List[dict]) -> Optional[Contract]:
    """Create a contract instance from an address and ABI."""
    try:
        return web3.eth.contract(address=web3.to_checksum_address(address), abi=abi)
    except Exception as e:
        logging.error(f"Failed to initialize contract {address}: {e}")
        return None

def has_nft(address: str, contract: Contract) -> bool:
    """Return True if the address owns at least one token in the contract."""
    try:
        balance = contract.functions.balanceOf(web3.to_checksum_address(address)).call()
        return balance > 0
    except exceptions.ContractLogicError as e:
        logging.warning(f"Contract logic error for {contract.address}: {e}")
    except Exception as e:
        logging.error(f"Error checking balance for {address} in {contract.address}: {e}")
    return False

def check_nft_ownership(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    """Check whether the address owns NFTs in any of the provided contracts."""
    owns = any(has_nft(address, contract) for contract in contracts)
    logging.info(f"{address} NFT ownership: {owns}")
    return address, owns

def save_results(results: List[Tuple[str, bool]], file_path: Path):
    """Save results to CSV."""
    try:
        df = pd.DataFrame(results, columns=["Address", "Owns NFT"])
        df.to_csv(file_path, index=False)
        logging.info(f"Results written to {file_path}")
    except Exception as e:
        logging.exception(f"Failed to save CSV: {e}")

def main():
    addresses = load_lines(INPUT_FILE)
    contract_addresses = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses:
        logging.error("No input addresses found.")
        return
    if not contract_addresses:
        logging.error("No contract addresses found.")
        return
    if abi is None:
        logging.error("ABI loading failed.")
        return

    valid_addresses = [addr for addr in addresses if web3.is_address(addr)]
    contracts = [init_contract(addr, abi) for addr in contract_addresses]
    contracts = [c for c in contracts if c]

    if not contracts:
        logging.error("No valid contracts initialized.")
        return

    results: List[Tuple[str, bool]] = []
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {executor.submit(check_nft_ownership, addr, contracts): addr for addr in valid_addresses}
        with tqdm(total=len(futures), desc="Checking addresses", unit="address") as pbar:
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logging.error(f"Error checking address {futures[future]}: {e}")
                finally:
                    pbar.update(1)

    save_results(results, OUTPUT_FILE)

if __name__ == "__main__":
    start = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
    duration = time.time() - start
    logging.info(f"Completed in {duration:.2f} seconds.")
    print(f"Done in {duration:.2f} seconds.")
