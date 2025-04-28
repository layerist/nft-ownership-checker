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
INFURA_URL = os.getenv('INFURA_URL', 'https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID')
ABI_FILE = Path('erc721_abi.json')
INPUT_FILE = Path('input_addresses.txt')
OUTPUT_FILE = Path('nft_owners.csv')
CONTRACTS_FILE = Path('nft_contracts.txt')
LOG_FILE = Path('nft_checker.log')
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
    raise ConnectionError("Unable to connect to Ethereum network. Check INFURA_URL.")

def load_file_lines(file_path: Path) -> Set[str]:
    """Load unique, non-empty lines from a file."""
    if not file_path.exists():
        logging.error(f"File not found: {file_path}")
        return set()

    with file_path.open('r', encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}

def load_abi(file_path: Path) -> Optional[List[dict]]:
    """Load ABI JSON from a file."""
    if not file_path.exists():
        logging.error(f"ABI file not found: {file_path}")
        return None

    try:
        with file_path.open('r', encoding='utf-8') as f:
            abi = json.load(f)
        if isinstance(abi, list):
            return abi
        logging.error("Invalid ABI format: expected a list.")
    except Exception as e:
        logging.exception(f"Failed to load ABI: {e}")
    return None

def get_contract(address: str, abi: List[dict]) -> Optional[Contract]:
    """Create a Web3 Contract instance."""
    try:
        return web3.eth.contract(address=web3.to_checksum_address(address), abi=abi)
    except Exception as e:
        logging.error(f"Failed to create contract for {address}: {e}")
    return None

def owns_nft(address: str, contract: Contract) -> bool:
    """Check if the address owns any NFTs from a contract."""
    try:
        balance = contract.functions.balanceOf(web3.to_checksum_address(address)).call()
        return balance > 0
    except exceptions.ContractLogicError as e:
        logging.warning(f"Contract logic error [{contract.address}]: {e}")
    except Exception as e:
        logging.error(f"Error checking NFT ownership [{contract.address}]: {e}")
    return False

def check_address(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    """Check if an address owns NFTs across any contract."""
    result = any(owns_nft(address, contract) for contract in contracts)
    logging.info(f"Checked {address}: owns NFT = {result}")
    return address, result

def save_to_csv(results: List[Tuple[str, bool]], file_path: Path):
    """Save results to a CSV file."""
    try:
        df = pd.DataFrame(results, columns=["Address", "Owns NFT"])
        df.to_csv(file_path, index=False)
        logging.info(f"Results saved to {file_path}")
    except Exception as e:
        logging.exception(f"Failed to save results: {e}")

def main():
    """Main execution flow."""
    addresses = load_file_lines(INPUT_FILE)
    contract_addresses = load_file_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses:
        logging.error("No addresses loaded.")
        return
    if not contract_addresses:
        logging.error("No contract addresses loaded.")
        return
    if not abi:
        logging.error("Invalid or missing ABI.")
        return

    valid_addresses = [addr for addr in addresses if web3.is_address(addr)]
    contracts = [get_contract(addr, abi) for addr in contract_addresses]
    contracts = [c for c in contracts if c is not None]

    if not contracts:
        logging.error("No valid contracts initialized.")
        return

    results = []
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {executor.submit(check_address, addr, contracts): addr for addr in valid_addresses}
        with tqdm(total=len(futures), desc="Checking addresses", unit="addr") as pbar:
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    address = futures[future]
                    logging.error(f"Error processing {address}: {e}")
                finally:
                    pbar.update(1)

    save_to_csv(results, OUTPUT_FILE)

if __name__ == "__main__":
    start_time = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
    elapsed = time.time() - start_time
    logging.info(f"Finished in {elapsed:.2f} seconds.")
    print(f"Done in {elapsed:.2f} seconds.")
