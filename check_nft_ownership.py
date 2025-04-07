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

# Logging setup
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

# Web3 setup
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.isConnected():
    raise RuntimeError("Failed to connect to Ethereum network. Check INFURA_URL.")

def load_file_lines(file_path: Path) -> Set[str]:
    """Load unique, non-empty lines from a file."""
    try:
        with file_path.open('r', encoding='utf-8') as f:
            return {line.strip() for line in f if line.strip()}
    except Exception as e:
        logging.exception(f"Failed to read {file_path}: {e}")
        return set()

def load_abi(file_path: Path) -> Optional[list]:
    """Load contract ABI from a JSON file."""
    try:
        with file_path.open('r', encoding='utf-8') as f:
            abi = json.load(f)
            if isinstance(abi, list):
                return abi
            logging.error("Invalid ABI format: expected a list.")
    except Exception as e:
        logging.exception(f"Error loading ABI from {file_path}: {e}")
    return None

def get_contract(contract_address: str, abi: list) -> Optional[Contract]:
    """Create a contract object from address and ABI."""
    try:
        return web3.eth.contract(address=web3.toChecksumAddress(contract_address), abi=abi)
    except Exception as e:
        logging.error(f"Failed to create contract for {contract_address}: {e}")
    return None

def check_nft_ownership(address: str, contract: Contract) -> bool:
    """Check if a wallet address owns NFTs from the given contract."""
    try:
        balance = contract.functions.balanceOf(web3.toChecksumAddress(address)).call()
        return balance > 0
    except exceptions.ContractLogicError as e:
        logging.warning(f"Contract logic error at {contract.address}: {e}")
    except Exception as e:
        logging.error(f"Error checking NFT ownership: {e}")
    return False

def process_address(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    """Check if the address owns any NFTs from a list of contracts."""
    owns_nft = any(check_nft_ownership(address, contract) for contract in contracts)
    logging.info(f"{address} ownership: {owns_nft}")
    return address, owns_nft

def save_results(results: List[Tuple[str, bool]], output_file: Path):
    """Save the result list to a CSV file."""
    try:
        df = pd.DataFrame(results, columns=["Address", "Owns NFT"])
        df.to_csv(output_file, index=False)
        logging.info(f"Saved results to {output_file}")
    except Exception as e:
        logging.exception(f"Failed to write results to {output_file}: {e}")

def main():
    """Main entry point to check NFT ownership for multiple addresses."""
    addresses = load_file_lines(INPUT_FILE)
    contract_addresses = load_file_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses or not contract_addresses or not abi:
        logging.error("Missing or invalid input files or ABI.")
        return

    valid_addresses = [addr for addr in addresses if web3.isAddress(addr)]
    contracts = [get_contract(c, abi) for c in contract_addresses]
    contracts = [c for c in contracts if c is not None]

    if not contracts:
        logging.error("No valid contracts loaded.")
        return

    results = []
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {executor.submit(process_address, addr, contracts): addr for addr in valid_addresses}
        with tqdm(total=len(futures), desc="Checking NFT ownership", unit="address") as pbar:
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    logging.error(f"Error processing {futures[future]}: {e}")
                pbar.update(1)

    save_results(results, OUTPUT_FILE)

if __name__ == "__main__":
    start = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
    duration = time.time() - start
    logging.info(f"Script completed in {duration:.2f} seconds.")
    print(f"Done in {duration:.2f} seconds.")
