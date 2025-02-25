import time
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import deque
from tqdm import tqdm
from web3 import Web3, exceptions
import pandas as pd

# Configuration
INFURA_URL = os.getenv('INFURA_URL', 'https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID')
ABI_FILE = Path('erc721_abi.json')
NUM_THREADS = 10
INPUT_FILE = Path('input_addresses.txt')
OUTPUT_FILE = Path('nft_owners.csv')
CONTRACTS_FILE = Path('nft_contracts.txt')
LOG_FILE = Path('nft_checker.log')

# Logging setup
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)

# Web3 setup
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.isConnected():
    raise RuntimeError("Failed to connect to Ethereum network. Check INFURA_URL.")

def load_file_lines(file_path):
    """Load unique, non-empty lines from a file."""
    try:
        if file_path.exists():
            with file_path.open('r') as f:
                return {line.strip() for line in f if line.strip()}
        logging.error(f"{file_path} not found or empty.")
    except Exception as e:
        logging.exception(f"Error reading {file_path}: {e}")
    return set()

def load_abi(file_path):
    """Load ABI from a JSON file."""
    try:
        if file_path.exists():
            with file_path.open('r') as f:
                return json.load(f)
        logging.error(f"ABI file {file_path} not found.")
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON format in {file_path}.")
    except Exception as e:
        logging.exception(f"Error loading ABI from {file_path}: {e}")
    return None

def check_nft_ownership(address, contract_address, abi):
    """Check if an address owns NFTs from the given contract."""
    try:
        contract = web3.eth.contract(address=contract_address, abi=abi)
        return contract.functions.balanceOf(address).call() > 0
    except exceptions.ContractLogicError as e:
        logging.warning(f"Contract logic error for {contract_address}: {e}")
    except Exception as e:
        logging.error(f"Error checking {address} on {contract_address}: {e}")
    return False

def process_address(address, contract_addresses, abi, results):
    """Check NFT ownership for an address across multiple contracts."""
    owns_nft = any(check_nft_ownership(address, contract, abi) for contract in contract_addresses)
    results.append((address, owns_nft))
    logging.info(f"Processed {address}: Owns NFT = {owns_nft}")

def save_results_to_csv(results, output_file):
    """Save results to a CSV file."""
    try:
        df = pd.DataFrame(results, columns=['Address', 'Owns NFT'])
        df.to_csv(output_file, index=False)
        logging.info(f"Results saved to {output_file}")
    except Exception as e:
        logging.exception(f"Failed to save results to {output_file}: {e}")

def main():
    """Main function to check NFT ownership."""
    addresses = load_file_lines(INPUT_FILE)
    contracts = load_file_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses or not contracts or not abi:
        logging.error("Missing data. Ensure input files exist and contain valid data.")
        return

    valid_addresses = {addr for addr in addresses if web3.isAddress(addr)}
    results = deque()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {
            executor.submit(process_address, addr, contracts, abi, results): addr
            for addr in valid_addresses
        }

        with tqdm(total=len(futures), desc="Checking NFTs", unit="address") as pbar:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing an address: {e}")
                pbar.update(1)

    save_results_to_csv(list(results), OUTPUT_FILE)

if __name__ == "__main__":
    start_time = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
    elapsed_time = time.time() - start_time
    logging.info(f"Elapsed time: {elapsed_time:.2f} seconds")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
