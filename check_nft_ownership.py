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

# Web3 setup
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.isConnected():
    raise RuntimeError("Failed to connect to Ethereum network. Check INFURA_URL.")

def load_file_lines(file_path):
    """Load non-empty, unique lines from a file."""
    if not file_path.exists():
        logging.error(f"{file_path} not found.")
        return set()
    try:
        with file_path.open('r') as f:
            lines = {line.strip() for line in f if line.strip()}
        if not lines:
            logging.error(f"{file_path} is empty.")
        return lines
    except Exception as e:
        logging.exception(f"Error loading data from {file_path}: {e}")
        return set()

def validate_eth_address(address):
    """Validate an Ethereum address."""
    return web3.isAddress(address)

def load_abi(file_path):
    """Load ABI from a JSON file."""
    if not file_path.exists():
        logging.error(f"ABI file {file_path} not found.")
        return None
    try:
        with file_path.open('r') as f:
            abi = json.load(f)
        if not abi:
            logging.error(f"ABI file {file_path} is empty or invalid.")
        return abi
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from {file_path}. Ensure proper formatting.")
    except Exception as e:
        logging.exception(f"Error loading ABI from {file_path}: {e}")
    return None

def check_nft_ownership(address, contract_address, abi):
    """Check if the address owns NFTs from the specified contract."""
    try:
        contract = web3.eth.contract(address=contract_address, abi=abi)
        return contract.functions.balanceOf(address).call() > 0
    except exceptions.ContractLogicError as e:
        logging.error(f"Contract logic error for {contract_address}: {e}")
    except Exception as e:
        logging.error(f"Error checking ownership for {address} on {contract_address}: {e}")
    return False

def process_address(address, nft_contract_addresses, abi, results_deque):
    """Process an address, check NFT ownership, and store the result."""
    try:
        owns_nft = any(
            check_nft_ownership(address, contract_address, abi)
            for contract_address in nft_contract_addresses
            if validate_eth_address(contract_address)
        )
        results_deque.append((address, owns_nft))
        logging.info(f"Processed {address}: Owns NFT = {owns_nft}")
    except Exception as e:
        logging.exception(f"Error processing {address}: {e}")

def save_results_to_csv(results, output_file):
    """Save the results to a CSV file."""
    try:
        df = pd.DataFrame(results, columns=['Address', 'Owns NFT'])
        df.to_csv(output_file, index=False)
        logging.info(f"Results saved to {output_file}")
    except Exception as e:
        logging.exception(f"Failed to save results to {output_file}: {e}")

def main():
    """Main function to coordinate NFT ownership checks."""
    addresses = load_file_lines(INPUT_FILE)
    nft_contract_addresses = load_file_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses:
        logging.error("No valid addresses found. Exiting.")
        return
    if not nft_contract_addresses:
        logging.error("No NFT contracts found. Exiting.")
        return
    if not abi:
        logging.error("No valid ABI loaded. Exiting.")
        return

    results_deque = deque()
    valid_addresses = {addr for addr in addresses if validate_eth_address(addr)}

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {
            executor.submit(process_address, address, nft_contract_addresses, abi, results_deque): address
            for address in valid_addresses
        }

        with tqdm(total=len(futures), desc="Checking NFTs", unit="address") as pbar:
            for future in as_completed(futures):  # No timeout here
                try:
                    future.result()  # Ensures exceptions are raised
                except Exception as e:
                    logging.error(f"Thread error: {e}")
                pbar.update(1)

    save_results_to_csv(list(results_deque), OUTPUT_FILE)

if __name__ == "__main__":
    start_time = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
    elapsed_time = time.time() - start_time
    logging.info(f"Elapsed time: {elapsed_time:.2f} seconds")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
