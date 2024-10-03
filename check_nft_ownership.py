import time
import json
import threading
from web3 import Web3
import pandas as pd
from queue import Queue
from tqdm import tqdm
import os
import logging
from concurrent.futures import ThreadPoolExecutor

# Configuration
INFURA_URL = os.getenv('INFURA_URL', 'https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID')
ABI_FILE = 'erc721_abi.json'
NUM_THREADS = 10
INPUT_FILE = 'input_addresses.txt'
OUTPUT_FILE = 'nft_owners.csv'
CONTRACTS_FILE = 'nft_contracts.txt'
LOG_FILE = 'nft_checker.log'

# Logging setup
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Web3 setup
web3 = Web3(Web3.HTTPProvider(INFURA_URL))


def load_file_lines(file_path):
    """Load non-empty lines from a file."""
    try:
        with open(file_path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {e}")
        return []


def load_abi(file_path):
    """Load ABI from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading ABI from {file_path}: {e}")
        return None


erc721_abi = load_abi(ABI_FILE)
if not erc721_abi:
    raise ValueError("ABI file could not be loaded. Please check the file and try again.")


def check_nft_ownership(address, nft_contract_addresses):
    """Check if the address owns NFTs from any of the specified contracts."""
    try:
        for contract_address in nft_contract_addresses:
            contract = web3.eth.contract(address=contract_address, abi=erc721_abi)
            balance = contract.functions.balanceOf(address).call()
            if balance > 0:
                return True
    except Exception as e:
        logging.error(f"Error checking ownership for {address}: {e}")
    return False


def process_address(address, nft_contract_addresses, results_queue):
    """Process an address, check NFT ownership, and store the result."""
    owns_nft = check_nft_ownership(address, nft_contract_addresses)
    results_queue.put((address, owns_nft))


def main(input_file, output_file, contracts_file):
    """Main function to coordinate NFT ownership checks."""
    addresses = load_file_lines(input_file)
    nft_contract_addresses = load_file_lines(contracts_file)

    if not addresses or not nft_contract_addresses:
        logging.error("No addresses or contracts loaded. Exiting.")
        return

    results_queue = Queue()

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        with tqdm(total=len(addresses), desc="Checking NFTs", unit="address") as pbar:
            futures = [executor.submit(process_address, address, nft_contract_addresses, results_queue) 
                       for address in addresses]

            for future in futures:
                future.result()  # Block until each thread completes
                pbar.update(1)

    # Collect results
    results = {}
    while not results_queue.empty():
        address, owns_nft = results_queue.get()
        results[address] = owns_nft

    # Save results to CSV
    pd.DataFrame(list(results.items()), columns=['Address', 'Owns NFT']).to_csv(output_file, index=False)
    logging.info(f"Results saved to {output_file}")


if __name__ == "__main__":
    start_time = time.time()
    try:
        main(INPUT_FILE, OUTPUT_FILE, CONTRACTS_FILE)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    elapsed_time = time.time() - start_time
    logging.info(f"Elapsed time: {elapsed_time:.2f} seconds")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
