import time
import json
import threading
from web3 import Web3
import pandas as pd
from queue import Queue
from tqdm import tqdm
import os

# Configuration
INFURA_URL = os.getenv('INFURA_URL', 'https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID')
ABI_FILE = 'erc721_abi.json'
NUM_THREADS = 10
INPUT_FILE = 'input_addresses.txt'
OUTPUT_FILE = 'nft_owners.csv'

# Web3 setup
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

def load_addresses(file_path):
    """Load Ethereum addresses from a text file."""
    try:
        with open(file_path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error loading addresses from {file_path}: {e}")
        return []

def load_abi(file_path):
    """Load the ERC-721 ABI from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading ABI from {file_path}: {e}")
        return []

erc721_abi = load_abi(ABI_FILE)

def check_nft_ownership(address, nft_contract_addresses, results_queue):
    """Check if the given address owns any NFTs from the specified contracts."""
    owns_nft = False
    try:
        for contract_address in nft_contract_addresses:
            contract = web3.eth.contract(address=contract_address, abi=erc721_abi)
            balance = contract.functions.balanceOf(address).call()
            if balance > 0:
                owns_nft = True
                break
    except Exception as e:
        print(f"Error checking address {address}: {e}")
    finally:
        results_queue.put((address, owns_nft))

def worker(address_queue, nft_contract_addresses, results_queue):
    """Worker thread function to process addresses from the queue."""
    while not address_queue.empty():
        address = address_queue.get()
        try:
            check_nft_ownership(address, nft_contract_addresses, results_queue)
        finally:
            address_queue.task_done()

def main(input_file, output_file):
    """Main function to coordinate the NFT ownership check."""
    addresses = load_addresses(input_file)
    nft_contract_addresses = [
        "0x06012c8cf97BEaD5deAe237070F9587f8E7A266d",  # Replace with actual contract addresses
        # Add more NFT contract addresses here
    ]

    address_queue = Queue()
    results_queue = Queue()

    for address in addresses:
        address_queue.put(address)

    threads = []
    for _ in range(NUM_THREADS):
        thread = threading.Thread(target=worker, args=(address_queue, nft_contract_addresses, results_queue))
        thread.start()
        threads.append(thread)

    # Display progress
    with tqdm(total=len(addresses), desc="Checking NFTs", unit="address") as pbar:
        while not address_queue.empty():
            address_queue.join()
            pbar.update(pbar.n)

    for thread in threads:
        thread.join()

    # Gather results
    results = {}
    while not results_queue.empty():
        address, owns_nft = results_queue.get()
        results[address] = owns_nft

    # Save results to file
    df = pd.DataFrame(list(results.items()), columns=['Address', 'Owns NFT'])
    df.to_csv(output_file, index=False)

if __name__ == "__main__":
    start_time = time.time()
    main(INPUT_FILE, OUTPUT_FILE)
    elapsed_time = time.time() - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
