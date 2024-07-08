import time
import json
import threading
from web3 import Web3
import pandas as pd
from queue import Queue
from tqdm import tqdm

# Web3 setup (replace with your Infura or other node provider URL)
infura_url = "https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID"
web3 = Web3(Web3.HTTPProvider(infura_url))

# Load addresses from file
def load_addresses(file_path):
    with open(file_path, 'r') as f:
        addresses = [line.strip() for line in f]
    return addresses

# ERC-721 ABI
with open('erc721_abi.json', 'r') as f:
    erc721_abi = json.load(f)

# Function to check if address owns any NFTs
def check_nft_ownership(address, results):
    # List of known NFT contract addresses (replace with actual contract addresses)
    nft_contract_addresses = ["0x06012c8cf97BEaD5deAe237070F9587f8E7A266d", "0x..."]

    owns_nft = False
    for contract_address in nft_contract_addresses:
        contract = web3.eth.contract(address=contract_address, abi=erc721_abi)
        balance = contract.functions.balanceOf(address).call()
        if balance > 0:
            owns_nft = True
            break

    results[address] = owns_nft

# Multithreading setup
def worker(address_queue, results):
    while not address_queue.empty():
        address = address_queue.get()
        check_nft_ownership(address, results)
        address_queue.task_done()

def main(input_file, output_file):
    addresses = load_addresses(input_file)
    address_queue = Queue()
    results = {}

    for address in addresses:
        address_queue.put(address)

    num_threads = 10
    threads = []

    for _ in range(num_threads):
        thread = threading.Thread(target=worker, args=(address_queue, results))
        thread.start()
        threads.append(thread)

    # Display progress
    for _ in tqdm(range(len(addresses)), desc="Checking NFTs", unit="address"):
        address_queue.join()

    for thread in threads:
        thread.join()

    # Save results to file
    df = pd.DataFrame(list(results.items()), columns=['Address', 'Owns NFT'])
    df.to_csv(output_file, index=False)

if __name__ == "__main__":
    input_file = 'input_addresses.txt'
    output_file = 'nft_owners.csv'
    start_time = time.time()
    main(input_file, output_file)
    elapsed_time = time.time() - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
