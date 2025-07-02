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
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Initialize Web3
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    raise ConnectionError("Failed to connect to Ethereum via Infura.")


def load_lines(file_path: Path) -> Set[str]:
    """
    Load unique, non-empty lines from a file.
    """
    if not file_path.exists():
        logging.error(f"File not found: {file_path}")
        return set()
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except Exception as e:
        logging.exception(f"Error reading {file_path}: {e}")
        return set()


def load_abi(file_path: Path) -> Optional[List[dict]]:
    """
    Load the contract ABI from a JSON file.
    """
    if not file_path.exists():
        logging.error(f"ABI file not found: {file_path}")
        return None
    try:
        with file_path.open("r", encoding="utf-8") as f:
            abi = json.load(f)
        if isinstance(abi, list):
            return abi
        logging.error("Invalid ABI format: expected a list of dicts.")
    except Exception as e:
        logging.exception(f"Failed to load ABI: {e}")
    return None


def init_contract(address: str, abi: List[dict]) -> Optional[Contract]:
    """
    Initialize a Web3 contract instance.
    """
    try:
        return web3.eth.contract(address=web3.to_checksum_address(address), abi=abi)
    except Exception as e:
        logging.error(f"Failed to initialize contract at {address}: {e}")
        return None


def has_nft(address: str, contract: Contract) -> bool:
    """
    Check whether the given address holds at least one token in the specified contract.
    """
    try:
        balance = contract.functions.balanceOf(web3.to_checksum_address(address)).call()
        return balance > 0
    except exceptions.ContractLogicError as e:
        logging.warning(f"Contract logic error on {contract.address}: {e}")
    except Exception as e:
        logging.error(f"Error checking NFT ownership for {address} on {contract.address}: {e}")
    return False


def check_nft_ownership(address: str, contracts: List[Contract]) -> Tuple[str, bool]:
    """
    Check if an address owns any NFT across multiple contracts.
    """
    owns_nft = any(has_nft(address, contract) for contract in contracts)
    logging.info(f"{address} owns NFT: {owns_nft}")
    return address, owns_nft


def save_results(results: List[Tuple[str, bool]], file_path: Path) -> None:
    """
    Save the ownership results as a CSV file.
    """
    try:
        df = pd.DataFrame(results, columns=["Address", "Owns NFT"])
        df.to_csv(file_path, index=False)
        logging.info(f"Results saved to {file_path}")
    except Exception as e:
        logging.exception(f"Failed to save CSV: {e}")


def filter_addresses(addresses: Set[str]) -> List[str]:
    """
    Filter and convert a set of addresses to checksum addresses.
    """
    valid = []
    for addr in addresses:
        if web3.is_address(addr):
            valid.append(web3.to_checksum_address(addr))
        else:
            logging.warning(f"Skipping invalid address: {addr}")
    return valid


def load_contracts(addresses: Set[str], abi: List[dict]) -> List[Contract]:
    """
    Load and validate all contract instances from their addresses.
    """
    contracts = [init_contract(addr, abi) for addr in addresses]
    valid_contracts = [c for c in contracts if c]
    if not valid_contracts:
        logging.error("No valid contracts were initialized.")
    return valid_contracts


def main() -> None:
    """
    Main execution logic.
    """
    addresses = load_lines(INPUT_FILE)
    contract_addresses = load_lines(CONTRACTS_FILE)
    abi = load_abi(ABI_FILE)

    if not addresses:
        logging.error("No addresses found to check.")
        return
    if not contract_addresses:
        logging.error("No NFT contract addresses provided.")
        return
    if abi is None:
        logging.error("No valid ABI loaded.")
        return

    valid_addresses = filter_addresses(addresses)
    contracts = load_contracts(contract_addresses, abi)
    if not contracts:
        return

    results: List[Tuple[str, bool]] = []

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_to_address = {
            executor.submit(check_nft_ownership, address, contracts): address
            for address in valid_addresses
        }
        with tqdm(total=len(future_to_address), desc="Checking NFT ownership", unit="address") as pbar:
            for future in as_completed(future_to_address):
                address = future_to_address[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logging.error(f"Unhandled exception checking {address}: {e}")
                finally:
                    pbar.update(1)

    save_results(results, OUTPUT_FILE)


if __name__ == "__main__":
    start = time.time()
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
    elapsed = time.time() - start
    logging.info(f"Script completed in {elapsed:.2f} seconds.")
    print(f"Done in {elapsed:.2f} seconds.")
