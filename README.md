# NFT Ownership Checker

This Python script checks if Ethereum addresses own any NFTs using the Web3 library. The script supports multithreading to speed up the process and outputs the progress and results to a CSV file.

## Requirements

- Python 3.x
- Web3.py
- pandas
- tqdm

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/layerist/nft-ownership-checker.git
   cd nft-ownership-checker
   ```

2. Install the required Python packages:
   ```bash
   pip install web3 pandas tqdm
   ```

3. Set up your Infura project and replace the `infura_url` variable in the script with your Infura project URL.

4. Create a file named `erc721_abi.json` with the ABI for ERC-721 tokens.

5. Create a text file named `input_addresses.txt` with the Ethereum addresses you want to check, one per line.

## Usage

Run the script with the following command:
```bash
python check_nft_ownership.py
```

The script will output the progress and remaining time to the console and save the results to `nft_owners.csv`.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
