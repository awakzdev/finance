import os
import re
import requests
import base64
import logging
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv

# -------------------------------
# Configuration and Setup
# -------------------------------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_data_automation.log"),
        logging.StreamHandler()
    ]
)

# Load environment variables from .env file
load_dotenv()
github_token = os.getenv('TOKEN')

if not github_token:
    logging.error("GitHub token not found in environment variables. Please set 'TOKEN' in your .env file.")
    exit(1)

repo = 'awakzdev/finance'  # Format: 'username/repo'
branch = 'main'             # Branch to which files will be uploaded

# Step 1: Fetch today's date in the format YYYY-MM-DD
today_date = datetime.now().strftime('%Y-%m-%d')

# Symbols to process
symbols = ['QLD', '^NDX']  # Add more symbols as needed

# GitHub API base URL
github_api_url = 'https://api.github.com'

# -------------------------------
# Utility Functions
# -------------------------------

def sanitize_symbol(symbol):
    """
    Sanitize the ticker symbol by removing any non-alphanumeric characters.
    This ensures compatibility with file systems and URLs.
    """
    sanitized = re.sub(r'[^\w]', '', symbol)
    logging.debug(f"Sanitized symbol: {symbol} -> {sanitized}")
    return sanitized

def get_file_sha(repo, path, branch, headers):
    """
    Get the SHA of a file in the GitHub repository.
    Returns the SHA string if the file exists, otherwise None.
    """
    url = f"{github_api_url}/repos/{repo}/contents/{path}?ref={branch}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        sha = response.json().get('sha')
        logging.debug(f"Found existing file '{path}' with SHA: {sha}")
        return sha
    elif response.status_code == 404:
        logging.debug(f"File '{path}' does not exist in the repository.")
        return None
    else:
        logging.error(f"Failed to fetch file SHA for '{path}'. Status Code: {response.status_code}")
        logging.error(f"Response: {response.json()}")
        return None

def upload_file_to_github(repo, path, branch, content_base64, commit_message, sha, headers):
    """
    Upload or update a file in the GitHub repository.
    """
    url = f"{github_api_url}/repos/{repo}/contents/{path}"
    payload = {
        'message': commit_message,
        'content': content_base64,
        'branch': branch
    }
    if sha:
        payload['sha'] = sha

    response = requests.put(url, headers=headers, json=payload)
    return response

# -------------------------------
# Main Processing Loop
# -------------------------------

def main():
    # GitHub API headers
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }

    for symbol in symbols:
        try:
            logging.info(f"Processing symbol: {symbol}")
            sanitized_symbol = sanitize_symbol(symbol)
            csv_filename = f"{sanitized_symbol.lower()}_stock_data.csv"
            file_path_in_repo = csv_filename  # Assuming root directory; adjust if needed

            # Step 2: Fetch historical data for the symbol
            logging.info(f"Fetching data for {symbol} from yfinance.")
            data = yf.download(symbol, start='2006-06-21', end=today_date)

            # Check if data is empty
            if data.empty:
                logging.warning(f"No data found for {symbol}. Skipping.")
                continue

            # Convert the index (dates) to the desired format (day/month/year)
            data.index = data.index.strftime('%d/%m/%Y')
            data.index.name = 'Date'  # Ensure the index has the correct name

            # Remove the column name to prevent extra header rows in CSV
            data.columns.name = None

            # Reorder the columns to match the desired output
            expected_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
            available_columns = [col for col in expected_columns if col in data.columns]

            if not available_columns:
                logging.warning(f"No expected columns found for {symbol}. Skipping.")
                continue

            data = data[available_columns]

            # Debug: Log the first few rows of the DataFrame
            logging.debug(f"Data for {symbol}:\n{data.head()}")

            # Step 3: Save the data to a CSV file with the sanitized symbol as a prefix
            logging.info(f"Writing data to CSV file: {csv_filename}")
            data.to_csv(csv_filename, index_label='Date', mode='w', header=True)

            # Step 4: Prepare to upload to GitHub
            sha = get_file_sha(repo, file_path_in_repo, branch, headers)

            if sha is None:
                commit_message = f"Create {sanitized_symbol} stock data"
                logging.info(f"Creating new file '{csv_filename}' in the repository.")
            else:
                commit_message = f"Update {sanitized_symbol} stock data"
                logging.info(f"Updating existing file '{csv_filename}' in the repository.")

            # Step 5: Read the new CSV file and encode it in base64
            with open(csv_filename, 'rb') as f:
                content = f.read()
            content_base64 = base64.b64encode(content).decode('utf-8')

            # Step 6: Upload the file to GitHub
            response = upload_file_to_github(
                repo=repo,
                path=file_path_in_repo,
                branch=branch,
                content_base64=content_base64,
                commit_message=commit_message,
                sha=sha,
                headers=headers
            )

            # Step 7: Check the response from GitHub
            if response.status_code in [200, 201]:
                action = "updated" if sha else "created"
                logging.info(f"File '{csv_filename}' {action} successfully in the repository.")
            else:
                logging.error(f"Failed to {'update' if sha else 'create'} the file '{csv_filename}' in the repository.")
                logging.error(f"Response Status Code: {response.status_code}")
                logging.error(f"Response: {response.json()}")

        except Exception as e:
            logging.error(f"An unexpected error occurred while processing '{symbol}': {e}")

if __name__ == "__main__":
    main()
