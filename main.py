import os
import re
import requests
import base64
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# -------------------------------
# Configuration and Setup
# -------------------------------

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
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

def validate_csv(csv_path, expected_columns):
    """
    Validate the CSV file to ensure it has the correct format.
    Returns True if valid, False otherwise.
    """
    try:
        df = pd.read_csv(csv_path)
        # Check if expected columns are present and in the correct order
        if list(df.columns) != expected_columns:
            logging.error(f"CSV '{csv_path}' has incorrect columns.")
            logging.error(f"Expected columns: {expected_columns}")
            logging.error(f"Found columns: {list(df.columns)}")
            return False
        # Check if there's at least one row of data
        if df.shape[0] == 0:
            logging.error(f"CSV '{csv_path}' is empty.")
            return False
        # Additional checks can be added here as needed
        logging.debug(f"CSV '{csv_path}' passed validation.")
        return True
    except Exception as e:
        logging.error(f"Failed to validate CSV '{csv_path}': {e}")
        return False

def clean_csv(csv_path):
    """
    Remove corrupted rows from the CSV file.
    Specifically, remove any row after the header that starts with 'Ticker' or 'Date'.
    """
    try:
        with open(csv_path, 'r') as f:
            lines = f.readlines()
        
        if len(lines) < 2:
            logging.warning(f"CSV '{csv_path}' does not have enough rows to clean.")
            return
        
        # Initialize cleaned_lines with the header
        cleaned_lines = [lines[0]]
        cleaned = False

        # Iterate over the remaining lines and exclude corrupted ones
        for idx, line in enumerate(lines[1:], start=2):
            stripped_line = line.strip()
            if stripped_line.startswith(('Ticker', 'Date')):
                logging.warning(f"Removing corrupted row {idx} from '{csv_path}': {stripped_line}")
                cleaned = True
                continue
            cleaned_lines.append(line)
        
        if cleaned:
            with open(csv_path, 'w') as f:
                f.writelines(cleaned_lines)
            logging.info(f"Cleaned corrupted rows from '{csv_path}'.")
        else:
            logging.info(f"No corrupted rows found in '{csv_path}'.")
    
    except Exception as e:
        logging.error(f"Failed to clean CSV '{csv_path}': {e}")

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
            expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
            available_columns = ['Date'] + [col for col in ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'] if col in data.columns]

            if len(available_columns) != len(expected_columns):
                logging.warning(f"Missing expected columns for {symbol}. Expected columns: {expected_columns}, Available columns: {available_columns}")
                # Optionally skip this symbol
                continue

            # Reset index to include 'Date' as a column
            data.reset_index(inplace=True)

            # Select and reorder columns
            data = data[expected_columns]

            # Debug: Log the first few rows of the DataFrame
            logging.debug(f"Data for {symbol}:\n{data.head()}")

            # Step 3: Save the data to a CSV file with the sanitized symbol as a prefix
            logging.info(f"Writing data to CSV file: {csv_filename}")
            data.to_csv(csv_filename, index=False, mode='w', header=True)

            # Step 4: Clean the CSV by removing corrupted rows
            logging.info(f"Cleaning CSV file: {csv_filename}")
            clean_csv(csv_filename)

            # Step 5: Validate the cleaned CSV
            logging.info(f"Validating CSV file: {csv_filename}")
            if not validate_csv(csv_filename, expected_columns):
                logging.error(f"Validation failed for '{csv_filename}'. The file will not be uploaded.")
                # Optionally remove the corrupted CSV file
                try:
                    os.remove(csv_filename)
                    logging.info(f"Corrupted CSV file '{csv_filename}' has been removed.")
                except Exception as e:
                    logging.error(f"Failed to remove corrupted CSV file '{csv_filename}': {e}")
                continue  # Skip uploading this file

            # Step 6: Prepare to upload to GitHub
            sha = get_file_sha(repo, file_path_in_repo, branch, headers)

            if sha is None:
                commit_message = f"Create {sanitized_symbol} stock data"
                logging.info(f"Creating new file '{csv_filename}' in the repository.")
            else:
                commit_message = f"Update {sanitized_symbol} stock data"
                logging.info(f"Updating existing file '{csv_filename}' in the repository.")

            # Step 7: Read the new CSV file and encode it in base64
            with open(csv_filename, 'rb') as f:
                content = f.read()
            content_base64 = base64.b64encode(content).decode('utf-8')

            # Step 8: Upload the file to GitHub
            logging.info(f"Uploading '{csv_filename}' to GitHub.")
            response = upload_file_to_github(
                repo=repo,
                path=file_path_in_repo,
                branch=branch,
                content_base64=content_base64,
                commit_message=commit_message,
                sha=sha,
                headers=headers
            )

            # Step 9: Check the response from GitHub
            if response.status_code in [200, 201]:
                action = "updated" if sha else "created"
                logging.info(f"File '{csv_filename}' {action} successfully in the repository.")
            else:
                logging.error(f"Failed to {'update' if sha else 'create'} the file '{csv_filename}' in the repository.")
                logging.error(f"Response Status Code: {response.status_code}")
                logging.error(f"Response: {response.json()}")

        except Exception as e:
            logging.error(f"An unexpected error occurred while processing '{symbol}': {e}")
            # Optionally, remove the CSV file if it exists
            if os.path.exists(csv_filename):
                try:
                    os.remove(csv_filename)
                    logging.info(f"CSV file '{csv_filename}' has been removed due to an error.")
                except Exception as remove_error:
                    logging.error(f"Failed to remove CSV file '{csv_filename}': {remove_error}")

if __name__ == "__main__":
    main()
