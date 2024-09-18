import os
import requests
import base64
import yfinance as yf
from datetime import datetime

github_token = os.getenv('TOKEN')
repo = 'awakzdev/test'
branch = 'main'
file_path_in_repo = 'qld_stock_data.csv'

# Step 1: Fetch today's date in the format day/month/year
today_date = datetime.now().strftime('%Y-%m-%d')

# Step 2: Fetch historical data for QLD (NASDAQ x2)
data = yf.download('QLD', start='2019-07-26', end=today_date)

# Convert the index (dates) to the desired format (day/month/year)
data.index = data.index.strftime('%d/%m/%Y')

# Step 3: Save the data to a CSV file
csv_filename = 'qld_stock_data.csv'
data.to_csv(csv_filename)

# Step 4: Get the current file's SHA (needed to update a file in the repository)
url = f'https://api.github.com/repos/{repo}/contents/{file_path_in_repo}'
headers = {'Authorization': f'token {github_token}'}

try:
    # Try to get the file's SHA
    response = requests.get(url, headers=headers)
    response_json = response.json()
    
    if response.status_code == 200:
        # File exists, extract the SHA
        sha = response_json['sha']
        print('File exists, updating it.')
    elif response.status_code == 404:
        # File does not exist, we'll create a new one
        sha = None
        print('File does not exist, creating a new one.')
    else:
        # Other errors
        print(f'Unexpected error: {response_json}')
        exit(1)
except Exception as e:
    print(f'Error fetching file info: {e}')
    exit(1)

# Step 5: Read the new CSV file and encode it in base64
with open(csv_filename, 'rb') as f:
    content = f.read()
content_base64 = base64.b64encode(content).decode('utf-8')

# Step 6: Create the payload for the GitHub API request
commit_message = 'Update QLD stock data'
data = {
    'message': commit_message,
    'content': content_base64,
    'branch': branch
}

# Include the SHA if the file exists (for updating)
if sha:
    data['sha'] = sha

# Step 7: Push the file to the repository
response = requests.put(url, headers=headers, json=data)

# Check if the file was updated/created successfully
if response.status_code in [200, 201]:
    print('File updated successfully in the repository.')
else:
    print('Failed to update the file in the repository.')
    print('Response:', response.json())
