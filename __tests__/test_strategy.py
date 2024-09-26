import csv
import os
from datetime import datetime

# Define file paths (make path relative to the script's location)
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, "data", "transactions.csv")

def read_transaction_log(file_path):
    """
    Reads the transaction log from the CSV and returns it as a list of dictionaries.
    """
    transactions = []
    with open(file_path, mode="r") as file:
        csv_reader = csv.DictReader(file)
        
        # Normalize column names by stripping whitespaces and converting to lowercase
        csv_reader.fieldnames = [field.strip().lower() for field in csv_reader.fieldnames]
        
        for row in csv_reader:
            # Normalize keys to lowercase for consistent access
            row = {key.lower(): value for key, value in row.items()}
            
            # Check if necessary columns exist and are not empty
            if 'date' not in row or 'low price' not in row or not row['low price'] or 'highest peak' not in row or not row['highest peak']:
                print(f"Skipping row due to missing columns: {row}")
                continue

            # Convert the necessary fields to float for comparison
            row['low price'] = float(row['low price'])
            row['highest peak'] = float(row['highest peak'])

            # Convert date to a datetime object for filtering
            try:
                row['date_obj'] = datetime.strptime(row['date'], "%d/%m/%Y")
            except ValueError:
                print(f"Skipping row due to invalid date format: {row}")
                continue
            
            transactions.append(row)
    return transactions

def test_buy_signal():
    """
    Test that a buy was triggered after a specified percentage drop from the highest peak.
    """
    transactions = read_transaction_log(csv_file_path)
    failed_buys = 0
    passed_buys = 0
    active_investments = []  # Track number of purchases to adjust drop percentage

    start_date = transactions[0]['date_obj']  # Assume the first transaction's date is the start date

    for tx in transactions:
        date_obj = tx['date_obj']
        # Log only the first 2 years of data
        if (date_obj - start_date).days > 730:  # 730 days ~ 2 years
            continue

        low_price = tx['low price']
        highest_peak = tx['highest peak']  # Directly use the highest peak column
        action = tx['action'].lower()

        # Calculate the trigger price for the current potential buy
        trigger_price = highest_peak * pow(1 - 0.05, len(active_investments))
        
        # Log the relevant details for debugging
        print(f"Date: {tx['date']}, Low Price: {low_price}, Highest Peak: {highest_peak}, Trigger Price: {trigger_price}, Purchases Count: {len(active_investments)}")

        # Check if the current day's low price is below the trigger price for buying
        if low_price <= trigger_price:
            # Only log days when a buy was supposed to happen
            if action == 'buy':
                passed_buys += 1
                active_investments.append(tx)  # Log the purchase
                print(f"Buy Signal at {tx['date']}: Low Price = {low_price}, Highest Peak = {highest_peak}, Trigger Price = {trigger_price}, Purchases Count = {len(active_investments) - 1}")
            else:
                failed_buys += 1
                print(f"Failed Buy: Low Price = {low_price}, Trigger Price = {trigger_price} at {tx['date']}, but Action = '{tx['action']}'")

    # Print number of passed buys
    print(f"{passed_buys} buy signals passed\n")

    # Final assertion to verify no failed buys
    assert failed_buys == 0, f"{failed_buys} buy signals failed"

if __name__ == "__main__":
    # Running test
    test_buy_signal()
