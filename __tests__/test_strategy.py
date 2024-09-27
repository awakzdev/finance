import csv
import os
from datetime import datetime

# Define file paths (make path relative to the script's location)
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, "data", "transactions.csv")

# Parameters for the trading strategy
INITIAL_CASH = 5000  # Example initial cash balance
INVESTMENT_PER_BUY = 5000  # Fixed amount to invest per buy

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
                continue

            # Convert the necessary fields to float for comparison
            row['low price'] = float(row['low price'])
            row['highest peak'] = float(row['highest peak'])

            # Convert date to a datetime object for filtering
            try:
                row['date_obj'] = datetime.strptime(row['date'], "%d/%m/%Y")
            except ValueError:
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
    active_investments = []  # Track purchases
    original_highest_peak = None  # Track the initial highest peak to manage purchases
    stocks_sold = True  # Flag to determine if all stocks have been sold
    last_purchase_price = None  # Track the lowest purchase price for the next 5% drop calculation
    cash_balance = INITIAL_CASH  # Initialize cash balance

    start_date = transactions[0]['date_obj']  # Assume the first transaction's date is the start date

    for tx in transactions:
        date_obj = tx['date_obj']
        # Log only the first 2 years of data
        if (date_obj - start_date).days > 730:  # 730 days ~ 2 years
            continue

        low_price = tx['low price']
        highest_peak = tx['highest peak']  # Directly use the highest peak column
        action = tx['action'].lower()

        # Set the original highest peak if stocks have been sold
        if stocks_sold:
            original_highest_peak = highest_peak
            last_purchase_price = original_highest_peak  # Initialize the last purchase price to the highest peak
            stocks_sold = False  # Reset flag

        # Calculate the trigger price based on the last purchase price
        current_drop_percentage = 0.95 ** (len(active_investments) + 1)
        trigger_price = last_purchase_price * current_drop_percentage

        # Check if the current day's low price is below the trigger price for buying
        if low_price <= trigger_price:
            # Check if there is enough cash to make the purchase
            if cash_balance >= INVESTMENT_PER_BUY:
                if action == 'buy':
                    passed_buys += 1
                    active_investments.append({'price': low_price, 'date': tx['date'], 'amount': INVESTMENT_PER_BUY})  # Log the purchase
                    last_purchase_price = low_price  # Set the lowest price of the current purchase
                    cash_balance -= INVESTMENT_PER_BUY  # Deduct the investment amount from cash balance
                    print(f"Buy Signal at {tx['date']}: Low Price = {low_price}, Original Highest Peak = {original_highest_peak}, Trigger Price = {trigger_price}, Purchases Count = {len(active_investments)}, Cash Balance = {cash_balance}")
                else:
                    failed_buys += 1
                    print(f"Failed Buy: Low Price = {low_price}, Trigger Price = {trigger_price} at {tx['date']}, but Action = '{tx['action']}'")
            else:
                print(f"Insufficient Cash: Low Price = {low_price}, Trigger Price = {trigger_price} at {tx['date']}, Cash Balance = {cash_balance}")

        # Check if it's a sell action and mark stocks as sold if no active investments are left
        if action == 'sell' and len(active_investments) > 0:
            last_investment = active_investments.pop()  # Sell the most recent stock
            sell_amount = last_investment['amount']
            cash_balance += sell_amount  # Add the investment amount back to the cash balance
            print(f"Sell Signal at {tx['date']}: Action = 'sell', Remaining Purchases = {len(active_investments)}, Cash Balance = {cash_balance}")
            if len(active_investments) == 0:
                stocks_sold = True  # All stocks sold, ready to update the highest peak

    # Print number of passed buys
    print(f"{passed_buys} buy signals passed\n")

    # Final assertion to verify no failed buys
    assert failed_buys == 0, f"{failed_buys} buy signals failed"

if __name__ == "__main__":
    # Running test
    test_buy_signal()
