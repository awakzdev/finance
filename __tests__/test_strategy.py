import csv
import os

# Define file paths
csv_file_path = os.path.join("data", "transactions.csv")

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
            # Convert the necessary fields to float for comparison
            row['price'] = float(row['price'])
            row['highest peak'] = float(row['highest peak'])
            row['down percentage from peak'] = float(row['down percentage from peak'])
            transactions.append(row)
    return transactions

def test_buy_signal():
    """
    Test that a buy was triggered after a 5% drop from the highest peak.
    """
    transactions = read_transaction_log(csv_file_path)
    failed_buys = 0
    passed_buys = 0

    last_buy_price = None
    highest_peak = None

    for tx in transactions:
        price = tx['price']
        
        # Set or update the highest peak based on new price values if it's the first transaction or price increases
        if highest_peak is None or price > highest_peak:
            highest_peak = price

        # If it's a buy signal, check if the price drop is valid
        if tx['action'].lower() == 'buy':
            # If no last buy price, this is the first buy, so we just log it
            if last_buy_price is None:
                last_buy_price = price
                passed_buys += 1
                continue

            # Calculate percentage drop from last buy price
            drop_percentage = ((last_buy_price - price) / last_buy_price) * 100

            # Check if it's at least a 5% drop since the last buy
            if drop_percentage >= 5:
                passed_buys += 1
                last_buy_price = price  # Update last buy price after a valid buy
            else:
                failed_buys += 1
                print(f"Failed Buy: Price dropped by {drop_percentage:.2f}% from the last buy at {tx['date']}")

    # Final assertion to verify no failed buys
    assert failed_buys == 0, f"{failed_buys} buy signals failed"
    print(f"{passed_buys} buy signals passed")


if __name__ == "__main__":
    # Running test
    test_buy_signal()
