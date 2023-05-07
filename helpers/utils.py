def avrg_init(avrg_price):
    
    if is_valid_price(avrg_price):
        price_counter = 1
        new_avrg_price = float(avrg_price)
    else:
        price_counter = 0
        new_avrg_price = 0

    return price_counter, new_avrg_price

def avrg_update(price_counter, new_avrg_price):
    if price_counter > 0:
        new_avrg_price = float(new_avrg_price) / price_counter
    return new_avrg_price


def avrg_count(price, new_avrg, price_counter):
    if is_valid_price(price):
        price_counter += 1
        new_avrg = float(new_avrg) + float(price)
        
    return price_counter, new_avrg


def is_valid_price(price):
    if price is None or price == 'NoneType':
        return False
    elif float(price) == 0.0:
        return False
    else:
        return True
    

import csv
import os

def export_files(exchanges, cur, output_folder):
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for exchange in exchanges:
        cur.execute(f'''SELECT row_number, id, timestamp, ticker, price, volume, currency, best_bid_price, best_ask_price, price_xetra, highest_price, lowest_price, trans_type, qual_BBBO, qual_CPM, matched FROM {exchange}_post WHERE matched = 1''')  # Change this line to fetch from the '_pre' table
        data = cur.fetchall()

        # Export data to a CSV file
        with open(os.path.join(output_folder, f'{exchange}_post_data.csv'), 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            
            # Write the header row with updated column names
            csv_writer.writerow(['Exchange', 'Row Number', 'ID', 'Timestamp', 'Ticker', 'Price', 'Volume', 'Currency', 'Best Bid Price', 'Best Ask Price', 'Price Xetra', 'Highest Price', 'Lowest Price', 'Transaction Type', 'Qual BBBO', 'Qual CPM', 'Matched'])
            
            # Write data rows with updated column values
            for row in data:
                row_number, id, timestamp, ticker, price, volume, currency, best_bid_price, best_ask_price, price_xetra, highest_price, lowest_price , trans_type, qual_BBBO, qual_CPM, matched = row
                csv_writer.writerow([exchange, row_number, id, timestamp, ticker, price, volume, currency, best_bid_price, best_ask_price, price_xetra, highest_price, lowest_price, trans_type, qual_BBBO, qual_CPM, matched])

def create_indexes_pre(cur, exchanges):
    for exchange in exchanges:
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{exchange}_post_timestamp_ticker ON {exchange}_post (timestamp, ticker)')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{exchange}_post_row_number ON {exchange}_post (row_number)')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{exchange}_pre_timestamp_ticker ON {exchange}_pre (timestamp, ticker)')

        
def create_indexes_xetra(cur, exchanges):
    for exchange in exchanges:
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{exchange}_post_timestamp_ticker ON {exchange}_post (timestamp, ticker)')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{exchange}_post_row_number ON {exchange}_post (row_number)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_DETR_post_timestamp_ticker ON DETR_post (timestamp, ticker)')


def create_indexes_post(cur, exchanges):
    for exchange in exchanges:
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{exchange}_post_timestamp_ticker ON {exchange}_post (timestamp, ticker)')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{exchange}_post_row_number ON {exchange}_post (row_number)')
