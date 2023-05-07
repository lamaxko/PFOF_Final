from helpers import con_setup, create_indexes_xetra, create_indexes_pre, create_indexes_post, is_valid_price, export_files 

def main():
    # Initialize variables
    output_folder = '/Users/lasse/Desktop/PFOF_Final/csv_export'
    exchanges_analysis = ['DGAT', 'GETX', 'LSEX', 'DUSC', 'DUSD']
    exchanges_reference = ['DETR', 'DEUR', 'DFRA', 'DGAT', 'LSEX', 'DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB']
    con, cur = con_setup()

    # Create indexes and match with Xetra
    create_indexes_xetra(cur, exchanges_analysis)
    match_Xetra(con, cur, exchanges_analysis)
    print('Xetra matched')
    
    # Create indexes and match post transactions
    create_indexes_post(cur, exchanges_reference)
    match_post(con, cur, exchanges_analysis, exchanges_reference)
    print('Post matched')
    
    # Create indexes and match pre transactions
    create_indexes_pre(cur, exchanges_reference)
    match_pre(con, cur, exchanges_analysis, exchanges_reference)
    print('Pre matched')
    
    # Classify transactions
    classify_transtype(con, cur, exchanges_analysis)
    classify_qual(con, cur, exchanges_analysis)

    # Export files as CSV
    export_csv(con, cur, exchanges_analysis, output_folder)

    
def match_Xetra(con, cur, exchanges_analysis):

    # Initialize variables
    chunk_size = 200

    # Iterate through the exchanges to be analyzed
    for exchange in exchanges_analysis:
        offset = 0

        # Process data in chunks
        while True:
            # Retrieve a chunk of data from the current exchange
            cur.execute(f'''SELECT row_number, ticker, timestamp FROM {exchange}_post LIMIT {chunk_size} OFFSET {offset}''')
            data1 = cur.fetchall()

            # Iterate through rows in the current chunk
            for row1 in data1:
                row_number, ticker, timestamp = row1
                print(row_number, timestamp, exchange)

                # Find matching data in the DETR_post table
                cur.execute(f'''SELECT price FROM DETR_post WHERE timestamp = '{timestamp}' and ticker = '{ticker}' ''')
                data2 = cur.fetchall()

                # Update the matched data
                for row2 in data2:
                    price_xetra, = row2

                    # Update the exchange table with the new average price
                    cur.execute(f'UPDATE {exchange}_post SET price_xetra = ?, matched = 1 WHERE row_number = ?', (price_xetra, row_number))
                    con.commit()
                    print('New average price', price_xetra, exchange)

            # Break the loop if there is no more data to process
            if len(data1) < chunk_size:
                break

            # Update the offset for the next chunk
            offset += chunk_size 


def match_post(con, cur, exchanges_analysis, exchanges_reference):
    # Initialize variables
    chunk_size = 200

    # Iterate through the exchanges to be analyzed
    for exchange1 in exchanges_analysis:
        offset = 0

        while True:
            # Retrieve a chunk of data from the current exchange
            cur.execute(f'''SELECT row_number, price_average, highest_price, lowest_price, ticker, timestamp 
                            FROM {exchange1}_post LIMIT {chunk_size} OFFSET {offset}''')
            data1 = cur.fetchall()

            # Iterate through rows in the current chunk
            for row1 in data1:
                row_number, avrg_price, highest_price, lowest_price, ticker, timestamp = row1
                print(timestamp, exchange1)

                # Compare with other exchanges
                for exchange2 in exchanges_reference:
                    if exchange1 != exchange2:
                        # Find matching data in the exchange2_post table
                        cur.execute(f'''SELECT price FROM {exchange2}_post WHERE timestamp = '{timestamp}' and ticker = '{ticker}' ''')
                        data2 = cur.fetchall()

                        # Update the highest and lowest price
                        for row2 in data2:
                            price, = row2

                            # Update the highest price if needed
                            if is_valid_price(price):
                                if not is_valid_price(highest_price) or price > highest_price:
                                    cur.execute(f'UPDATE {exchange1}_post SET highest_price = ?, matched = 1 WHERE row_number = ?', (price, row_number))
                                    con.commit()
                                    highest_price = price
                                    print('New highest price', price, exchange2)

                            # Update the lowest price if needed
                                if not is_valid_price(lowest_price) or price < lowest_price:
                                    cur.execute(f'UPDATE {exchange1}_post SET lowest_price = ?, matched = 1 WHERE row_number = ?', (price, row_number))
                                    con.commit()
                                    lowest_price = price
                                    print('New lowest price', price, exchange2)

            if len(data1) < chunk_size:
                break

            offset += chunk_size



def match_pre(con, cur, exchanges_analysis, exchanges_reference):
    # Initialize variables
    print('Indexes created')
    chunk_size = 50

    # Iterate through the exchanges to be analyzed
    for exchange1 in exchanges_analysis:
        offset = 0

        # Process data in chunks
        while True:
            # Retrieve a chunk of data from the current exchange
            cur.execute(f'''SELECT row_number, best_bid_price, best_ask_price, ticker, timestamp 
                            FROM {exchange1}_post WHERE matched = 1 
                            LIMIT {chunk_size} OFFSET {offset}''')
            data1 = cur.fetchall()

            # Iterate through rows in the current chunk
            for row1 in data1:
                row_number, old_best_bid_price, old_best_ask_price, ticker, timestamp = row1
                print(row_number, timestamp, exchange1)

                # Compare with other exchanges
                for exchange2 in exchanges_reference:
                    if exchange1 != exchange2:
                        # Find matching data in the exchange2_pre table
                        cur.execute(f'''SELECT bid_price, ask_price 
                                        FROM {exchange2}_pre 
                                        WHERE timestamp = '{timestamp}' and ticker = '{ticker}' ''')
                        data2 = cur.fetchall()

                        # Update best bid and ask price
                        for row2 in data2:
                            new_bid_price, new_ask_price = row2

                            if old_best_bid_price is None or (new_bid_price is not None and new_bid_price > old_best_bid_price):
                                cur.execute(f'UPDATE {exchange1}_post SET best_bid_price = ? WHERE row_number = ?', (new_bid_price, row_number))
                                con.commit()
                                print('New best bid price', new_bid_price, exchange1)

                            if old_best_ask_price is None or (new_ask_price is not None and new_ask_price < old_best_ask_price):
                                cur.execute(f'UPDATE {exchange1}_post SET best_ask_price = ? WHERE row_number = ?', (new_ask_price, row_number))
                                con.commit()
                                print('New best ask price', new_ask_price, exchange1)

            # Break the loop if there is no more data to process
            if len(data1) < chunk_size:
                break

            # Update the offset for the next chunk
            offset += chunk_size


def classify_transtype(con, cur, exchanges_analysis):
    # Iterate through the exchanges to be analyzed
    for exchange in exchanges_analysis:
        # Retrieve data for matched transactions
        cur.execute(f'SELECT row_number, price, best_bid_price, best_ask_price FROM {exchange}_post WHERE matched = 1')
        data = cur.fetchall()

        # Classify the transaction type
        for row in data:
            row_number, price, best_bid_price, best_ask_price = row
            print('Price', price, 'Bid', best_bid_price, 'ASK', best_ask_price)

            if is_valid_price(price) and is_valid_price(best_bid_price) and is_valid_price(best_ask_price):
                
                # calculate the mid price for EFQ formular
                mid_price = (float(best_bid_price) + float(best_ask_price)) / 2

                if float(price) <= float(best_bid_price):
                    transtype = 'SELL'
                elif float(price) >= float(best_ask_price):
                    transtype = 'BUY'
                else:
                    transtype = 'NONE'

                cur.execute(f'UPDATE {exchange}_post SET mid_price = ?, trans_type = ? WHERE row_number = ?', (mid_price, transtype, row_number))
                con.commit()

def classify_qual(con, cur, exchanges_analysis):
    for exchange in exchanges_analysis:
        cur.execute(f'''SELECT row_number, price, best_bid_price, best_ask_price, highest_price, 
                        lowest_price, trans_type FROM {exchange}_post WHERE matched = 1''')
        data = cur.fetchall()

        for row in data:
            row_number, price, best_bid_price, best_ask_price, highest_price, lowest_price, trans_type = row

            # Classification after BBBO
            if is_valid_price(price) and is_valid_price(best_bid_price) and is_valid_price(best_ask_price):
                qual_BBBO = None

                if trans_type == 'BUY':
                    if float(price) > float(best_ask_price):
                        qual_BBBO = 'worse'
                    elif float(price) < float(best_bid_price):
                        qual_BBBO = 'better'
                    elif float(best_bid_price) < float(price) and float(price) < float(best_ask_price):
                        qual_BBBO = 'similar'

                if trans_type == 'SELL':
                    if float(price) < float(best_bid_price):
                        qual_BBBO = 'worse'
                    elif float(price) > float(best_ask_price):
                        qual_BBBO = 'better'
                    elif float(best_bid_price) < float(price) and float(price) < float(best_ask_price):
                        qual_BBBO = 'similar'

                print(qual_BBBO, exchange)
                cur.execute(f'UPDATE {exchange}_post SET qual_BBBO = ? WHERE row_number = ?', (qual_BBBO, row_number))
                con.commit()

            # Classification after CPM
            if is_valid_price(price) and is_valid_price(highest_price) and is_valid_price(lowest_price):
                qual_CPM = None

                if trans_type == 'BUY':
                    if float(price) > float(highest_price):
                        qual_CPM = 'worse'
                    elif float(price) < float(lowest_price):
                        qual_CPM = 'better'
                    elif float(lowest_price) < float(price) and float(price) < float(highest_price):
                        qual_CPM = 'similar'

                if trans_type == 'SELL':
                    if float(price) < float(lowest_price):
                        qual_CPM = 'worse'
                    elif float(price) > float(highest_price):
                        qual_CPM = 'better'
                    elif float(lowest_price) < float(price) and float(price) < float(highest_price):
                        qual_CPM = 'similar'

                print(qual_CPM, exchange)
                cur.execute(f'UPDATE {exchange}_post SET qual_CPM = ? WHERE row_number = ?', (qual_CPM, row_number))
                con.commit()


def export_csv(con, cur, exchanges_analysis, output_folder):
    export_files(exchanges_analysis, cur, output_folder)


if __name__ == '__main__':
    main()
