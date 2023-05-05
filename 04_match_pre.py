from helpers import con_setup, avrg_init, avrg_count, avrg_update, create_indexes_pre

def main():
    
    # initialize variables
    exchanges1 = ['DGAT', 'GETX', 'LSEX', 'DUSC', 'DUSD']
    exchanges2 = ['DETR', 'DEUR', 'DFRA', 'DGAT', 'LSEX', 'DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB']
    con, cur = con_setup()
    create_indexes_pre(cur, exchanges2)
    print('Indexes created')
    chunk_size = 50

    for exchange1 in exchanges1:

        cur.execute(f'SELECT row_number, bid_price, ask_price, ticker, timestamp FROM {exchange1}_post WHERE matched = 1')
        data1 = cur.fetchall()
            
        for row1 in data1:
            row_number, bid_price_venue, ask_price_venue, ticker, timestamp = row1
            print(row_number, timestamp, exchange1)
                
            for exchange2 in exchanges2:
                if exchange1 != exchange2:
                        cur.execute(f'''SELECT bid_price, ask_price FROM {exchange2}_pre WHERE timestamp = '{timestamp}' and ticker = '{ticker}' ''')
                        data2 = cur.fetchall()

                        # Initialize variables
                        bid_price_counter, new_avrg_bid = avrg_init(bid_price_venue)
                        ask_price_counter, new_avrg_ask = avrg_init(ask_price_venue)
                            
                        for row2 in data2:
                            bid_price, ask_price = row2
                            
                            # Count the average bid and ask price         
                            bid_price_counter, new_avrg_bid = avrg_count(bid_price, new_avrg_bid, bid_price_counter)
                            ask_price_counter, new_avrg_ask = avrg_count(ask_price, new_avrg_ask, ask_price_counter)
                               
                        
                        # Update the average bid and ask price
                        new_avrg_bid = avrg_update(bid_price_counter, new_avrg_bid)
                        if new_avrg_bid > 0:
                            cur.execute(f'UPDATE {exchange1}_post SET bid_price = ?, matched = 1 WHERE row_number = ?', (new_avrg_bid, row_number))
                            con.commit()
                            print('bid', new_avrg_bid, exchange1)
                        new_avrg_ask = avrg_update(ask_price_counter, new_avrg_ask)
                        if new_avrg_ask > 0:
                            cur.execute(f'UPDATE {exchange1}_post SET ask_price = ?, matched = 1 WHERE row_number = ?', (new_avrg_ask, row_number))
                            con.commit()
                            print('ask', new_avrg_ask, exchange1)


if __name__ == '__main__':
    main()