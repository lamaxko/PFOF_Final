from helpers import con_setup, avrg_init, avrg_count, avrg_update, create_indexes_post

def main():

    # initialize variables
    exchanges1 = ['DGAT', 'GETX', 'LSEX', 'DUSC', 'DUSD']
    exchanges2 = ['DETR', 'DEUR', 'DFRA', 'DGAT', 'LSEX', 'DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB']
    con, cur = con_setup()
    create_indexes_post(exchanges2, cur)

    for exchange1 in exchanges1:
        cur.execute(f'SELECT row_number, price_average, ticker, timestamp FROM {exchange1}_post WHERE matched = 1')
        data1 = cur.fetchall()
        
        for row1 in data1:
            row_number, price_average, ticker, timestamp = row1
            print(row_number, timestamp, exchange1)
                        
            for exchange2 in exchanges2:
                if exchange1 != exchange2:
                    cur.execute(f'''SELECT price FROM {exchange2}_post WHERE timestamp = '{timestamp}' and ticker = '{ticker}' ''')
                    data2 = cur.fetchall()

                    # Initialize variables
                    price_counter, new_avrg_price = avrg_init(price_average)
                                    
                    for row2 in data2:
                        price, = row2 # Unpack the price value from the tuple
                                    
                        # Count the average bid and ask price         
                        price_counter, new_avrg_price = avrg_count(price, new_avrg_price, price_counter)
                                
                        # Update the average price
                        new_avrg_price = avrg_update(price_counter, new_avrg_price)
                        if new_avrg_price > 0:
                            cur.execute(f'UPDATE {exchange1}_post SET price_average = ?, matched = 1 WHERE row_number = ?', (new_avrg_price, row_number))
                            con.commit()
                            print('New average price', new_avrg_price, exchange2)


if __name__ == '__main__':
    main()
