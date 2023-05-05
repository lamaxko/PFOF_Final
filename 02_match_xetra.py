from helpers import con_setup, create_indexes_xetra

def main():
    
    # initialize variables
    exchanges = ['DGAT', 'GETX', 'LSEX', 'DUSC', 'DUSD']
    con, cur = con_setup()
    create_indexes_xetra(cur, exchanges)
    chunk_size = 200

    for exchange in exchanges:
        offset = 0 

        while True:
            cur.execute(f'''SELECT row_number, ticker, timestamp FROM {exchange}_post LIMIT {chunk_size} OFFSET {offset}''')
            data1 = cur.fetchall()
            
            for row1 in data1:
                row_number, ticker, timestamp = row1
                print(row_number, timestamp, exchange)
                
                cur.execute(f'''SELECT price FROM DETR_post WHERE timestamp = '{timestamp}' and ticker = '{ticker}' ''')
                data2 = cur.fetchall()
                            
                for row2 in data2:
                    price_xetra, = row2 
                        
                    cur.execute(f'UPDATE {exchange}_post SET price_xetra = ?, matched = 1 WHERE row_number = ?', (price_xetra, row_number))
                    con.commit()
                    print('New average price', price_xetra, exchange)

            if len(data1) < chunk_size:
                break

            offset += chunk_size  

if __name__ == '__main__':
    main()
