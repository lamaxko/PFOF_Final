from helpers import con_setup, is_valid_price

def main():

    # initialize variables
    exchanges = ['DGAT', 'GETX', 'LSEX', 'DUSC', 'DUSD']
    con, cur = con_setup()

    for exchange in exchanges:
        cur.execute(f'SELECT row_number, price, bid_price, ask_price FROM {exchange}_post WHERE matched = 1')
        data = cur.fetchall()
        
        for row in data:
            row_number, price, bid_price, ask_price = row
            print('Price', price, 'Bid', bid_price, 'ASK', ask_price)
            
            if is_valid_price(price) and is_valid_price(bid_price) and is_valid_price(ask_price):
                
                new_avrg_price = (float(bid_price) + float(ask_price)) / 2
                
                if float(price) > float(new_avrg_price): # flip signgs 
                    transtype = 'BUY'
                elif float(price) < float(new_avrg_price): # flip signgs
                    transtype = 'SELL'
                else:
                    transtype = 'NONE'
                
                print('BIG MATCH', transtype, new_avrg_price)
                cur.execute(f'UPDATE {exchange}_post SET price_average = ?, trans_type = ? WHERE row_number = ?', (new_avrg_price, transtype, row_number))
                con.commit()
                
            elif is_valid_price(price) and is_valid_price(bid_price):
                
                if float(price) < float(bid_price): # flip signgs
                    transtype = 'SELL'

                    print('SMALL MATCH', transtype, bid_price)
                    cur.execute(f'UPDATE {exchange}_post SET trans_type = ? WHERE row_number = ?', (transtype, row_number))
                    con.commit()
                    
            elif is_valid_price(price) and is_valid_price(ask_price):

                if float(price) > float(ask_price): # flip signgs
                    transtype = 'BUY'
                    
                    print('SMALL MATCH', transtype, ask_price)
                    cur.execute(f'UPDATE {exchange}_post SET trans_type = ? WHERE row_number = ?', (transtype, row_number))
                    con.commit()


if __name__ == '__main__':
    main()
