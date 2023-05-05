from helpers import generate_url_list, db_setup_post, db_setup_pre, download_files, insert_data_to_db


def main():
   
    # Initialize variables Boerse ag venues only for current day
    exchanges = ['GETX', 'DUSC', 'DUSD', 'DEUR', 'DUSA', 'DUSB', 'HAMA', 'HAMB', 'HANA', 'HANB', 'DFRA', 'DGAT', 'LSEX', 'DETR']
    trans_types = ['post', 'pre']
    date = '2023.05.05'
    begin_time = '060000'
    stop_time =  '220000'
    
    # Set up databases
    db_conn = db_setup_post(exchanges)
    db_conn = db_setup_pre(exchanges)

    # Interate through exhanges and scrape data
    for trans_type in trans_types:
        for exchange in exchanges:
            url_list = generate_url_list(exchange, trans_type, date, begin_time, stop_time)
            for url in url_list:
                print(url)
                file_content = download_files(url, exchange, trans_type)
                response = insert_data_to_db(db_conn, exchange, file_content, trans_type)
                print(response)

if __name__ == '__main__':
    main()


 
