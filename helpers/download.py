import csv
import gzip
import json
import requests
from datetime import datetime, timedelta

def print_table_schema(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    schema = cursor.fetchall()
    print(f"Table schema for {table_name}:")
    for column in schema:
        print(column)


def download_files(url, exchange, trans_type):
    file_content = []  # Initialize file_content as an empty list
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Files of Deutsche Boerse have JSON format
            if exchange in ['DFRA', 'DGAT', 'DETR']: # DEUR not included too big
                file_content = gzip.decompress(response.content)
                file_content = json.loads(file_content)
                
            # Files from Lang and Schwarz come in the form of CSV
            elif exchange == 'LSEX':
                csv_content = response.content.decode('utf-8')
                csv_reader = csv.DictReader(csv_content.splitlines(), delimiter=';')
                file_content = [row for row in csv_reader]
                
            # Files with direct CSV content
            elif exchange in ['DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB']:
                csv_content = response.content.decode('utf-8')
                csv_reader = csv.DictReader(csv_content.splitlines(), delimiter=';')
                # Update the fieldnames to remove leading and trailing spaces
                csv_reader.fieldnames = [name.strip() for name in csv_reader.fieldnames]
                file_content = [row for row in csv_reader]
            
            elif exchange in ['GETX']:
                file_content = gzip.decompress(response.content)
                csv_content = file_content.decode('utf-8')
                csv_reader = csv.reader(csv_content.splitlines(), delimiter=',')
                if trans_type == 'post':
                    custom_headers = ['isin', 'timestamp', 'currency', 'price', 'volume']
                    file_content = [dict(zip(custom_headers, [row[0], row[1], row[2], float(row[3]), float(row[4])])) for row in csv_reader]
                elif trans_type == 'pre':
                    custom_headers = ['isin', 'timestamp', 'currency', 'bid_price', 'bid_volume', 'ask_price', 'ask_volume']
                    file_content = [dict(zip(custom_headers, [row[0], row[1], row[2], float(row[3]), float(row[4]), float(row[5]), float(row[6])])) for row in csv_reader]


                

    except Exception as e:
        print(f"Error downloading {url}: {e}")
    
    return file_content

        
def insert_data_to_db(db_conn, exchange, file_content, trans_type):
    cursor = db_conn.cursor()

    def convert_timestamp(timestamp_str):
        if timestamp_str is None or timestamp_str.startswith('ffff-ff-ffTff:ff:ff'):
            return None
        if exchange in ['DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB']:
            dt = datetime.strptime(timestamp_str, "%d.%m.%Y %H:%M:%S")
        elif exchange == 'GETX':
            dt = datetime.strptime(timestamp_str, "%H:%M:%S.%f")
        else:
            timestamp_str = timestamp_str[:-4] + "Z"  # Truncate the fractional part to 6 digits
            if exchange in ['LSEX'] and trans_type == 'pre':
                try:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%SZ")
            else:
                try:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
        iso_timestamp = dt.isoformat(timespec='seconds')  # Convert to ISO 8601 format string
        return iso_timestamp



    if trans_type == 'pre':

        '''if exchange in ['DETR']:
            for record in file_content:
                best_bid = record.get('mdBidMktDepthGroup1')
                best_ask = record.get('mdAskMktDepthGroup1')
                md_quotation_time = record.get('mdQuotationTime')

                if best_bid:
                    best_bid = best_bid[0]
                if best_ask:
                    best_ask = best_ask[0]

                data = (convert_timestamp(md_quotation_time) if md_quotation_time else None, record['symbol'],
                        float(best_bid['price']) if best_bid else None,
                        float(best_bid['quantity']) if best_bid else None,
                        float(best_ask['price']) if best_ask else None,
                        float(best_ask['quantity']) if best_ask else None,)
                cursor.execute(f'INSERT INTO {exchange}_{trans_type} (timestamp, ticker, bid_price, bid_volume, ask_price, ask_volume) VALUES (?, ?, ?, ?, ?, ?)', data)'''


        if exchange in ['DGAT']:
            for record in file_content:
                data = (convert_timestamp(record['quotationTime']), record['symbol'],
                        float(record.get('bid', None)) if record.get('bid', None) else None,
                        float(record.get('bidQty', None)) if record.get('bidQty', None) else None,
                        float(record.get('ask', None)) if record.get('ask', None) else None,
                        float(record.get('askQty', None)) if record.get('askQty', None) else None)
                cursor.execute(f'INSERT INTO {exchange}_{trans_type} (timestamp, ticker, bid_price, bid_volume, ask_price, ask_volume) VALUES (?, ?, ?, ?, ?, ?)', data)
        

        if exchange in ['DFRA']:
            for record in file_content:
                data = (convert_timestamp(record['quotationTime']), record['symbol'],
                        record.get('bestBid', None), record.get('bestBidQty', None),
                        record.get('bestAsk', None), record.get('bestAskQty', None))
                cursor.execute(f'INSERT INTO {exchange}_{trans_type} (timestamp, ticker, bid_price, bid_volume, ask_price, ask_volume) VALUES (?, ?, ?, ?, ?, ?)', data)

        if exchange in ['LSEX']:
            for record in file_content:
                data = (convert_timestamp(record.get('timestamp', None)), record['isin'],
                        float(record.get('bid', None).replace(',', '')) if record.get('bid', None) else None,
                        float(record.get('bidvolume', None).replace(',', '')) if record.get('bidvolume', None) else None,
                        float(record.get('ask', None).replace(',', '')) if record.get('ask', None) else None,
                        float(record.get('askvolume', None).replace(',', '')) if record.get('askvolume', None) else None,)
                cursor.execute(f'INSERT INTO {exchange}_{trans_type} (timestamp, ticker, bid_price, bid_volume, ask_price, ask_volume) VALUES (?, ?, ?, ?, ?, ?)', data)
        
        if exchange in ['GETX']:
            for record in file_content:
                data = (convert_timestamp(record['timestamp']), record['isin'],
                        float(record.get('bid_price', None)) if record.get('bid_price', None) else None,
                        float(record.get('bid_volume', None)) if record.get('bid_volume', None) else None,
                        float(record.get('ask_price', None)) if record.get('ask_price', None) else None,
                        float(record.get('ask_volume', None)) if record.get('ask_volume', None) else None,)
                cursor.execute(f'INSERT INTO {exchange}_{trans_type} (timestamp, ticker, bid_price, bid_volume, ask_price, ask_volume) VALUES (?, ?, ?, ?, ?, ?)', data)

    if trans_type == 'post':
        
        if exchange in ['DGAT', 'DFRA', 'DETR']:
            for record in file_content:
                data = (record['transIdCode'], convert_timestamp(record['lastTradeTime']),
                        record.get('symbol', None), record.get('lastTrade', None),
                        record.get('lastQty', None), record.get('currency', None),
                        )
                cursor.execute(f'INSERT INTO {exchange}_{trans_type} (id, timestamp, ticker, price, volume, currency) VALUES (?, ?, ?, ?, ?, ?)', data)
        
        if exchange in ['DEUR']:
            for record in file_content:
                if record.get("contractType") == "S":
                    data = (None, convert_timestamp(record.get('lastTradeTime')), record.get('symbol'),
                            float(record.get('notionalAmount', None)) if record.get('notionalAmount', None) else None,
                            float(record.get('lastQty', None)) if record.get('lastQty', None) else None,
                            record.get('currency', None),
                            )
                    cursor.execute(f'INSERT INTO {exchange}_{trans_type} (id, timestamp, ticker, price, volume, currency) VALUES (?, ?, ?, ?, ?, ?)', data)

        if exchange in ['LSEX']:   
            for record in file_content:
                data = (record['TVTIC'], convert_timestamp(record['tradeTime']),
                        record.get('isin', None),
                        float(record.get('price', None).replace(',', '.')) if record.get('price', None) else None,
                        float(record.get('size', None).replace(',', '.')) if record.get('size', None) else None,
                        record.get('currency', None),
                        )
                cursor.execute(f'INSERT INTO {exchange}_{trans_type} (id, timestamp, ticker, price, volume, currency) VALUES (?, ?, ?, ?, ?, ?)', data)

        if exchange in ['DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB']:
                    for record in file_content:
                        data = (None, convert_timestamp(record['time']),
                                record.get('ISIN', None),
                                float(record.get('price', None).replace(',', '.')) if record.get('price', None) else None,
                                float(record.get('size', None).replace(',', '.')) if record.get('size', None) else None,
                                None,
                                )
                        cursor.execute(f'INSERT INTO {exchange}_{trans_type} (id, timestamp, ticker, price, volume, currency) VALUES (?, ?, ?, ?, ?, ?)', data)
        
        if exchange in ['GETX']:
            for record in file_content:
                data = (None, convert_timestamp(record['timestamp']),
                        record.get('isin', None),
                        float(record.get('price', None)) if record.get('price', None) else None,
                        float(record.get('volume', None)) if record.get('volume', None) else None,
                        record.get('currency', None),
                        )
                cursor.execute(f'INSERT INTO {exchange}_{trans_type} (id, timestamp, ticker, price, volume, currency) VALUES (?, ?, ?, ?, ?, ?)', data)

                
    db_conn.commit()
    
    return f"Data inserted into the {exchange}_{trans_type} table."