from datetime import datetime, timedelta

def generate_time_list(exchange, date_time, begin_time, stop_time, trans_type):
    
    time_list = []
    
    if exchange in ['DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB']:
        if trans_type == 'post':
            formatted_time = date_time.replace('.', '')
            time_list.append(formatted_time)
    else:
        # Account for different file format of the deutsche boerse venues and Lang and Schwarz
        if exchange in ['DETR', 'DGAT', 'DEUR', 'DFRA']:
            start_time = datetime.combine(datetime.strptime(date_time, "%Y.%m.%d").date(), datetime.strptime(begin_time, "%H%M%S").time())
            end_time = datetime.combine(datetime.strptime(date_time, "%Y.%m.%d").date(), datetime.strptime(stop_time, "%H%M%S").time())
            interval = timedelta(minutes=1)
        elif exchange in ['LSEX']:
            start_time = datetime.combine(datetime.strptime(date_time, "%Y.%m.%d").date(), datetime.strptime(begin_time, "%H%M%S").time())
            end_time = datetime.combine(datetime.strptime(date_time, "%Y.%m.%d").date(), datetime.strptime(stop_time, "%H%M%S").time())
            interval = timedelta(minutes=5)
        elif exchange in ['GETX']:
            start_time = datetime.combine(datetime.strptime(date_time, "%Y.%m.%d").date(), datetime.strptime(begin_time, "%H%M%S").time())
            end_time = datetime.combine(datetime.strptime(date_time, "%Y.%m.%d").date(), datetime.strptime(stop_time, "%H%M%S").time())
            interval = timedelta(minutes=15)
        
        # Create list to store the times and load times 
        
        current_time = start_time
        
        while current_time <= end_time:
            formatted_time = ""  
            if exchange in ['DGAT', 'DFRA', 'DETR', 'DEUR']:
                formatted_time = current_time.strftime("%Y-%m-%dT%H:%M")
            elif exchange in ['LSEX']:
                formatted_time = current_time.strftime("%H%M%S")
            elif exchange in ['DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB']:
                formatted_time = current_time.strftime("%Y%m%d")  
            elif exchange in ['GETX']:
                formatted_time = current_time.strftime("%Y%m%d.%H.%M")
            time_list.append(formatted_time)
            current_time += interval
            
    return time_list


def generate_url_list(exchange, trans_type, date, begin_time, stop_time):
    
    # Get list of times to put into Links
    time_list = generate_time_list(exchange, date, begin_time, stop_time, trans_type)
    
    # Check for correct input
    if trans_type not in ['pre', 'post']:
        return 'wrong trans_type use pre or post'
    
    # Initialize and append the appropriate link to list
    links_to_download = []
    
    # LSEX post traded data is just one file
    if exchange == 'LSEX' and trans_type == 'post':
        links_to_download.append("https://www.ls-x.de/_rpc/json/.lstc/instrument/list/lsxtradesyesterday") 
        links_to_download.append("https://www.ls-x.de/_rpc/json/.lstc/instrument/list/lsxtradestoday") 
        
    # The other files come in the form of multiple links
    for timestamp in time_list:
        '''if exchange == 'DEUR' and trans_type == 'pre':
            links_to_download.append(f"https://mifid2-apa-data.deutsche-boerse.com/{exchange}-{trans_type}tradeOthers/{exchange}-{trans_type}tradeOthers-{timestamp}.json.gz")'''
        
        if exchange == 'DEUR' and trans_type == 'post':
           links_to_download.append(f"https://mifid2-apa-data.deutsche-boerse.com/{exchange}-{trans_type}trade/{exchange}-{trans_type}trade-{timestamp}.json.gz")
        
        elif exchange in ['DETR', 'DGAT', 'DFRA']: 
            links_to_download.append(f"https://mifid2-apa-data.deutsche-boerse.com/{exchange}-{trans_type}trade/{exchange}-{trans_type}trade-{timestamp}.json.gz")
        
        elif exchange == 'LSEX' and trans_type == 'pre':
            links_to_download.append(f'https://www.ls-x.de/_rpc/json/.lstc/instrument/list/lsxpretrades?time={timestamp}')
            
        elif exchange in ['DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB'] and trans_type == 'post':
            links_to_download.append(f"https://cld42.boersenag.de/m13data/data/Mifir13DelayedData_{exchange}_00000030_{timestamp}0000000000.csv")
        
        elif exchange in ['GETX']:
            links_to_download.append(f"https://erdk.bayerische-boerse.de/?u=edd-MUNCD&p=public&path=/{trans_type}trade/{trans_type}trade.{timestamp}.munc.csv.gz")
    return links_to_download

