# PFOF_Final

## Usage
It is recommended to scrape between 22:00 and 00:00, the data will be most complete. The `01_scrape.py` will approximatly take 2 hours to scrape one trading day. And the `02_analysis.py` will run up to 15 minutes.

1. Install the required packages:
    ```
    pip install requirements
    ```
2. Update the following variables in `01_scrape.py` to your needs:
    ```python
    exchanges = ['GETX', 'DUSC', 'DUSD', 'DEUR', 'DUSA', 'DUSB', 'HAMA', 'HAMB', 'HANA', 'HANB', 'DFRA', 'DGAT', 'LSEX', 'DETR']
    trans_types = ['post', 'pre']
    date = '2023.05.08'
    begin_time = '060000'
    stop_time = '220000'
    ```
3. Run the `01_scrape.py` script:
    ```
    python3 scrape.py
    ```
4. Ajust the following varaiables to your needs in the `02_analysis.py`:
    ```
    output_folder = f'/Users/lasse/Desktop/PFOF_Final/csv_export/{datetime.date.today().strftime("%Y-%m-%d")}'
    exchanges_analysis = ['DGAT', 'GETX', 'LSEX', 'DUSC', 'DUSD']
    exchanges_reference = ['DETR', 'DEUR', 'DFRA', 'DGAT', 'LSEX', 'DUSA', 'DUSB', 'DUSC', 'DUSD', 'HAMA', 'HAMB', 'HANA', 'HANB', 'GETX']
    ``` 

5. Run the `02_analysis.py` script, the finished csv files will be saved in the csv_export specifies in step 4.folder:
    ```
    python3 scrape.py
    ```

# Sources

## CTP - Lang & Schwarz Exchange
* https://www.ls-x.de/de/download
### Trading venues:
LSEX - Lang & Schwarz Exchange

## CTP - Gettex
* https://www.gettex.de/handel/delayed-data/pretrade-data
* https://www.gettex.de/handel/delayed-data/posttrade-data
### Trading venues:
GETX - Gettex Börse München

## CTP - Deutsche Börse
* https://www.mds.deutsche-boerse.com/mds-en/real-time-data/Delayed-data
### Trading venues:
DGAT - Tradegate
DETR - Xetra
DFRA - Börse Frankfurt
DEUR - Eurex (only derivatives)

## CTP - Börse AG
* https://www.boersenag.de/mifid-ii-delayed-data-pretrade/
* https://www.boersenag.de/mifid-ii-delayed-data/
### Trading venues:
DUSA - Börse Düsseldorf Regulierter Markt
DUSB - Börse Düsseldorf Freiverkehr
DUSC - Börse Düsseldorf Quotrix Regulierter Markt
DUSD - Börse Düsseldorf Quotrix Freiverkehr
HAMA - Börse Hamburg Regulierter Markt
HAMB - Börse Hamburg Freiverkehr
HANA - Börse Hannover Regulierter Markt
HANB - Börse Hannover Freiverkehr
