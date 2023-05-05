from helpers import con_setup, export_files

def main():
    
    # initialize variables
    exchanges = ['DGAT', 'GETX', 'LSEX', 'DUSC', 'DUSD']
    output_folder = '/Users/lasse/Desktop/PFOF_Final/csv_export'
    con, cur = con_setup()
    
    export_files(exchanges, cur, output_folder)
    
if __name__ == '__main__':
    main()