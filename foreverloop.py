from argparse import ArgumentParser
import gzip
import shutil
from pathlib import Path
import csv
from urllib.parse import urlparse
import os
import requests

def main(input_artifact: str, schema_value: str):
    #DATA_DIR = Path('C:/Users/svenm/Documents/Radboud/BachelorThesis/CSVtoTXT/data_separation/Data').expanduser()
    DATA_DIR = Path('/vol/csedu-nobackup/other/smeijboom/data_separation/').expanduser()
    input_dir = DATA_DIR / input_artifact

    with gzip.open(input_dir, 'rb') as f_in:
        with open('file.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    with open('file.csv', newline='') as f:
        reader = csv.reader(f)

        first_row = next(reader)
        if schema_value in first_row:
            i = first_row.index(schema_value)

            used_websites = set()
            used_links = set()

            websites = {}
            items = {}

            good_status_codes = 0
            fault_status_codes = 0

            for row in reader:
                if row[i] != "":
                    link = row[0]

                    try:
                        response = requests.get(link)
                        if response.status_code == 200:
                            good_status_codes += 1
                            up = urlparse(link).hostname
                            if up not in used_websites:
                                used_websites.add(up)
                                websites[up] = [link]
                            else:
                                websites[up] += [link]
                            
                            if link not in used_links:
                                used_links.add(link)
                                items[link] = [row[i]]
                            else:
                                items[link] += [row[i]]
                        else:
                            fault_status_codes += 1
                    except:
                        fault_status_codes += 1
            
            dataset_folder = "Data/telephone-set/"
            category = "university"
            schema_value_ab = schema_value.rsplit('/', 1)[1]

            if not os.path.exists(dataset_folder+"groundtruth"):
                os.makedirs(dataset_folder+"groundtruth")

            if not os.path.exists(dataset_folder+"groundtruth/" + category):
                os.makedirs(dataset_folder+"groundtruth/" + category)
            
            if not os.path.exists(dataset_folder+category):
                os.makedirs(dataset_folder+category)

            i = 0
            while i < 10:
                pass




if __name__ == '__main__':
    parser = ArgumentParser(description='Download html files and groundtruths from CSV database.')

    parser.add_argument('-f', '--input-file', type=str, required=True, help='name of the input file in the Data folder')
    parser.add_argument('-sv', '--schema-value', type=str, required=True, help='selected schema value')

    args = parser.parse_args()
    main(args.input_file, args.schema_value)