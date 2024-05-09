from argparse import ArgumentParser
import gzip
import shutil
from pathlib import Path
import csv
from urllib.parse import urlparse
import os
import requests

def main(input_artifact: str, schema_type: str, schema_value: str):
    #DATA_DIR = Path('C:/Users/svenm/Documents/Radboud/BachelorThesis/CSVtoTXT/data_separation/Data').expanduser()
    DATA_DIR = Path('/vol/csedu-nobackup/other/smeijboom/data_separation/').expanduser()
    input_dir = DATA_DIR / input_artifact

    with gzip.open(input_dir, 'rb') as f_in:
        with open('file.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    no_attributes = 0
    with open('file.csv', newline='') as f:
        reader = csv.reader(f)

        first_row = next(reader)
        if schema_value in first_row:
            i = first_row.index(schema_value)

            used_websites = set()
            used_links = set()

            websites = {}
            items = {}

            for row in reader:
                if row[2] == schema_type:
                    if row[i] != "":
                        link = row[0]

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
    
    with open("no_links.txt", mode="w") as file:
        file.write("Number of links: " + str(len(used_links)) + '\n')


if __name__ == '__main__':
    parser = ArgumentParser(description='Check number of attributes that contain schema value.')

    parser.add_argument('-f', '--input-file', type=str, required=True, help='name of the input file in the Data folder')
    parser.add_argument('-st', '--schema-type', type=str, required=True, help='selected schema type')
    parser.add_argument('-sv', '--schema-value', type=str, required=True, help='selected schema value')

    args = parser.parse_args()
    main(args.input_file, args.schema_type, args.schema_value)