from pathlib import Path
from argparse import ArgumentParser
import csv
import gzip
import shutil
from urllib.parse import urlparse
import requests
import os

def main(txt_input: str, schema_value: str):
    #DATA_DIR = Path('/vol/csedu-nobackup/other/smeijboom/data_separation/Data').expanduser()
    #DATA_DIR = Path('C:/Users/svenm/Documents/Radboud/BachelorThesis/CSVtoTXT/data_separation/Data').expanduser()
    #input_csv_dir = DATA_DIR / csv_input

    #with gzip.open(input_csv_dir, 'rb') as f_in:
    #    with open('file.csv', 'wb') as f_out:
    #        shutil.copyfileobj(f_in, f_out)

    #TXT_DIR = Path('C:/Users/svenm/Documents/Radboud/BachelorThesis/CSVtoTXT/data_separation/').expanduser()
    TXT_DIR = Path('/vol/csedu-nobackup/other/smeijboom/data_separation/').expanduser()
    input_txt_dir = TXT_DIR / txt_input
    with open(input_txt_dir) as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            url, schema_type = row[0], row[2]
            find_download_websites(url, schema_type, schema_value)




def find_download_websites(url: str, schema_type: str, schema_value: str):
    with open('file.csv', newline='') as f:
        reader = csv.reader(f)

        first_row = next(reader)
        if schema_value in first_row:
            i = first_row.index(schema_value)

            used_urls = set()
            items = {}

            total_attribute_values = 0
            unique_attribute_values = set()

            for row in reader:
                up = urlparse(row[0])
                if up.hostname == url: #check url constraint
                    if row[2] in schema_type: #check schema type constraint
                        if row[0] not in used_urls:
                            used_urls.add(row[0])
                            items[row[0]] = [row[i]]
                        else:
                            items[row[0]] += [row[i]]
                        total_attribute_values += 1
                        unique_attribute_values.add(row[i])

            dataset_folder = "Data/telephone-set/"
            category = "university"
            schema_value_ab = schema_value.rsplit('/', 1)[1]

            if not os.path.exists(dataset_folder+"groundtruth"):
                os.makedirs(dataset_folder+"groundtruth")

            if not os.path.exists(dataset_folder+"groundtruth/" + category):
                os.makedirs(dataset_folder+"groundtruth/" + category)

            write_out = open(dataset_folder+"groundtruth/"+category+"/"+category+"-"+url+"-"+schema_value_ab+".txt", "w")
            write_out.write(category + '\t' + url + '\t' + schema_value + '\n')
            write_out.write(str(len(used_urls)) + '\t' + str(len(used_urls)) + '\t' + str(total_attribute_values) + '\t' + str(len(unique_attribute_values)) + '\n')
            
            id = 0

            fault_403 = 0
            fault_404 = 0
            fault_else = 0
            good = 0

            if not os.path.exists(dataset_folder+category):
                os.makedirs(dataset_folder+category)

            if not os.path.exists(dataset_folder+category+"/"+category+"-"+url):
                os.makedirs(dataset_folder+category+"/"+category+"-"+url)
            
            for item in items:
                try:
                    with open(dataset_folder+category+"/"+category+"-"+url+"/"+str(id)+".htm", mode="wb") as file:
                        file.write(requests.get(item).content)
            
                    write_out.write(str(id) + '\t' + str(len(items[item])))
                    for it in items[item]:
                        write_out.write('\t' + it.strip('"\''))
                    write_out.write('\n')

                    id += 1

                    response = requests.get(item)
                    if (response.status_code == 403):
                        fault_403 += 1
                    elif (response.status_code == 404):
                        fault_404 += 1
                    elif (response.status_code == 200):
                        good += 1
                    else:
                        fault_else += 1
                except:
                    print(item + ": not requestable")

            print("Good: " + str(good))
            print("fault 403: " + str(fault_403))
            print("fault 404: " + str(fault_404))
            print("fault else: " + str(fault_else))

        


if __name__ == '__main__':
    parser = ArgumentParser(description='Download html files and groundtruths from CSV database.')

    parser.add_argument('-f', '--input-file', type=str, required=True, help='name of the input file in the Data folder')
    parser.add_argument('-sv', '--schema-value', type=str, required=True, help='selected schema value')

    args = parser.parse_args()
    main(args.input_file, args.schema_value)