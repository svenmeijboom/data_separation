from argparse import ArgumentParser
import gzip
import shutil
from pathlib import Path
import csv
from urllib.parse import urlparse
import os
import requests
from itertools import islice

def chunks(data, SIZE=10000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}

def main(input_artifact: str, schema_value: str, file_count: int):
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

            for row in reader:
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
            

            folders = chunks(websites, int(len(websites)/10))
            
            dataset_folder = "Data/telephone-set/"
            category = "university"
            schema_value_ab = schema_value.rsplit('/', 1)[1]

            if not os.path.exists(dataset_folder+"groundtruth/" + category):
                os.makedirs(dataset_folder+"groundtruth/" + category)
            
            folnr = 0
            for websites in folders:
                while folnr < 10:
                    website_name = "various_websites" + str(folnr)

                    if not os.path.exists(dataset_folder+category+"/"+category+"-"+website_name):
                        os.makedirs(dataset_folder+category+"/"+category+"-"+website_name)

                    name_gt_file = dataset_folder+"groundtruth/"+category+"/"+category+"-"+website_name+"-"+schema_value_ab+".txt"
                    with open(name_gt_file, "a") as gt_file:
                        gt_file.write(category + '\t' + website_name + '\t' + schema_value + '\n')
                        gt_file.close()

                    id = 0
                    good_status_codes = 0
                    fault_status_codes = 0

                    all_attributes = []

                    for website in websites:
                        for link in websites[website]:
                            if id >= file_count:
                                break
                            try:
                                response = requests.get(link)
                                if response.status_code == 200:
                                    good_status_codes += 1

                                    with open(dataset_folder+category+"/"+category+"-"+website_name+"/"+str(id)+".htm", mode="wb") as file:
                                        file.write(response.content)
                            
                                    with open(name_gt_file, "a") as gt_file:
                                        gt_file.write(str(id) + '\t' + str(len(items[link])))
                                        for it in items[link]:
                                            gt_file.write('\t' + it.strip('"\''))
                                        gt_file.write('\n')

                                    all_attributes += items[link]
                                    id += 1
                                else:
                                    fault_status_codes += 1
                            except:
                                fault_status_codes += 1
                    
                    with open(dataset_folder+"groundtruth/"+category+"/"+category+"-"+website_name+"-"+schema_value_ab+".txt", "r") as edit_gt_file:
                        new_content = edit_gt_file.readlines()
                    
                    unique_attributes = list(dict.fromkeys(all_attributes))
                    new_content.insert(1, str(id) + '\t' + str(id) + '\t' + str(len(all_attributes)) + '\t' + str(len(unique_attributes)) + '\n')
                    
                    with open(dataset_folder+"groundtruth/"+category+"/"+category+"-"+website_name+"-"+schema_value_ab+".txt", "w") as edit_gt_file:
                        new_content = "".join(new_content)
                        edit_gt_file.write(new_content)
                            
                    with open("responses.txt", mode="a") as file:
                        file.write("Good status codes: " + str(good_status_codes) + '\n')
                        file.write("fault status codes: " + str(fault_status_codes) + '\n')
                    
                    folnr += 1




if __name__ == '__main__':
    parser = ArgumentParser(description='Download html files and groundtruths from CSV database.')

    parser.add_argument('-f', '--input-file', type=str, required=True, help='name of the input file in the Data folder')
    parser.add_argument('-sv', '--schema-value', type=str, required=True, help='selected schema value')
    parser.add_argument('-fc', '--file-count', type=int, required=True, help='number of files crawled per folder')

    args = parser.parse_args()
    main(args.input_file, args.schema_value, args.file_count)