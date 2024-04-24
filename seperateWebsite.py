from pathlib import Path
from argparse import ArgumentParser
import csv
import gzip
import shutil
from urllib.parse import urlparse

def main(input_artifact: str, url: str, schema_type: str, schema_value: str):
    DATA_DIR = Path('/vol/csedu-nobackup/other/smeijboom/data_separation/Data').expanduser()
    #DATA_DIR = Path('C:/Users/svenm/Documents/Radboud/BachelorThesis/CSVtoTXT/data_separation/Data').expanduser()

    input_dir = DATA_DIR / input_artifact

    print(input_dir)

    with gzip.open(input_dir, 'rb') as f_in:
        with open('file.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

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

        
            write_out = open("outfile.txt", "w")
            write_out.write("Vertical" + '\t' + url + '\t' + schema_type + "-" + schema_value + '\n')
            write_out.write(str(len(used_urls)) + '\t' + str(len(used_urls)) + '\t' + str(total_attribute_values) + '\t' + str(len(unique_attribute_values)) + '\n')
            for item in items:
                write_out.write(item + '\t' + str(len(items[item])))
                for it in items[item]:
                    write_out.write('\t' + it.strip('"\''))
                write_out.write('\n')

        


if __name__ == '__main__':
    parser = ArgumentParser(description='Seperate training files by website')

    parser.add_argument('-f', '--input-file', type=str, required=True, help='name of the input file in the Data folder')
    parser.add_argument('-u', '--url', type=str, required=True, help='url of website')
    parser.add_argument('-st', '--schema-type', type=str, required=True, help='selected schema type')
    parser.add_argument('-sv', '--schema-value', type=str, required=True, help='selected schema value')

    args = parser.parse_args()
    main(args.input_file, args.url, args.schema_type, args.schema_value)