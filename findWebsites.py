from pathlib import Path
from argparse import ArgumentParser
import csv
import gzip
import shutil
from urllib.parse import urlparse

def main(input_artifact: str, schema_value: str):
    #DATA_DIR = Path('/vol/csedu-nobackup/other/smeijboom/data_separation/Data').expanduser()
    DATA_DIR = Path('C:/Users/svenm/Documents/Radboud/BachelorThesis/CSVtoTXT/data_separation/Data').expanduser()

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

            for row in reader:
                if row[i] != "":
                    up = urlparse(row[0]).hostname
                    schema_type = row[2]
                    if up not in used_urls:
                        used_urls.add(up)
                        items[up] = [schema_type]
                    else:
                        items[up] += [schema_type]

        
            write_out = open("list_sv_per_url.txt", "w")
            #write_out.write("Vertical" + '\t' + url + '\t' + schema_type + "-" + schema_value + '\n')
            #write_out.write(str(len(used_urls)) + '\t' + str(len(used_urls)) + '\t' + str(total_attribute_values) + '\t' + str(len(unique_attribute_values)) + '\n')
            for used_url in used_urls:
                write_out.write(str(used_url) + '\t' + str(len(items[used_url])) + '\t' + "[")
                first = True
                items[used_url] = list(dict.fromkeys(items[used_url]))
                for it in items[used_url]:
                    if first:
                        write_out.write(it)
                        first = False
                    else:
                        write_out.write("," + it)
                write_out.write("]")
                write_out.write('\n')

        


if __name__ == '__main__':
    parser = ArgumentParser(description='Seperate training files by website')

    parser.add_argument('-f', '--input-file', type=str, required=True, help='name of the input file in the Data folder')
    parser.add_argument('-sv', '--schema-value', type=str, required=True, help='selected schema value')

    args = parser.parse_args()
    main(args.input_file, args.schema_value)