import csv

#with open("C:/Users/svenm/Documents/Radboud/BachelorThesis/CSVtoTXT/data_separation/list_sv_per_url.txt") as f:
#    reader = csv.reader(f, delimiter="\t")
#    row = next(reader)
#    print(row)

s = "http://schema.org/telephone"

print(s.rsplit('/', 1)[1])