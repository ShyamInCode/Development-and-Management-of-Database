import csv
import re

file = open("source.txt")
filename = 'data.csv'

num = file.readline()
num = num[:-1]
temp = file.read()
papers = re.split("\n\n", temp)
papers = papers[:-1]

def csv_convertible(arr):
    paper = [''] * 7
    sym = ['*','@','t','c','i','%','!']
    for i in arr:
        k = sym.index(i[0])
        if(k == 4):
            paper[k] = i[5:]
        elif(k == 5):
            if(paper[k] == ''):
                paper[k] = i[1:]
            else:
                paper[k] += (',' + i[1:])
        else:
            paper[k] = i[1:]
    return paper

fields = ['Title', 'Authors', 'Year', 'Venue', 'ID', 'RefIDs', 'Abstract']
csvfile = open(filename,'w')
csvwriter = csv.writer(csvfile)
csvwriter.writerow(fields)

for temp in papers:
    temp = '\n' + temp
    paper = re.split("\n#",temp)
    csvwriter.writerow(csv_convertible(paper[1:]))

print("\nParsing completed\n")
