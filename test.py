import csv


with open('resources/mysql_data_test.csv', encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        metaid = row['metaid']
        label = row['label']
        year = row['year']
        dic = {'metaid': metaid, 'label': label, 'year': year}
        print(dic)
