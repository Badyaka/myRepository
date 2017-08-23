import csv


init = 'G:\MAILING\\users_transactions\\2003_1020_trans_2015_05_01.csv'
output = 'G:\MAILING\\users_transactions\\transactions.csv'
rows = 0
wr = open(output, 'w', newline='\n')
writer = csv.writer(wr, delimiter=';')
with open(init) as fp:
    r = csv.reader(fp, delimiter=";")
    for row in r:
        #if rows > 2:
        #    break
        temp = row[1:6] + row[7:9] + row[10:]
        if temp[0]=='2003':
            temp[0]='2'
        elif temp[0]=='1020':
            temp[0]='1'
        # print(row)
        #print(temp)
        writer.writerow(temp)
        rows += 1

wr.close()
fp.close()

print(rows)
