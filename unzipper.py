import gzip
from os import listdir
import csv
from datetime import datetime


#indir = "C:\\Users\\user\\Downloads\\csv_casinox\\FS"
#outdir = "C:\\Users\\user\\Downloads\\csv_casinox\\FS\\allFS_" + str(datetime.utcnow())[0:10] + ".csv"

#indir = "C:\\Users\\user\\Downloads\\csv_casinox\\CB"
#outdir = "C:\\Users\\user\\Downloads\\csv_casinox\\CB\\allCB_" + str(datetime.utcnow())[0:10] + ".csv"

indir = "C:\\Users\\user\\Downloads\\csv_fonbet"
outdir = "C:\\Users\\user\\Downloads\\csv_fonbet\\allFS_" + str(datetime.utcnow())[0:10] + ".csv"

#indir = "C:\\Users\\user\\Downloads\\csv_joycasino\\CB"
#outdir = "C:\\Users\\user\\Downloads\\csv_joycasino\\CB\\allCB_" + str(datetime.utcnow())[0:10] + ".csv"

#indir = "C:\\Users\\user\\Downloads\\csv_joycasino\\FS"
#outdir = "C:\\Users\\user\\Downloads\\csv_joycasino\\FS\\allFS_" + str(datetime.utcnow())[0:10] + ".csv"

count = 0

L=[]
lines = []
header = "mail_name;userid;lang;currency;bet_sum;bet_cnt;bet_cpoints;win_sum;win_cnt;deposit_sum;deposit_cnt;fd_sum;rebill_sum;" \
         "cash_out_sum;cash_out_cnt;balance_sum;bonus_balance_sum".split(";")

with open(outdir, 'w', newline='\n') as fp:
    w = csv.writer(fp, dialect='excel', delimiter=";")
    for file in listdir(indir):
        if file.endswith(".gz"):
        #if file.endswith(".csv"):
            try:
                lcount = 0
                name = file[0:len(file)-3]  # name
                #name = file[0:len(file) - 4]  # name
                count += 1   # no of files processed
                fname = indir + "\\"+file  # file path
                for s in gzip.open(fname, 'rb'):
                # for s in open(fname, 'rb'):
                    if s.startswith(bytes("userid", 'utf-8')):
                        pass
                    else:
                        line = s.decode('utf-8').split(",")
                        #  line = list(map(lambda st: st.replace(".", ","), line))
                        line.insert(0, name)
                        lines.append(line)
                        lcount += 1

                L.append(lcount)
                print(fname, " lines: ", lcount)

            except FileNotFoundError:
                print("oops! no such file on a path")
    lines.insert(0, header)
    w.writerows(lines)
print(count)
for l in L:
    print(l)
print(sum(L))
fp.close()