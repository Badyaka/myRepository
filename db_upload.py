from pymysql import *
from pymysql import err
import csv
import re


def get_pass():
    return 'FNZloy1989'


def upload_transactions(source):
    # Connect to the database 'mailing'
    conn = connect(host='localhost', user='root', password=get_pass(), db='mailing', charset='utf8mb4',
                   cursorclass=cursors.DictCursor)
    c = conn.cursor()
    #c.execute("use mailing; alter table transactions disable keys;")
    # transactions
    n = 0

    with open(source) as fp:
        r = csv.reader(fp, delimiter=",")
        try:
            for row in r:
                if row[0].startswith("IntId"):
                    continue
                else:
                    #print(row)
                    #while n < 10:
                        sql = "use mailing; insert into transactions values "
                        sql += str(tuple(row[0].split('\t')))
                        sql = sql.replace("''", "NULL")
                        sql = sql.replace('None', "-1")
                        print(sql)
                        c.execute(sql)
                        n += 1
                        print(n)
                        if n % 10000 == 0:
                            print(n)
                            conn.commit()
        finally:
            conn.commit()
            print(n, " rows inserted")
            conn.close()


def upload_games(source):
    # Connect to the database 'mailing'
    conn = connect(host='localhost', user='root', password=get_pass(), db='mailing', charset='utf8mb4',
                   cursorclass=cursors.DictCursor)
    c = conn.cursor()
    n = 0  # transactions

    with open(source) as fp:
        r = csv.reader(fp, delimiter=",")
        try:
            for row in r:
                if row[0].startswith("ProjectName"):
                    continue
                else:
                    sql = "insert into games values "
                    v = row[0].split(';')[1:]
                    v[0] = str.replace(v[0], '"1020"', '1')
                    v[0] = str.replace(v[0], '"2003"', '2')
                    s = str.replace(str(tuple(v)), '"', "")
                    s =s.replace("''", "NULL")
                    sql += s
                    c.execute(sql)
                    n += 1
                    if n % 10000 == 0:
                        print(n)
                        conn.commit()
        finally:
            conn.commit()
            print(n, " rows inserted")
            conn.close()


def upload_mailings(psource, source):
    # Connect to the database 'mailing'
    conn = connect(host='localhost', user='root', password=get_pass(), db='mailing', charset='utf8mb4',
                   cursorclass=cursors.DictCursor)
    c = conn.cursor()
    n = 0  # transactions

    with open(source) as fp:
        r = csv.reader(fp, delimiter=",")
        try:
            for row in r:
                if row[0] == 'mail_id':
                    continue
                else:
                    try:
                        sql = "insert into mailings(psource, mail_id, date, uid, open_count, first_open_date, " \
                              "last_open_date, hit_date) values "
                        val = str(tuple([psource]+[row[1]] + row[3:]))
                        val = re.sub("[\.]*[0-9]+\+[0-9]+", "", val)
                        val = val.replace("''", "NULL")
                        sql += val
                        c.execute(sql)
                        n += 1
                        if n % 10000 == 0:
                            print(n)
                            conn.commit()
                    except err.IntegrityError:
                        print("duplicate caught, processing further...")
        finally:
            conn.commit()
            print(n, " rows inserted")
            conn.close()


#init = 'G:\MAILING\\users_transactions\\transactions.csv'
#init = 'G:\\SQL DATA\\trans_jx.csv'
init = 'G:\\SQL DATA\\mailing_stats\\email_stats_20160319_casinox.csv'
#upload_transactions(init)
upload_mailings(1, init)
