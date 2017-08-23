import sys
import os
import urllib
import gzip
import urllib.request
from urllib.request import Request
import re

import pymysql
from pymysql import err


TIMEOUT = 600
#  Link for each casino
URL = {1: 'https://stat.casino-x.com/sa/serious_stats/csv/', 2: 'https://stat.joycasino.com/sa/serious_stats/csv/'}
FEEDS = {'regs': 'regs.csv', 'trans': 'money.csv', 'mailing': 'emails.csv',
         'games': 'games.csv', 'bonus': 'presents.csv'}  # different kind of data to grab


USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/49.0.2623.87 Safari/537.36'


def pack_cookie(cookies):
    return '; '.join(['%s=%s' % (k, v) for k, v in cookies.items()])


def do_get(url, cookies=None):
    print('GET', url, cookies)
    headers = {'User-Agent': USER_AGENT, 'Cookie': pack_cookie(cookies)}
    request = Request(url, headers=headers)
    return urllib.request.urlopen(request, timeout=TIMEOUT)


def process_line(psource, data, feed):
    def process_regs(val):
        val = val.decode('utf-8').split(',')
        val = str([psource] + val[0:2] + val[3:5])
        val = re.sub("[\.]*\+[0-9]+", "", val)
        return val

    def process_mailing(val):
        val = val.decode('utf-8').split(',')
        val = str(tuple([psource] + [val[3]] + val[0:2] + [val[7]] + val[4:7] + val[9:]))
        val = re.sub("\+[0-9]+:[0-9]+", "", val)
        return val

    def process_trans(val):
        val = val.decode('utf-8').split(',')
        val = str(tuple([psource] + val[2:4] + [val[0]] + [val[4]] + [val[1]] + [val[5]]))
        val = re.sub("\+[0-9]+:[0-9]+", "", val)
        val = str.replace(val, "deposit\\n", 'payin')
        val = str.replace(val, "withdraw\\n", 'payout')
        val = str.replace(val, "cancel\\n", 'cancel')
        val = str.replace(val, '\\n', '')
        return val

    def process_games(val):
        pass

    def process_bonus(val):
        pass

    funcs = {'regs': process_regs, 'trans': process_trans, 'mailing': process_mailing,
             'games': process_games, 'bonus': process_bonus}

    return funcs.get(feed)(data)


def get_mysql_pass():
    return 'FNZloy1989'


def insert_mailings(data, psource, feed):
        print('---------------------------------------------')
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
        mysql_cur = mysql_conn.cursor()
        n = 0  # no of rows inserted
        for i in data:
            try:
                values = process_line(psource, i, feed)
                values = str.replace(values, "None", 'NULL')
                values = str.replace(values, "''", 'NULL')
                mysql_sql = "insert into mailings_test(psource, mail_id, date, uid, open_count, first_open_date, " \
                            "last_open_date, hit_date, locale, hit_cnt, unsub_cnt) values " + values + '''
                on duplicate key update
                open_count = open_count + COALESCE(VALUES(open_count), 0),
                date = COALESCE( least(VALUES(date), date), date),
                first_open_date = COALESCE( least(VALUES(first_open_date), first_open_date), first_open_date),
                last_open_date = COALESCE( greatest(VALUES(last_open_date), last_open_date), last_open_date),
                hit_date = COALESCE( least(VALUES(hit_date), hit_date), hit_date),
                hit_cnt = COALESCE(hit_cnt, 0) + COALESCE(VALUES(hit_cnt), 0),
                unsub_cnt = COALESCE(unsub_cnt, 1) + COALESCE(VALUES(unsub_cnt), 0)
                '''

                if n % 1000 == 0:
                    print(n)
                    mysql_conn.commit()
                    #  print(mysql_sql)
                mysql_cur.execute(mysql_sql)
                n += 1
            except err.IntegrityError:
                print("inserting duplicate was omitted")
            except err.InternalError:
                print("error in row: " + mysql_sql)
        mysql_conn.commit()

        print(str(n) + " rows were inserted into table MAILING.MAILINGS")


def main(psource):
    cookies = {1: {'sid': '1d1ef79bc2d97d4ad5c5cac072972dc1'},  # session id, for connection to be valid (Chrome)
               2: {'sid': '80ea36a5f1f6d0033429534a26902ca2'}}  # 1: Casino X; 2: JoyCasino

#    cookies = {1: {'sid': 'd0bc2a581cb6aea015bae91ad2f198c9'},
#               2: {'sid': '5363182e38f32d881a4066c0c79d094d'}}  # session id, for connection to be valid
    fp = "temp.gz"  # to create temp .gz file (for storing data)

    feed_type = 'mailing'  # regs, trans, games, bonus, mailing
    feed = FEEDS.get(feed_type)
    start_date = '2017-08-17'
    end_date = '2017-08-23'

    print('Trying to load', feed)
    url = '%s%s?start_date=%s&end_date=%s' % (URL.get(psource), feed, start_date, end_date)
    response = do_get(url, cookies=cookies.get(psource))

    with open(fp, "wb") as code:
        code.write(response.read())

        with gzip.open(fp, 'rb') as f:
            print(f.readline().decode('utf-8').split(','))  # to skip headers (1st line)
            insert_mailings(f.readlines(), psource, feed_type)

    os.remove(fp)  # remove temp .gz archive

if __name__ == '__main__':
    main(1)  # change source here (1= casino-x)
    main(2)  # change source here (2 = joycasino)
