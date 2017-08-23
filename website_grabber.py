import sys
import os
import urllib
import gzip
import urllib.request
from urllib.request import Request
import re

import pymysql
from pymysql import err


TIMEOUT = 60
#  Link for each casino
URL = {1: 'https://casino-x.com/sa/serious_stats/csv/', 2: 'https://joycasino.com/sa/serious_stats/csv/'}
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
        val = str([psource] + val[0:2] + val[3:5])
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


def insert_trans(data, psource, feed):
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
                mysql_sql = "insert into transactions_miliseconds(psource, amount, currency, stdate, " \
                            "payment_group_id, uid, type) values %s" % values
                if n % 500 == 0:
                    print(n)
                    mysql_conn.commit()
                mysql_cur.execute(mysql_sql)
                n += 1
            except err.IntegrityError:
                print("inserting duplicate was omitted")
            except err.InternalError:
                print("error in row: " + mysql_sql)
        mysql_conn.commit()
        print(str(n) + " rows were inserted into table MAILING.TRANSACTIONS")

        def update_trans_table():  # calculate amt_usd column via join with currency_rates
            # Connect to the database 'mailing' in local MySQL DB
            mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor, autocommit=True)
            mysql_cur = mysql_conn.cursor()
            sql = '''
            update transactions_miliseconds t inner join currency_rates c
            on date(t.stdate) = c.cdate and t.currency = c.rub_to
            set t.amt_usd = t.amount * c.usd_rate
            where amt_usd is null;
            '''
            mysql_cur.execute(sql)
            print("transactions (in USD) were updated")
        update_trans_table()


def main(psource):
    cookies = {1: {'sid': '867db74aa3b300920f21da218c115ae4'},
               2: {'sid': '18372f12715a58d0792a36f2b5e8d637'}}  # session id, for connection to be valid
    fp = "temp.gz"  # to create temp .gz file (for storing data)

    feed_type = 'trans'  # regs, trans, games, bonus, mailing
    feed = FEEDS.get(feed_type)
    start_date = '2016-04-28'
    end_date = '2016-04-29'

    print('Trying to load', feed)
    url = '%s%s?start_date=%s&end_date=%s' % (URL.get(psource), feed, start_date, end_date)
    response = do_get(url, cookies=cookies.get(psource))

    with open(fp, "wb") as code:
        code.write(response.read())

        with gzip.open(fp, 'rb') as f:
            print(f.readline().decode('utf-8').split(','))  # to skip headers (1st line)
            insert_trans(f.readlines(), psource, feed_type)

    os.remove(fp)  # remove temp .gz archive

if __name__ == '__main__':
    main(1)  # change source here (1= casino-x)
    main(2)  # change source here (2 = joycasino)
