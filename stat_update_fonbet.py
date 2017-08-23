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
URL = {1: 'https://stat.casino-x.com/sa/serious_stats/csv/', 2: 'https://stat.joycasino.com/sa/serious_stats/csv/', 3:'https://fonbet.pomadorro.com/sa/serious_stats/csv/'}
FEEDS = {'regs': 'regs.csv', 'trans': 'money.csv', 'mailing': 'emails.csv',
         'games': 'games.csv', 'bonus': 'presents.csv'}  # different kind of data to grab


USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/49.0.2623.87 Safari/537.36'


def date_from():
    return '2017-08-17'

def date_to():
    return '2017-08-23'

def get_mysql_pass():
    return 'FNZloy1989'


def replace_project_id(casino_id):
   # ids = {1020: 1, 2003: 2,127:3}
    #return ids.get(casino_id,-1)
    return casino_id

def convert_datetime(dt):
    if dt is None:
        return None
    else:
        return dt.strftime('%Y-%m-%d %H:%M:%S')

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
        #val = str([psource] + val[0:2] + val[3:5])
        val = str(tuple([psource] + [val[0]] + [val[3]] + [val[4]] + [val[1]]))
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
        val = val.decode('utf-8').split(',')
        if val[3] == 'bet':
            val[4] = re.sub("-", "", val[4])
            val = str(tuple([psource] + [val[4]] + [val[5]] + [val[6]] + [val[0]] + [val[2]] + [val[1]] + [0] + [0]))
        else:
            val = str(tuple([psource] + [0] + [0] + [val[6]] + [val[0]] + [val[2]] + [val[1]] + [val[4]] + [val[5]]))

        val = re.sub("\+[0-9]+:[0-9]+", "", val)
        val = str.replace(val, '\\n', '')

        return val

    def process_bonus(val):
        val = val.decode('utf-8').split(',')
        val[2] = str.replace(val[2], '+00:00', '')
        val[4] = str.replace(val[4], '+00:00', '')
        val = str(
            tuple([psource] + [val[6]] + [val[5]] + [val[3]] + [val[0]] + [val[2]] + [val[4]] + [val[7]] + [val[1]]))
        val = re.sub("[\.]*\+[0-9]+", "", val)
        val = str.replace(val, '\\n', '')
        return val

    funcs = {'regs': process_regs, 'trans': process_trans, 'mailing': process_mailing,
             'games': process_games, 'bonus': process_bonus}

    return funcs.get(feed)(data)

def insert_users(data, psource, feed):
    print('---------------------------------------------')
    # Connect to the database 'mailing' in local MySQL DB
    mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    mysql_cur = mysql_conn.cursor()
    n = 0  # no of rows inserted
    for i in data:
        try:
            # try to solve problem with IPv6
            dummy_list = i.decode('utf-8').split(',')
            if len(dummy_list[4]) > 15:
                dummy_list[4] = dummy_list[4][0:10] + "IPv6"
                i = ",".join(dummy_list).encode('utf8')

            values = process_line(psource, i, feed)
            values = str.replace(values, "None", 'NULL')
            values = str.replace(values, "''", 'NULL')
            mysql_sql = "insert into users_fonbet(psource,uid,currency,reg_ip,regdate) values %s" % values
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
    print(str(n) + " rows were inserted into table MAILING.USERS_fonbet")


def update_users(psource):
    cookies = {1: {'sid': '1d1ef79bc2d97d4ad5c5cac072972dc1'},  # session id, for connection to be valid (Chrome)
               2: {'sid': '80ea36a5f1f6d0033429534a26902ca2'},
    #           3: {'sid': '7257108abded356b78b23eb7a8e09b67'}}  # 1: Casino X; 2: JoyCasino; 3: Fonbet
               3: {'sid': 'ce674a0a1965fdd38000889c4319cb23'}}  # 1: Casino X; 2: JoyCasino; 3: Fonbet

#    cookies = {1: {'sid': 'd0bc2a581cb6aea015bae91ad2f198c9'},
#               2: {'sid': '5363182e38f32d881a4066c0c79d094d'},
#               3: {'sid': '2d3917b441b4e1b19ec46871945673e8'}}  # session id, for connection to be valid
    fp = "temp.gz"  # to create temp .gz file (for storing data)

    feed_type = 'regs'  # regs, trans, games, bonus, mailing
    feed = FEEDS.get(feed_type)
    start_date = date_from()
    end_date = date_to()

    print('Trying to load', feed)
    url = '%s%s?start_date=%s&end_date=%s' % (URL.get(psource), feed, start_date, end_date)
    response = do_get(url, cookies=cookies.get(psource))

    with open(fp, "wb") as code:
        code.write(response.read())

        with gzip.open(fp, 'rb') as f:
            print(f.readline().decode('utf-8').split(','))  # to skip headers (1st line)
            insert_users(f.readlines(), psource, feed_type)

    os.remove(fp)  # remove temp .gz archive

def insert_trans(data, psource, feed):
        print('---------------------------------------------')
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        mysql_cur = mysql_conn.cursor()
        n = 0  # no of rows inserted
        for i in data:
            try:
                values = process_line(psource, i, feed)
                values = str.replace(values, "None", 'NULL')
                values = str.replace(values, "''", 'NULL')
                mysql_sql = "insert into transactions_fonbet(psource, amount, currency, stdate, " \
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
        print(str(n) + " rows were inserted into table MAILING.transactions_fonbet")

        def update_trans_table():  # calculate amt_usd column via join with currency_rates
            # Connect to the database 'mailing' in local MySQL DB
            mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor, autocommit=True)
            mysql_cur = mysql_conn.cursor()
            sql = '''
            update transactions_fonbet t inner join currency_rates c
            on date(t.stdate) = c.cdate and t.currency = c.rub_to
            set t.amt_usd = t.amount * c.usd_rate
            where amt_usd is null;
            '''
            mysql_cur.execute(sql)
            print("transactions (in USD) were updated")

        update_trans_table()


def update_trans(psource):
    cookies = {1: {'sid': '1d1ef79bc2d97d4ad5c5cac072972dc1'},  # session id, for connection to be valid (Chrome)
               2: {'sid': '80ea36a5f1f6d0033429534a26902ca2'},
               3: {'sid': 'ce674a0a1965fdd38000889c4319cb23'}}  # 1: Casino X; 2: JoyCasino; 3: Fonbet

#    cookies = {1: {'sid': 'd0bc2a581cb6aea015bae91ad2f198c9'},
#               2: {'sid': '5363182e38f32d881a4066c0c79d094d'},
#               3: {'sid': '2d3917b441b4e1b19ec46871945673e8'}}  # session id, for connection to be valid
    fp = "temp.gz"  # to create temp .gz file (for storing data)

    feed_type = 'trans'  # regs, trans, games, bonus, mailing
    feed = FEEDS.get(feed_type)
    start_date = date_from()
    end_date = date_to()

    print('Trying to load', feed)
    url = '%s%s?start_date=%s&end_date=%s' % (URL.get(psource), feed, start_date, end_date)
    response = do_get(url, cookies=cookies.get(psource))

    with open(fp, "wb") as code:
        code.write(response.read())

        with gzip.open(fp, 'rb') as f:
            print(f.readline().decode('utf-8').split(','))  # to skip headers (1st line)
            insert_trans(f.readlines(), psource, feed_type)

    os.remove(fp)  # remove temp .gz archive

def insert_mailings(data, psource, feed):
        print('---------------------------------------------')
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                     charset='utf8mb4',
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

                if n % 500 == 0:
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

        print(str(n) + " rows were inserted into table MAILING.mailings_test")


def update_mailings(psource):
    cookies = {1: {'sid': '1d1ef79bc2d97d4ad5c5cac072972dc1'},  # session id, for connection to be valid (Chrome)
               2: {'sid': '80ea36a5f1f6d0033429534a26902ca2'},
               3: {'sid': 'ce674a0a1965fdd38000889c4319cb23'}}  # 1: Casino X; 2: JoyCasino; 3: Fonbet

#    cookies = {1: {'sid': 'd0bc2a581cb6aea015bae91ad2f198c9'},
#               2: {'sid': '5363182e38f32d881a4066c0c79d094d'},
#               3: {'sid': '2d3917b441b4e1b19ec46871945673e8'}}  # session id, for connection to be valid
    fp = "temp.gz"  # to create temp .gz file (for storing data)

    feed_type = 'mailing'  # regs, trans, games, bonus, mailing
    feed = FEEDS.get(feed_type)
    start_date = date_from()
    end_date = date_to()

    print('Trying to load', feed)
    url = '%s%s?start_date=%s&end_date=%s' % (URL.get(psource), feed, start_date, end_date)
    response = do_get(url, cookies=cookies.get(psource))

    with open(fp, "wb") as code:
        code.write(response.read())

        with gzip.open(fp, 'rb') as f:
            print(f.readline().decode('utf-8').split(','))  # to skip headers (1st line)
            insert_mailings(f.readlines(), psource, feed_type)

    os.remove(fp)  # remove temp .gz archive

def insert_games(data, psource, feed):
    print('---------------------------------------------')
    # Connect to the database 'mailing' in local MySQL DB
    mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    mysql_cur = mysql_conn.cursor()
    n = 0  # no of rows inserted
    for i in data:
        try:
            values = process_line(psource, i, feed)
            values = str.replace(values, "None", 'NULL')
            values = str.replace(values, "''", 'NULL')
            mysql_sql = "insert into games_fonbet_temp(psource,bets,bets_cnt,currency,date,gameref,uid,wins,wins_cnt) values %s" % values
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
    print(str(n) + " rows were inserted into table MAILING.games_fonbet_temp")

    def update_games_table():  # calculate amt_usd column via join with currency_rates
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor, autocommit=True)
        mysql_cur = mysql_conn.cursor()
        sql = '''
        insert into games_fonbet
        select psource,sum(bets) as bets,sum(bets_cnt) as bets_cnt,currency,date,gameref,uid,sum(wins) as wins,sum(wins_cnt) as wins_cnt
        from games_fonbet_temp
        group by 1,4,5,6,7
        '''
        mysql_cur.execute(sql)
        print("inserted into table MAILING.games_fonbet")

    update_games_table()

    def trank_games_table():  # calculate amt_usd column via join with currency_rates
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor, autocommit=True)
        mysql_cur = mysql_conn.cursor()
        sql = '''
        truncate table games_fonbet_temp
        '''
        mysql_cur.execute(sql)
        print("truncate table MAILING.games_fonbet_temp")

    trank_games_table()


def update_games(psource):
    cookies = {1: {'sid': '1d1ef79bc2d97d4ad5c5cac072972dc1'},  # session id, for connection to be valid (Chrome)
               2: {'sid': '80ea36a5f1f6d0033429534a26902ca2'},
               3: {'sid': 'ce674a0a1965fdd38000889c4319cb23'}}  # 1: Casino X; 2: JoyCasino; 3: Fonbet

#    cookies = {1: {'sid': 'd0bc2a581cb6aea015bae91ad2f198c9'},
#               2: {'sid': '5363182e38f32d881a4066c0c79d094d'},
#               3: {'sid': '2d3917b441b4e1b19ec46871945673e8'}}  # session id, for connection to be valid
    fp = "temp.gz"  # to create temp .gz file (for storing data)

    feed_type = 'games'  # regs, trans, games, bonus, mailing
    feed = FEEDS.get(feed_type)
    start_date = date_from()
    end_date = date_to()

    print('Trying to load', feed)
    url = '%s%s?start_date=%s&end_date=%s' % (URL.get(psource), feed, start_date, end_date)
    response = do_get(url, cookies=cookies.get(psource))

    with open(fp, "wb") as code:
        code.write(response.read())

        with gzip.open(fp, 'rb') as f:
            print(f.readline().decode('utf-8').split(','))  # to skip headers (1st line)
            insert_games(f.readlines(), psource, feed_type)

    os.remove(fp)  # remove temp .gz archive

def insert_bonuses(data, psource, feed):
    print('---------------------------------------------')
    # Connect to the database 'mailing' in local MySQL DB
    mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    mysql_cur = mysql_conn.cursor()
    n = 0  # no of rows inserted
    for i in data:
        try:
            values = process_line(psource, i, feed)
            values = str.replace(values, "None", 'NULL')
            values = str.replace(values, "''", 'NULL')
            mysql_sql = "insert into bonuses_fonbet(psource, amount, currency,event_id,present_id, date,accepted, type, uid) values %s" % values
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
    print(str(n) + " rows were inserted into table MAILING.bonuses_fonbet")


def update_bonuses(psource):
    cookies = {1: {'sid': '1d1ef79bc2d97d4ad5c5cac072972dc1'},  # session id, for connection to be valid (Chrome)
               2: {'sid': '80ea36a5f1f6d0033429534a26902ca2'},
               3: {'sid': 'ce674a0a1965fdd38000889c4319cb23'}}  # 1: Casino X; 2: JoyCasino; 3: Fonbet

#    cookies = {1: {'sid': 'd0bc2a581cb6aea015bae91ad2f198c9'},
#               2: {'sid': '5363182e38f32d881a4066c0c79d094d'},
#               3: {'sid': '2d3917b441b4e1b19ec46871945673e8'}}  # session id, for connection to be valid
    fp = "temp.gz"  # to create temp .gz file (for storing data)

    feed_type = 'bonus'  # regs, trans, games, bonus, mailing
    feed = FEEDS.get(feed_type)
    start_date = date_from()
    end_date = date_to()

    print('Trying to load', feed)
    url = '%s%s?start_date=%s&end_date=%s' % (URL.get(psource), feed, start_date, end_date)
    response = do_get(url, cookies=cookies.get(psource))

    with open(fp, "wb") as code:
        code.write(response.read())

        with gzip.open(fp, 'rb') as f:
            print(f.readline().decode('utf-8').split(','))  # to skip headers (1st line)
            insert_bonuses(f.readlines(), psource, feed_type)

    os.remove(fp)  # remove temp .gz archive


if __name__ == '__main__':
    # change source here (3 = fonbet)
    #update_users(1)
    #update_trans(1)
    #update_mailings(1)
    #update_games(1)
    #update_bonuses(1)

    #update_users(2)
    #update_trans(2)
    #update_mailings(2)
    #update_games(2)
    #update_bonuses(2)

    update_users(3)
    update_trans(3)
    update_mailings(3)
    update_games(3)
    update_bonuses(3)