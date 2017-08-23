# альтернатива скрипту SQL_Server_updates_TP.py, который может не работать и вообще "сторонний"
# идея модификации родилась 18/07/2017- у аналитиков лег винт и базы не обновлялись два дня, должен быть инструмент, который обеспечить независимость от базы аналитиков :-_)

import sys
import os
import urllib
import gzip
import urllib.request
from urllib.request import Request
import re

import pypyodbc
import pymysql
from pymysql import err
from pymysql import *
import datetime



TIMEOUT = 600
#  Link for each casino
URL = {1: 'https://stat.casino-x.com/sa/serious_stats/csv/', 2: 'https://stat.joycasino.com/sa/serious_stats/csv/'}
FEEDS = {'regs': 'regs.csv', 'trans': 'money.csv', 'mailing': 'emails.csv',
         'games': 'games.csv', 'bonus': 'presents.csv'}  # different kind of data to grab


USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/49.0.2623.87 Safari/537.36'


# Time-range boundaries
def date_from():
    return '2017-08-22'

def date_to():
    return '2017-08-23'


def convert_datetime(dt):
    if dt is None:
        return None
    else:
        return dt.strftime('%Y-%m-%d %H:%M:%S')


def get_mysql_pass():
    return 'FNZloy1989'


def get_sqlserver_pass():
    return 'Loh3shoh'

def replace_project_id(casino_id):
    ids = {1 : '1020', 2 : '2003'}
    return ids.get(casino_id, -1)


def pack_cookie(cookies):
    return '; '.join(['%s=%s' % (k, v) for k, v in cookies.items()])

sql_server_string = "DRIVER={SQL Server}; SERVER=78.140.130.90; DATABASE=pomodorro; " \
               "UID=f.navruzov; PWD=" + get_sqlserver_pass() + "; Charset:UTF-8"


def do_get(url, cookies=None):
    print('GET', url, cookies)
    headers = {'User-Agent': USER_AGENT, 'Cookie': pack_cookie(cookies)}
    request = Request(url, headers=headers)
    return urllib.request.urlopen(request, timeout=TIMEOUT)


def update_currency_rates():  # update currency rates (table mailing.currency_rates, MySQL)

    def get_currency(date):
        # change (add/remove) currencies here
        currencies = ['USD', 'EUR', 'RUB', 'NOK', 'SEK', 'AUD', 'CAD', 'CNY', 'JPY']
        from bs4 import BeautifulSoup
        from urllib.request import Request, urlopen
        url = 'http://www.cbr.ru/currency_base/daily.aspx?date_req=' + date
        print (url)
        req = Request(url)
        wp = urlopen(req).read()
        ss = BeautifulSoup(wp, "html.parser")
        table = ss.findChildren('table')[0]
        rows = table.findChildren(['tr'])
        source = list()

        for row in rows:
            cells = row.findChildren('td')
            cur = list()
            cur.append(date)
            for c in cells:
                if c.string == '':
                    continue
                else:
                    cur.append(c.string)
                if len(cur) > 5:
                    if cur[2] in currencies:
                        source.append([cur[0], cur[2], float(cur[5].replace(',', '.'))/float(cur[3].replace(',', '.'))])
        return source

    def upload_currency(source):
        # Connect to the database 'mailing'
        conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                       cursorclass=cursors.DictCursor, autocommit=True)
        c = conn.cursor()

        # transactions
        n = 0
        try:
            for row in source:
                sql = "use mailing; insert into currency_rates(cdate, rub_to, rub) values "
                sql += str(tuple(row))
                print(sql)
                c.execute(sql)
                n += 1
                conn.commit()
        finally:
            print(n, " rows inserted")
            conn.close()

    def update_currency_table():
        # Connect to the database 'mailing'
        conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                       cursorclass=cursors.DictCursor)
        c = conn.cursor()

        try:
                sql = '''use mailing; delete from currency_rates where rub_to = 'RUB';
                        insert into currency_rates(cdate, rub_to, rub)
                        (select distinct cdate, 'RUB', 1
                        from currency_rates);
                        -- update usd_rate
                        update currency_rates
                        set usd_rate = 0;
                        update currency_rates c inner join
                            -- -----------------------
                            (select cdate, rub
                            from currency_rates
                             where rub_to = 'USD') usd
                            -- -----------------------
                        on c.cdate = usd.cdate
                        set c.usd_rate = c.rub/usd.rub;
                        -- now all currencies correctly mapped into USD (usd_rate column)

                '''
                c.execute(sql)
                conn.commit()
        finally:
            conn.commit()
            print("currency table successfully updated")
            conn.close()

    def get_last_reg_date():
        # Connect to the database 'mailing'
        conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                       cursorclass=cursors.DictCursor)
        c = conn.cursor()
        sql = "select max(cdate) md from currency_rates;"
        c.execute(sql)
        return c.fetchone().get('md')

    base = datetime.datetime.today().date()
    days_ago = int(str(base - get_last_reg_date()).split(" ")[0])
    print(days_ago)
    date_list = [str(base - datetime.timedelta(days=x))[0:10] for x in range(0, days_ago)]

    s = list()
    s.append(map(get_currency, date_list))

    r = list()
    for st in s:
        for e in st:
            r.extend(e)

    print("...data successfully grabbed from site")
    upload_currency(r)
    update_currency_table()

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
        val = str(
            tuple([psource] + [val[6]] + [val[5]] + [val[2]] + [val[7]] + [val[1]]))
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
            mysql_sql = "insert ignore into users(psource, uid, currency, reg_ip, regdate) values %s" % values

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
    print(str(n) + " rows were inserted into table MAILING.USERS")

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

            # try to solve problem with "cancel" type
            dummy_list = i.decode('utf-8').split(',')
            #print (dummy_list, len(dummy_list), dummy_list[5]=='cancel\n' )
            if dummy_list[5]=='cancel\n' :
                dummy_list[5] = 'payout'
                dummy_list[2] = '0'
                i = ",".join(dummy_list).encode('utf8')


            values = process_line(psource, i, feed)
            values = str.replace(values, "None", 'NULL')
            values = str.replace(values, "''", 'NULL')

            #print(values, values.split(',')[6],values.split(',')[6] != " 'refund')")
            #print()

            if values.split(',')[6] != " 'refund')":
                mysql_sql = "insert into transactions(psource, amount, currency, stdate, payment_group_id, " \
                            " uid, type) values " + values + " on duplicate key update amount = amount + values(amount)"
                if n % 500 == 0:
                    print(n)
                    mysql_conn.commit()
                mysql_cur.execute(mysql_sql)
                n += 1

        except err.IntegrityError:
            print("inserting duplicate was omitted")
#        except err.InternalError:
#            print("error in row: " + mysql_sql)
    mysql_conn.commit()

    print(str(n) + " rows were inserted into table MAILING.transactions")

    def update_trans_table():  # calculate amt_usd column via join with currency_rates
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor, autocommit=True)
        mysql_cur = mysql_conn.cursor()
        sql = '''
        update transactions t inner join currency_rates c
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
            #print(values.split(',')[4], len(values.split(',')[4]), values.split(',')[4] == " 'cash_bonus'")
            #print (values.split(',')[4], values.split(',')[4] in [" 'freespin'", " 'cash_bonus'", " 'cash_admin_bonus'", " 'cancel_bonus'"])
            if values.split(',')[4] in [" 'freespin'", " 'cash_bonus'", " 'cash_admin_bonus'", " 'cancel_bonus'"]:
                mysql_sql = "insert ignore into bonuses (psource, amount, currency, date, type, uid) values %s" % values

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
    print(str(n) + " rows were inserted into table MAILING.bonuses")

def update_bonuses(psource):
    cookies = {1: {'sid': '1d1ef79bc2d97d4ad5c5cac072972dc1'},  # session id, for connection to be valid (Chrome)
               2: {'sid': '80ea36a5f1f6d0033429534a26902ca2'},
               3: {'sid': 'ce674a0a1965fdd38000889c4319cb23'}}  # 1: Casino X; 2: JoyCasino; 3: Fonbet

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


def update_games(psource):  # update users games (table mailing.bonuses, MySQL)
    def get_last_game_date(psource):
        # Connect to the database 'mailing'
        conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                       cursorclass=cursors.DictCursor)
        c = conn.cursor()
        sql = "select max(date) md from games where psource = " +str(psource)+ ";"
        c.execute(sql)
        return convert_datetime(c.fetchone().get('md'))

    def get_game_updates(psource):
        # connect to MS SQL SERVER DB
        sqlserver_conn = pypyodbc.connect(sql_server_string)
        sqlserver_cur = sqlserver_conn.cursor()
        sqlserver_sql = "select IntId, balance_after, balance_before, bets, bets_cnt, currency, " \
                        "date, gameref, user_id, wins, wins_cnt " \
                        "from pomadorro_casino_games " \
                        "where IntId = "+ replace_project_id(psource) + " and date > Cast('" + get_last_game_date(psource) + "' as datetime)"

        #############"where IntId in (1020) and date >= '2017-07-24' AND date < '2017-07-28'"

        print(sqlserver_sql)
        result = sqlserver_cur.execute(sqlserver_sql)
        return result.fetchall()

    def insert_game(psource):
        print('---------------------------------------------')
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
        mysql_cur = mysql_conn.cursor()
        n = 0  # no of rows inserted

        for i in get_game_updates(psource):
            try:
                values = str(tuple([psource, i[1], i[2], i[3], i[4], i[5],
                                    convert_datetime(i[6]), i[7], i[8], i[9], i[10]]))
                values = str.replace(values, "None", 'NULL')
                mysql_sql = "insert ignore into games values " + values
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
        print(str(n) + " rows were inserted into table MAILING.GAMES")

    insert_game(psource)  # get updates (SQL Server) and insert into bonus table (MySQL)

try:
    update_currency_rates()
except ValueError:
    print("currencies already fetched")

#ready $ tested
#update_trans(1)# only for old table "transactions", use website_grabber.py instead !
#update_trans(2)

#ready & tested
update_users(1)
update_users(2)

#ready & tested
#update_bonuses(1)
#update_bonuses(2)

#ready & tested (but still with DATABASE=pomodorro)
update_games(1)
update_games(2)