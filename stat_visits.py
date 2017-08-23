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
import csv
import datetime as dt
from dateutil.relativedelta import relativedelta


TIMEOUT = 6000
#  Link for each casino
URL = {1: 'https://stat.casino-x.com/sa/serious_stats/users_ng_csv/', 2: 'https://stat.joycasino.com/sa/serious_stats/users_ng_csv/', 3:'https://fonbet.pomadorro.com/sa/serious_stats/users_ng_csv/'}



USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/49.0.2623.87 Safari/537.36'


def get_mysql_pass():
    return 'FNZloy1989'

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


def convert_datetime(dt):
    if dt is None:
        return None
    else:
        return dt.strftime('%Y-%m-%d')


def get_event(psource):
    # Connect to the database 'mailing'
    conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                   cursorclass=cursors.DictCursor)
    c = conn.cursor()
    sql = "select date(max(u.date)) md from users_visits u where u.psource = "+str(psource)+""
    c.execute(sql)
    return c.fetchone().get('md')


def insert_users(psource):
    print('---------------------------------------------')
    # Connect to the database 'mailing' in local MySQL DB
    mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    mysql_cur = mysql_conn.cursor()
    n = 0  # no of rows inserted


##special ed. to contact-centre research: fill users_visits table since 01.06.2016 till 30.09.2016
#    cur_date = dt.date(2016, 6, 1)
#    end_period_date = dt.date(2016, 10, 1)
#    print (cur_date, end_period_date)
#    while cur_date < end_period_date:

#special ed. to contact-centre research: fill users_visits table since 01.01.2013 till 31.05.2016
#    cur_date = dt.date(2013, 1, 1)
#    end_period_date = dt.date(2016, 6, 1)
#    print (cur_date, end_period_date)

#    while cur_date < end_period_date:

    # special ed. to FB бездеп: fill users_visits table since 09.08.2016 till 30.09.2016
    #cur_date = dt.date(2016, 8, 9)
    #end_period_date = dt.date(2016, 10, 1)
    #print (cur_date, end_period_date)

    #while cur_date < end_period_date:



    while get_event(psource) < dt.date.today() - relativedelta(days=1):


    #   start_date = cur_date
        start_date = get_event(psource) + relativedelta(days=1)

        end_date = start_date + relativedelta(days=1)
     #   print(start_date, end_date)

        cookies = {1: {'sid': '1d1ef79bc2d97d4ad5c5cac072972dc1'},  # session id, for connection to be valid (Chrome)
                   2: {'sid': '80ea36a5f1f6d0033429534a26902ca2'},
                   3: {'sid': 'ce674a0a1965fdd38000889c4319cb23'}}  # 1: Casino X; 2: JoyCasino; 3: Fonbet

    #        cookies = {1: {'sid': 'd0bc2a581cb6aea015bae91ad2f198c9'},
#                   2: {'sid': '5363182e38f32d881a4066c0c79d094d'},
#                   3: {'sid': '2d3917b441b4e1b19ec46871945673e8'}}  # session id, for connection to be valid

        fp = "temp.csv"  # to create temp .gz file (for storing data)

        print('Trying to load')
        url = '%s?interval=day&start_date=%s&end_date=%s' % (URL.get(psource), start_date, end_date)
        response = do_get(url, cookies=cookies.get(psource))
        with open(fp, "wb") as code:
            code.write(response.read())

            with open(fp, 'r', newline="\n", encoding='cp1251') as test:
                file = test.readline()
                f = csv.reader(test, lineterminator='\n')
                for row in f:
                    try:

                        val = str(row)
                        val = str.replace(val, "']", "")
                        val = val.split(';')
                        if val[3] == "0":
                            continue
                        else:
                            values = str(tuple([psource] + [convert_datetime(start_date)] + val[1:23]))
                            mysql_sql_in = "insert into users_visits(psource,date, uid,currency,hits,amount_of_bets,qnty_of_bets,game_points,amount_of_winnings,qnty_of_winnings,amount_of_buyin_bets,amount_of_buyin_winnings,inout_by_games," \
                                    "amount_of_awards,qnty_of_awards,amount_of_points,qnty_of_point_converts,amount_of_deposits,qnty_of_deposits,fd_sum,rebill_sum,amount_of_payoffs,qnty_of_payoffs,inout_by_cash) values " + values
                            if n % 500 == 0:
                                print(n)
                                mysql_conn.commit()
                            mysql_cur.execute(mysql_sql_in)
                            n += 1

                    except err.IntegrityError:
                        print("inserting duplicate was omitted")
                    except err.InternalError:
                        print("error in row: " + mysql_sql)
                mysql_sql_up = "update users_visits set currency = null where currency = 'None'"
                mysql_cur.execute(mysql_sql_up)
                mysql_conn.commit()
                print(str(n) + " rows were inserted into table MAILING.users_visits")

        os.remove(fp)


        if get_event(psource) == dt.date.today() - relativedelta(days=1):
            break
            print('Список пуст')


        #cur_date += relativedelta(days=1)
        #print(cur_date, end_period_date)

#        if cur_date == end_period_date:
#            break
#            print('Список пуст')


insert_users(1) #Casino-x
insert_users(2) #Joy-casino # вернуть "как было" после заполнения
insert_users(3) #Fonbet #