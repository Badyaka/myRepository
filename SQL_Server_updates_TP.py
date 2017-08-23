import pypyodbc
import pymysql
from pymysql import err
from pymysql import *
import datetime


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
    ids = {1020: 1, 2003: 2}
    return ids.get(casino_id, -1)


#sql_server_string = "DRIVER={SQL Server}; SERVER=10.8.4.241; DATABASE=pomodorro; " \
#               "UID=f.navruzov; PWD=" + get_sqlserver_pass() + "; Charset:UTF-8"

#since 26/05/2017 the server address was changed to the following 78.140.130.90
sql_server_string = "DRIVER={SQL Server}; SERVER=78.140.130.90; DATABASE=pomodorro; " \
               "UID=f.navruzov; PWD=" + get_sqlserver_pass() + "; Charset:UTF-8"

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


def update_users():  # update users registration (table mailing.users, MySQL)
    def get_max_user_date():
        # Connect to the database 'mailing'
        conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                       cursorclass=cursors.DictCursor)
        c = conn.cursor()
        sql = "select max(regdate) md from users;"
        c.execute(sql)
        return convert_datetime(c.fetchone().get('md'))

    def get_user_updates():
        # connect to MS SQL SERVER DB
        sqlserver_conn = pypyodbc.connect(sql_server_string)
        sqlserver_cur = sqlserver_conn.cursor()
        sqlserver_sql = "select IntId, uid, currency, reg_ip, RegDate from  pomadorro_reg " \
        "where  IntId in (1020, 2003)  and regdate > Cast('" + get_max_user_date() + "' as datetime)"
        print(sqlserver_sql)
        result = sqlserver_cur.execute(sqlserver_sql)
        return result.fetchall()

    def insert_users():
        print('---------------------------------------------')
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
        mysql_cur = mysql_conn.cursor()
        n = 0  # no of rows inserted

        for i in get_user_updates():

            try:
                #print (type(i), i)
                # try to solve problem with IPv6

                dummy_ipv = i[3]
                #print (type(dummy_ipv), dummy_ipv)
                if dummy_ipv is not None and len(i[3]) > 15:
                    dummy_ipv = i[3][0:10] + "IPv6"

                #values = str(tuple([replace_project_id(i[0]), i[1], i[2],  i[3], convert_datetime(i[4])]))
                values = str(tuple([replace_project_id(i[0]), i[1], i[2], dummy_ipv, convert_datetime(i[4])]))

                values = str.replace(values, "None", 'NULL')

                #print(type(values), values)

                mysql_sql = 'insert ignore into users(psource, uid, currency, reg_ip, regdate) values ' + values
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

    def update_users_table():
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor, autocommit=True)
        mysql_cur = mysql_conn.cursor()
        sql = '''
        update users u inner join (select psource, uid, currency from transactions group by 1,2) t
        on u.uid = t.uid and t.psource = u.psource
        set u.currency = t.currency
        where u.currency is null or u.currency = '' or u.currency = 'nul'
        '''
        mysql_cur.execute(sql)
        print("user's currencies were updated (where it was null)")

    insert_users()  # get updates (SQL Server) and insert into users table (MySQL)
    update_users_table()  # updating currency where currency is NULL


def update_transactions():  # update users transactions (table mailing.transactions, MySQL)
    def get_last_trans_date():
        # Connect to the database 'mailing'
        conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                       cursorclass=cursors.DictCursor)
        c = conn.cursor()
        sql = "select max(stdate) md from transactions;"
        c.execute(sql)
        return convert_datetime(c.fetchone().get('md'))

    def get_trans_updates():
        # connect to MS SQL SERVER DB

        sqlserver_conn = pypyodbc.connect(sql_server_string)
        sqlserver_cur = sqlserver_conn.cursor()
        sqlserver_sql = "select  IntId, amount, currency, date, payment_group_id, " \
        "case when type = 'cash' or type = 'first_deposit' then 'payin' else 'payout' end as type, uid " \
        "from  pomadorro_tranz " \
        "where  IntId in (1020, 2003)  and date > Cast('" + get_last_trans_date() + "' as datetime) and invoiceid  is not null"
        # "where  IntId in (1020, 2003)  and date > Cast('" + get_last_trans_date() + "' as datetime) and status = 1" #изменили структуру базы, status перестал обновляться (NULL) и появилось поле invoiceid
        print(sqlserver_sql)
        result = sqlserver_cur.execute(sqlserver_sql)
        return result.fetchall()

    def insert_trans():
        print('---------------------------------------------')
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
        mysql_cur = mysql_conn.cursor()
        n = 0  # no of rows inserted
        for i in get_trans_updates():
            try:
                values = str(tuple([replace_project_id(i[0]), i[1], i[2], convert_datetime(i[3]), i[4], i[5], i[6]]))
                values = str.replace(values, "None", 'NULL')
                mysql_sql = "insert into transactions(psource, amount, currency, stdate, payment_group_id, " \
                            " type, uid) values " + values + " on duplicate key update amount = amount + values(amount)"
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
        update transactions t inner join currency_rates c
        on date(t.stdate) = c.cdate and t.currency = c.rub_to
        set t.amt_usd = t.amount * c.usd_rate
        where amt_usd is null;
        '''
        mysql_cur.execute(sql)
        print("transactions (in USD) were updated")

    insert_trans()  # get updates (SQL Server) and insert into users table (MySQL)
    update_trans_table()  # updating payments in USD


def update_bonuses():  # update users bonuses (table mailing.bonuses, MySQL)
    def get_last_bonus_date():
        # Connect to the database 'mailing'
        conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                       cursorclass=cursors.DictCursor)
        c = conn.cursor()
        sql = "select max(date) md from bonuses;"
        c.execute(sql)
        return convert_datetime(c.fetchone().get('md'))

    def get_bonus_updates():
        # connect to MS SQL SERVER DB
        sqlserver_conn = pypyodbc.connect(sql_server_string)
        sqlserver_cur = sqlserver_conn.cursor()
        sqlserver_sql = "select  IntId, amount, currency, date, type, user_id " \
        "from  pomadorro_bonuses " \
        "where IntId in (1020, 2003) and date > Cast('" + get_last_bonus_date() + "' as datetime) " \
        "and type in ('freespin', 'cash_bonus', 'cash_admin_bonus', 'cancel_bonus')"
        print(sqlserver_sql)
        result = sqlserver_cur.execute(sqlserver_sql)
        return result.fetchall()

    def insert_bonus():
        print('---------------------------------------------')
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
        mysql_cur = mysql_conn.cursor()
        n = 0  # no of rows inserted
        for i in get_bonus_updates():
            try:
                values = str(tuple([replace_project_id(i[0]), i[1], i[2], convert_datetime(i[3]), i[4], i[5]]))
                values = str.replace(values, "None", 'NULL')
                mysql_sql = "insert ignore into bonuses(psource, amount, currency, date, type, uid) values " + values
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
        print(str(n) + " rows were inserted into table MAILING.BONUSES")

    insert_bonus()  # get updates (SQL Server) and insert into bonus table (MySQL)


def update_games():  # update users games (table mailing.bonuses, MySQL)
    def get_last_game_date():
        # Connect to the database 'mailing'
        conn = connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                       cursorclass=cursors.DictCursor)
        c = conn.cursor()
        sql = "select max(date) md from games;"
        c.execute(sql)
        return convert_datetime(c.fetchone().get('md'))

    def get_game_updates():
        # connect to MS SQL SERVER DB
        sqlserver_conn = pypyodbc.connect(sql_server_string)
        sqlserver_cur = sqlserver_conn.cursor()
        sqlserver_sql = "select IntId, balance_after, balance_before, bets, bets_cnt, currency, " \
                        "date, gameref, user_id, wins, wins_cnt " \
                        "from pomadorro_casino_games " \
                        "where IntId in (1020, 2003) and date > Cast('" + get_last_game_date() + "' as datetime)"
        #############"where IntId in (1020) and date >= '2017-07-24' AND date < '2017-07-28'"

        print(sqlserver_sql)
        result = sqlserver_cur.execute(sqlserver_sql)
        return result.fetchall()

    def insert_game():
        print('---------------------------------------------')
        # Connect to the database 'mailing' in local MySQL DB
        mysql_conn = pymysql.connect(host='localhost', user='root', password=get_mysql_pass(), db='mailing', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
        mysql_cur = mysql_conn.cursor()
        n = 0  # no of rows inserted
        for i in get_game_updates():
            try:
                values = str(tuple([replace_project_id(i[0]), i[1], i[2], i[3], i[4], i[5],
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

    insert_game()  # get updates (SQL Server) and insert into bonus table (MySQL)


try:
    update_currency_rates()
except ValueError:
    print("currencies already fetched")

update_transactions()  # only for old table "transactions", use website_grabber.py instead !
##update_users()
update_bonuses()

##update_games()