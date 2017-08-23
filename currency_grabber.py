import datetime
from pymysql import *


def get_pass():  # define your pass here
    return 'FNZloy1989'

def get_currency(date):
    currencies = ['USD', 'EUR', 'RUB', 'NOK', 'SEK'] # change (add/remove) currencies here
    from bs4 import BeautifulSoup
    from urllib.request import Request, urlopen
    url = 'http://www.cbr.ru/currency_base/daily.aspx?date_req=' + date
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
    conn = connect(host='localhost', user='root', password=get_pass(), db='mailing', charset='utf8mb4',
                   cursorclass=cursors.DictCursor)
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
        conn.commit()
        print(n, " rows inserted")
        conn.close()

def update_currency_table():
    # Connect to the database 'mailing'
    conn = connect(host='localhost', user='root', password=get_pass(), db='mailing', charset='utf8mb4',
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

def get_last_date():
    # Connect to the database 'mailing'
    conn = connect(host='localhost', user='root', password=get_pass(), db='mailing', charset='utf8mb4',
                   cursorclass=cursors.DictCursor)
    c = conn.cursor()
    sql = "select max(cdate) md from currency_rates;"
    c.execute(sql)
    return c.fetchone().get('md')

base = datetime.datetime.today()
days_ago = int(str(datetime.datetime.today().date() - get_last_date())[0])
print(days_ago)
date_list = [str(base - datetime.timedelta(days=x))[0:10] for x in range(0, days_ago)]

S = list()
S.append(map(get_currency, date_list))

r = list()
for s in S:
    for e in s:
        r.extend(e)

print("...data successfully grabbed from site")
upload_currency(r)
update_currency_table()



