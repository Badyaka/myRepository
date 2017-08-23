# import library
import gzip
from os import listdir
import csv
from datetime import datetime
import re
#import json

import pypyodbc
import pymysql
from pymysql import err
from pymysql import *


'''
def my_json_to_csv_convert():

    with open("C:\\Users\\user\\Downloads\\special_ach_stats_csx.json") as file:
        data = json.load(file)

    with open("C:\\Users\\user\\Downloads\\data.csv", "w") as file:
        csv_file = csv.writer(file)
        for item in data:
            csv_file.writerow([item['_id__$oid'], item['date_earned__$date']] + item['fields'].values())
'''

def weekly_CP_sample_split():


    # склеиваю в одну weekly выборки csv
    # местоположение:
    # C:\DUMP_ALL\(my_)reports\2017.04.11_CPoints\IN\5_month(by_week)
    # header = "psource,start_date,end_date,uid,bets_type,amount_of_bets,qnty_of_bets,game_points,amount_of_winnings,qnty_of_winnings,amount_of_points,qnty_of_point_converts,amount_of_deposits,qnty_of_deposits,amount_of_payoffs,qnty_of_payoffs".split(",")

        indir = "C:\\DUMP_ALL\\(my_)reports\\2017.04.11_CPoints\\IN\\5_month(by_week)"
        outdir = "C:\DUMP_ALL\\(my_)reports\\2017.04.11_CPoints\\IN\\5_month(by_week)\\all_together_" + str(datetime.utcnow())[0:10] + ".csv"
        count = 0

        L=[]
        lines = []
        header = "psource,start_date,end_date,uid,bets_type,amount_of_bets,qnty_of_bets,game_points,amount_of_winnings,qnty_of_winnings,amount_of_points,qnty_of_point_converts,amount_of_deposits,qnty_of_deposits,amount_of_payoffs,qnty_of_payoffs".split(",")



        with open(outdir, 'w', newline='\n') as fp:
            w = csv.writer(fp, dialect='excel', delimiter=";")
            for file in listdir(indir):
                if file.endswith(".csv"):
                    try:
                        lcount = 0
                        #name = file[0:len(file)-3]  # name
                        count += 1   # no of files processed
                        fname = indir + "\\"+file  # file path
                        for s in open(fname, 'rb'):
                        # for s in open(fname, 'rb'):
                            if s.startswith(bytes("psource", 'utf-8')):
                                pass
                            else:
                                #if s.rfind(bytes("first_dep", 'utf-8')) != 0 and ( s.rfind(bytes("2016-11-", 'utf-8')) != 0 or s.rfind(bytes("2016-12-", 'utf-8')) != 0):

                                line = s.decode('utf-8').split(",")

                                line[-1] = re.sub("^\s+|\n|\r|\s+$", '',line[-1])

                                #line = re.sub("^\s+|\n|\r|\s+$", '', s.decode('utf-8').split(","))
                                #line.insert(0, name)

                                lines.append(line)
                                lcount += 1
                        #L.append(lcount)
                        print(fname, " lines: ", lcount)

                    except FileNotFoundError:
                        print("oops! no such file on a path")
            lines.insert(0, header)
            w.writerows(lines)

        fp.close()

def replace_project_id(casino_id):
    ids = {1020: 1, 2003: 2}
    return ids.get(casino_id, -1)

def first_dep_import(dummy_ps):
    ''' забираю только нужные депозиты (по дате) из общей выборки (импорта)
     местоположение:
        C:\DUMP_ALL\from_csv\csv_casinox
     header=

     при возможности сделать обновление этого файла первых депов (из админки с начала работы и по текущую дату)
     нужно запомнить с какого периода для каждого проекта
        # 12.12.2012 for 21
        # 20.06.2014 for 94
     разобраться с конвертацией даты
     вытягивать архив из админки автоматически!!!
    '''
    if dummy_ps == 1:
        indir = "C:\\DUMP_ALL\\from_csv\\csv_casinox"
        outdir = "C:\\DUMP_ALL\\from_csv\\csv_casinox\\first_dep_CX_" + str(datetime.utcnow())[0:10] + ".csv"
    elif dummy_ps == 2:
        indir = "C:\\DUMP_ALL\\from_csv\\csv_joycasino"
        outdir = "C:\\DUMP_ALL\\from_csv\\csv_joycasino\\first_dep_JC_" + str(datetime.utcnow())[0:10] + ".csv"

    count = 0

    L=[]
    lines = []
    header = "date;uid;amount;currency;payment_type_id;type".split(";")

    with open(outdir, 'w', newline='\n') as fp:
        w = csv.writer(fp, dialect='excel', delimiter=";")

        for file in listdir(indir):

            if file.endswith(".gz") and (file.startswith("casino-x.com-deps") or file.startswith("joycasino.com-deps")):
            #if file.endswith(".gz")

                try:
                    lcount = 0

                    count += 1   # no of files processed
                    fname = indir + "\\"+file  # file path

                    for s in gzip.open(fname, 'rb'):

                        if s.startswith(bytes("date", 'utf-8')):
                            pass
                        else:

                            line = s.decode('utf-8').split(",")

                            if line[5].find("first_dep") == 0:
                               line[5]= line[5].rstrip()

                            #if line[1].find("58763f32d02eb170ef49d430") == 0:
                            #    line[1] = line[1].rstrip()

                               lines.append(line)
                               lcount += 1

                    print(fname, " lines: ", lcount)

                except FileNotFoundError:
                    print("oops! no such file on a path")
        lines.insert(0, header)
        w.writerows(lines)

    fp.close()

if __name__ == '__main__':
    print(replace_project_id(2003))
    # weekly_CP_sample_split()
   # first_dep_import(1)


    #def get_sqlserver_pass():
     #   return 'Loh3shoh'


    sql_server_string = "DRIVER={SQL Server}; SERVER=78.140.130.90; DATABASE=pomodorro; " \
                        "UID=f.navruzov; PWD=Loh3shoh; Charset:UTF-8"
    sqlserver_conn = pypyodbc.connect(sql_server_string)
    sqlserver_cur = sqlserver_conn.cursor()
    #sqlserver_sql = "select IntId, uid, currency, reg_ip, RegDate from  pomadorro_reg " \
    #                   "where  IntId in (1020, 2003)  and regdate > Cast('2017-07-18 0:00:00' as datetime)"

    '''
    sqlserver_sql = "select  IntId, amount, currency, date, type, user_id " \
                    "from  pomadorro_bonuses " \
                    "where IntId in (2003) and date > '2017-07-24'" \
                    "and type in ('freespin', 'cash_bonus', 'cash_admin_bonus', 'cancel_bonus')"
    '''
    '''
    sqlserver_sql = "select  IntId, amount, currency, date, payment_group_id, " \
                    "case when type = 'cash' or type = 'first_deposit' then 'payin' else 'payout' end as type, uid " \
                    "from  pomadorro_tranz " \
                    "where  IntId in (2003) and date >= '2017-07-15' AND date <  '2017-07-16' and invoiceid  is not null"
    '''

    '''
    sqlserver_sql = "select IntId, balance_after, balance_before, bets, bets_cnt, currency, " \
                    "date, gameref, user_id, wins, wins_cnt " \
                    "from pomadorro_casino_games " \
                    "where IntId in (1020, 2003) and date > '2017-07-15'"
    '''

'''
    sqlserver_sql = "select * from pomadorro_casino_games " \
                    "where IntId in (2003) and date >= '2017-08-06'"

    print(sqlserver_sql)
    result = sqlserver_cur.execute(sqlserver_sql)
    for record in result:
        print (record)
    #print (result.fetchall())
'''





#    first_dep_import(1)
'''    try:
        print (True + 10 / (False * 3 + True))
    except Exception as e:
        print (e)
'''

'''
m = b'58a379598a4feac83bfeabe0,2017-02-14 21:40:52+00,cgames,RUB,188.187.158.187,1606e5206fa9b9e21\n'
n = b'58a37aab8a4feac83bfed293,2017-02-14 21:46:30+00,cgames,RUB,2a02:2168:23ef:5200:8d8c:fe7a:ffb1:fedb,bd5b5087be1c1ba0d\n'
print (n, type(n))
#print (n.decode('utf-8'), type (n.decode('utf-8')))
#print (n.decode('utf-8').split(','), type (n.decode('utf-8').split()))


dummy_list = n.decode('utf-8').split(',')
if len(dummy_list[4]) > 15:
    dummy_list[4] = dummy_list[4][0:10] + "IPv6"
    n = ",".join(dummy_list).encode('utf8')
    print (n, type(n))



    #n.decode('utf-8').split(',')[4] = n.decode('utf-8').split(',')[4][0:10]+"IPv6"
    #print (n.decode('utf-8').split(',')[4])
    #text = text.encode('utf8')
#print (n)
'''



'''
# contact_processor_cut
class ActiveContactController(BaseContactController):
    department = 'd2'

    def _default_accounting(self):
        return {
            'contacts_num': 0,
            'contacts_coef': 0,
            'period_completed': False,
            'month_active_num': None,
            'month_awolness_num': None,
            'remain_active_users_coef': None,
            'awol_days_num': 0,
            'awol_days_coef': 1,
            'group_duration_seconds': 0,
            'group_coef': 0,
            'qa_avg_coef': 1,
        }

    def _import_settings(self, user, settings):
        new_settings = {
            'contacts_range_coef': user.cc_settings.get('contacts_range_coef') or settings['contacts_range_coef'],
            'awolness_range_coef': settings['awolness_range_coef']
        }
        return new_settings

    @gen.coroutine
    def calculate_bonus(self, user):
        """
        (MaxFeePerSale { % * NetProfit (AccountingPeriod) }) * CoefContact * CoefAwol
        """
        def _calc_bonus(amount, qa_coef, contacts_coef, awol_days_coef, remain_active_coef):
            return int(round(amount * qa_coef * contacts_coef * awol_days_coef * (remain_active_coef or 1)))

        contact = self.contact
        settings = contact.settings
        acct = contact.accounting
        acct['acct_seconds'] = (contact.accounting_end - contact.accounting_start).total_seconds()

        cc_unit = get_cc_unit_by_lang(contact.lang)
        cc_langs = get_cc_langs_by_unit(cc_unit)

        ym = contact.date.strftime('%Y-%m')
        period_start, period_end = self.get_month_interval(contact.date)

        recheck_contacts = self.forced_recheck or not acct.get('period_completed', False)
        if recheck_contacts:
            acct['period_completed'] = utcnow() > period_end

            # TODO check user's settings
            acct['contacts_num'] = yield self.get_user_contacts_number(contact['user_id'], period_start, period_end)
            acct['contacts_coef'] = round(settings['contacts_range_coef'].get(str(acct['contacts_num']), 1), 5)

            # find users who were active but had AWOLness in the last day
            if acct['period_completed']:
                if self.cache.setdefault('month_active_awolness_num', {}).setdefault(cc_unit, {}).get(ym) is None:
                    self.cache['month_active_awolness_num'][cc_unit][ym] = yield self.get_active_awolness_users_number(contact.date, cc_langs)

                acct['month_active_num'] = self.cache['month_active_awolness_num'][cc_unit][ym][0]
                acct['month_awolness_num'] = self.cache['month_active_awolness_num'][cc_unit][ym][1]

            # TODO check user's settings
            acct['awol_days_num'] = yield self.get_user_awolness_days_count(contact.user_id, ym)
            acct['awol_days_coef'] = settings['awolness_range_coef'].get(str(acct['awol_days_num']))

        if acct['month_active_num']:
            acct['remain_active_users_coef'] = min(1, float(acct['month_awolness_num'] or 0) / (acct['month_active_num']))

        calc_ratio = lambda a, b: round(float(a)/b if b else 0, 5)

        # get positive netprofit less than limit_usd
        _extract_positive_np = lambda np, limit, rate: max(min(np*rate, limit), 0)

        # calculate total group bonus amount and divide it proportionally between
        # the all user's contacts for chosen period of time
        linked_contacts = yield self.get_linked_contacts()

        positive_netprofit_usd = _extract_positive_np(acct['raw_netprofit_usd'], settings['limit_usd'], settings['rate'])

        netprofit_by_operators = {}
        netprofit_by_operators[contact['operator_id']] = [positive_netprofit_usd*acct['qa_ratio'], positive_netprofit_usd]

        group_seconds = acct['acct_seconds']
        group_brutto_usd_bonus_amount = positive_netprofit_usd
        for lc in linked_contacts:
            lc_positive_netprofit_usd = _extract_positive_np(lc['accounting'].get('raw_netprofit_usd', 0), lc['settings']['limit_usd'], lc['settings']['rate'])
            group_seconds += lc.get('accounting', {}).get('acct_seconds', 0)
            group_brutto_usd_bonus_amount += lc_positive_netprofit_usd

            _np = netprofit_by_operators.setdefault(lc['operator_id'], [0, 0])
            _np[0] += (lc_positive_netprofit_usd * lc.get('accounting', {}).get('qa_ratio', 1))
            _np[1] += lc_positive_netprofit_usd

        qa_coef_by_operators = {}
        for oid, np in netprofit_by_operators.iteritems():
            qa_coef_by_operators[oid] = 1
            if np[1] != 0:
                qa_coef_by_operators[oid] = round(np[0] / float(np[1]), 5)

        acct['group_coef'] = calc_ratio(acct['acct_seconds'], group_seconds)

        for lc in linked_contacts:
            lc_acct = lc.get('accounting', {})

            lc_group_coef = calc_ratio(lc_acct.get('acct_seconds', 0), group_seconds)
            brutto_usd_bonus_amount = int(round(group_brutto_usd_bonus_amount * lc_group_coef))
            lc_qa_avg_coef = qa_coef_by_operators.get(lc['operator_id'], 1)

            q = dict(
                collection=CONTACTS_COLLECTION,
                query={'_id': lc['_id']},
                data={'$set': {
                    'usd_bonus_amount': _calc_bonus(brutto_usd_bonus_amount, lc_qa_avg_coef, lc_acct['contacts_coef'], lc_acct['awol_days_coef'], lc_acct.get('remain_active_users_coef')),
                    'accounting.brutto_usd_bonus_amount': brutto_usd_bonus_amount,
                    'accounting.qa_avg_coef': lc_qa_avg_coef,
                    'accounting.group_coef': lc_group_coef,
                    'accounting.group_duration_seconds': group_seconds,
                    'accounting.group_brutto_usd_bonus_amount': int(round(group_brutto_usd_bonus_amount))
                }},
                upsert=False)
            yield gen.Task(self.mongo.update, **q)

        log.info('[{}][{}] Share profits with a group of another contacts: {}'.format(contact._id, contact.department, len(linked_contacts)))

        qa_avg_coef = qa_coef_by_operators.get(contact['operator_id'], 1)

        brutto_usd_bonus_amount = int(round(group_brutto_usd_bonus_amount * acct['group_coef']))
        acct['qa_avg_coef'] = qa_avg_coef
        acct['brutto_netprofit_usd'] = acct['raw_netprofit_usd']
        acct['group_duration_seconds'] = group_seconds
        acct['group_brutto_usd_bonus_amount'] = int(round(group_brutto_usd_bonus_amount))
        acct['brutto_usd_bonus_amount'] = brutto_usd_bonus_amount
        contact.usd_bonus_amount = _calc_bonus(brutto_usd_bonus_amount, qa_avg_coef, acct['contacts_coef'], acct['awol_days_coef'], acct['remain_active_users_coef'])


class AWOLContactController(BaseContactController):
    department = 'd3'

    def _default_accounting(self):
        return {
            'group_num': 0,
            'group_coef': 0,
        }

    @gen.coroutine
    def calculate_bonus(self, user):
        """
        MaxFeePerSale { % * NetProfit (AccountingPeriod) }
        """
        def _calc_bonus(amount, qa_ratio):
            return int(round(amount * qa_ratio))

        contact = self.contact
        acct = contact['accounting']
        settings = contact.settings

        # some operators or another contacts could have shared period
        linked_contacts = yield self.get_linked_contacts()
        group_num = len(linked_contacts) + 1
        group_coef = round(1.0 / group_num, 5)

        # netprofit per one contact
        brutto_netprofit_usd = acct['raw_netprofit_usd'] * group_coef
        brutto_usd_bonus_amount = max(min(settings['limit_usd'], acct['raw_netprofit_usd'] * settings['rate']), 0) * group_coef

        for lc in linked_contacts:
            usd_bonus_amount = _calc_bonus(brutto_usd_bonus_amount, lc['accounting'].get('qa_ratio', 1))
            q = dict(
                collection=CONTACTS_COLLECTION,
                query={'_id': lc['_id']},
                data={'$set': {
                    'usd_bonus_amount': usd_bonus_amount,
                    'accounting.brutto_netprofit_usd': brutto_netprofit_usd,
                    'accounting.brutto_usd_bonus_amount': brutto_usd_bonus_amount,
                    'accounting.group_num': group_num,
                    'accounting.group_coef': group_coef,
                }},
                upsert=False)
            yield gen.Task(self.mongo.update, **q)

        acct['group_num'] = group_num
        acct['group_coef'] = group_coef
        acct['brutto_netprofit_usd'] = brutto_netprofit_usd
        acct['brutto_usd_bonus_amount'] = brutto_usd_bonus_amount
        self.contact.usd_bonus_amount = _calc_bonus(brutto_usd_bonus_amount, acct['qa_ratio'])


class VipContactController(BaseContactController):
    department = 'd4'

    def _default_accounting(self):
        return {
            'total_vip_users': 0,
            'contacted_vip_users': 0,
            'contacted_vip_users_coef': 0,
            'period_completed': False,
            'group_num': 0,
            'group_coef': 0,
        }

    @gen.coroutine
    def calculate_bonus(self, user):
        """
        (MaxFeePerSale { % * NetProfit (AccountingPeriod) }) * (VipMonth / VipTotal)
        """
        def _calc_bonus(amount, qa_ratio, all_vip_contacted):
            return int(round(amount * qa_ratio * all_vip_contacted))

        contact = self.contact
        acct = contact['accounting']
        settings = contact.settings

        cc_unit = get_cc_unit_by_lang(contact.lang)
        cc_langs = get_cc_langs_by_unit(cc_unit)

        # VIP users can have a period for calculation coefs even in another month
        ym = self.contact.end_sale_date.strftime('%Y-%m')
        period_start, period_end = self.get_month_interval(contact.end_sale_date)

        # save the number of vip user only once
        if not acct.get('total_vip_users') or self.forced_recheck:
            if self.cache.setdefault('total_vip_users', {}).get(cc_unit, None) is None:
                self.cache['total_vip_users'][cc_unit] = yield self.get_all_vip_users_number(cc_langs)
            acct['total_vip_users'] = self.cache['total_vip_users'][cc_unit]

        # recheck number of contacted users till to next month
        recheck_contacted_vip = self.forced_recheck or not acct.get('period_completed', False)
        if recheck_contacted_vip:
            if self.cache.setdefault('contacted_vip_users', {}).setdefault(ym, {}).get(cc_unit, None) is None:
                self.cache['contacted_vip_users'][ym][cc_unit] = yield self.get_contacted_vip_users_number(period_start, period_end, cc_langs)

            acct['contacted_vip_users'] = self.cache['contacted_vip_users'][ym][cc_unit]
            acct['period_completed'] = utcnow() > period_end

        if acct.get('total_vip_users', 0):
            contacted_vip_users_coef = round(float(acct['contacted_vip_users']) / acct['total_vip_users'], 5)
            acct['contacted_vip_users_coef'] = max(0, min(1, contacted_vip_users_coef))

        # find all VIP users contacts and calculate proportion
        linked_contacts = yield self.get_linked_contacts()
        group_num = len(linked_contacts) + 1
        group_coef = round(1.0 / group_num, 5)

        brutto_netprofit_usd = acct['raw_netprofit_usd'] * group_coef
        brutto_usd_bonus_amount = max(min(settings['limit_usd'], acct['raw_netprofit_usd'] * settings['rate']), 0) * group_coef

        for lc in linked_contacts:
            qa_ratio = lc['accounting'].get('qa_ratio', 1)

            q = dict(
                collection=CONTACTS_COLLECTION,
                query={'_id': lc['_id']},
                data={'$set': {
                    'usd_bonus_amount': _calc_bonus(brutto_usd_bonus_amount, qa_ratio, acct['contacted_vip_users_coef']),
                    'accounting.brutto_netprofit_usd': brutto_netprofit_usd,
                    'accounting.brutto_usd_bonus_amount': brutto_usd_bonus_amount,
                    'accounting.group_num': group_num,
                    'accounting.group_coef': group_coef,
                    'accounting.total_vip_users': acct['total_vip_users'],
                    'accounting.contacted_vip_users': acct['contacted_vip_users'],
                    'accounting.contacted_vip_users_coef': acct['contacted_vip_users_coef'],
                }},
                upsert=False)
            yield gen.Task(self.mongo.update, **q)

        acct['brutto_netprofit_usd'] = brutto_netprofit_usd
        acct['brutto_usd_bonus_amount'] = brutto_usd_bonus_amount
        acct['group_num'] = group_num
        acct['group_coef'] = group_coef
        self.contact.usd_bonus_amount = _calc_bonus(brutto_usd_bonus_amount, acct['qa_ratio'], acct['contacted_vip_users_coef'])
'''

'''
def main():
    a, b = map(int, input().split())
    res = a + b
    print(res)


if __name__ == "__main__":
    main()


#or

import sys
a, b = sys.stdin.read().split()
print(int(a) + int(b))

'''