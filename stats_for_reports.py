from pymysql import *
import csv
import time
from datetime import datetime


def get_pass():
    return 'FNZloy1989'


def get_query():
    return '''
    select 	MT2.stdate, if(p.segment='COMMON',  MT2.mail_name, left(MT2.mail_name, locate('_', MT2.mail_name)-1)) parent, MT2.mail_name, MT2.ptype,
		p.segment, md2.min_dep min_dep_usd, md2.no_of_fs, MT2.user_count, p.open, MT2.act_cnt, MT2.true_act, MT2.auto_act,
        MT2.act_deps_usd, MT2.min_deps_usd, MT2.true_effect_usd,
		MT2.nact_balance, MT2.nact_maxdep, MT2.nact_empty, MT2.nact_early, MT2.nact_awol, MT2.nact_other,
        MT2.`awol 0`, MT2.`awol 1-7`, MT2.`awol 8-14`, MT2.`awol 15-30`, MT2.`awol 30+`,
		p.theme, round(p.dep_sum_traced) dep_sum_traced


from

(select 	MT.stdate, MT.mail_name, MT.ptype,
		sum(MT.activated) act_cnt, round(sum(MT.min_dep2),0) min_deps_usd, round(sum(MT.deps_act_usd),0)  act_deps_usd,
        count(MT.m_uid) user_count, sum(MT.open_count) open, -- sum(MT.activated) act_cnt,
        round(sum(MT.real_act_usd),0)  true_effect_usd,

        sum(case when MT.segment = 'ACT: TRUE' then 1 else 0 end) true_act,
        sum(case when MT.segment = 'ACT: AUTO' then 1 else 0 end) auto_act,
        sum(case when MT.segment = 'NO-ACT: ENOUGH BALANCE' then 1 else 0 end) nact_balance,
        sum(case when MT.segment = 'NO-ACT: LOW MAX DEPOSIT' then 1 else 0 end) nact_maxdep,
        sum(case when MT.segment = 'NO-ACT: EMPTY?' then 1 else 0 end) nact_empty,
        sum(case when MT.segment = 'NO-ACT: TOO EARLY' then 1 else 0 end) nact_early,
        sum(case when MT.segment = 'AWOL 30+' then 1 else 0 end) nact_awol,
        sum(case when MT.segment = 'OTHER' then 1 else 0 end) nact_other,

        sum(case when MT.activated = 1 and MT.AWOL_TYPE = 'AWOL 0' then 1 else 0 end) `awol 0`,
        sum(case when MT.activated = 1 and MT.AWOL_TYPE = 'AWOL 1-7' then 1 else 0 end) `awol 1-7`,
        sum(case when MT.activated = 1 and MT.AWOL_TYPE = 'AWOL 8-14' then 1 else 0 end) `awol 8-14`,
        sum(case when MT.activated = 1 and MT.AWOL_TYPE = 'AWOL 15-30' then 1 else 0 end) `awol 15-30`,
        sum(case when MT.activated = 1 and MT.AWOL_TYPE = 'AWOL 30+' then 1 else 0 end) `awol 30+`


from
(
		-- explain
		select total.*,
        -- 1 user_count, -- I ADD IT HERE
		(case  when total.activated = 1 then total.min_deps else 0 end) min_dep2,
		(case 	when total.deps_act_usd > total.min_deps*2 then '2X+'
				when total.deps_act_usd >= total.min_deps*1.5 then '1.5-2X'
				when total.deps_act_usd >= total.min_deps*1.05 then '1-1.5X'
				when total.deps_act_usd >= total.min_deps or (total.act_deps_cnt = 0 and total.activated = 1) then 'Exact'
				else null end) min_dep_type,

		greatest((case 	when total.max_dep_before_usd >= total.min_deps then 0
				when act_deps_cnt = 1 then total.min_deps - total.avg_dep_before_usd
				else total.deps_act_usd - total.min_deps + total.avg_dep_before_usd end),0) real_act_usd,

		-- SEGMENT DEFINITION

		(case 	when total.activated = 1 and total.max_dep_before_usd >= total.min_deps then 'ACT: AUTO'
				when total.activated = 1 and total.max_dep_before_usd < total.min_deps then 'ACT: TRUE'
				when total.activated = 0 and total.max_dep_before_usd >= total.min_deps*3/4 and total.balance > total.min_deps then 'NO-ACT: ENOUGH BALANCE'
				when total.activated = 0 and total.max_dep_before_usd >= total.min_deps*3/4 and total.balance <= total.min_deps/2 then 'NO-ACT: EMPTY?'
				when total.activated = 0 and total.max_dep_before_usd >= total.min_deps*3/4 and total.expected_date >= total.stdate + interval 2 day then 'NO-ACT: TOO EARLY'
				when total.activated = 0 and total.max_dep_before_usd = 0 then 'AWOL 30+'
				when total.activated = 0 and total.max_dep_before_usd < total.min_deps*3/4 then 'NO-ACT: LOW MAX DEPOSIT'

				else 'OTHER'


		end) segment

		from
			(select

			  raw.stdate, raw.mail_name, raw.ptype, raw.m_uid,  raw.regdate, raw.open_count, raw.currency, raw.activated,
			  sum(case when raw.activated = 1 and raw.before_event = 0 and raw.dep_after_act = 1 then 1 else 0 end) act_deps_cnt,
			  sum(case when  raw.dep_after_act = 1 and raw.before_event = 0 and raw.dep_after_act = 1 and raw.activated = 1 then raw.amt_usd else 0 end) deps_act_usd,
			  avg(case when raw.ptype = 'FS' then raw.min_dep_usd else raw.CB_amt_usd end) min_deps,
			  sum(case when raw.before_event = 1 then 1 else 0 end) deps_before_cnt,
			  sum(case when  raw.before_event = 1 then raw.amt_usd else 0 end) deps_before_usd,
			  max(case when  raw.before_event = 1 then raw.amt_usd else 0 end) max_dep_before_usd,
			  avg(case when  raw.before_event = 1 then raw.amt_usd else 0 end) avg_dep_before_usd,
			  avg(raw.balance) balance,

			  max(case when raw.before_event = 1 then raw.tdate else null end) last_dep_date,
			  max(case when raw.before_event = 1 then raw.tdate else null end) + interval raw.dep_freq day expected_date,
			  (case
				when timestampdiff(day,  raw.last_game, raw.stdate) between 0 and 1 then 		'AWOL 0'
				when timestampdiff(day,  raw.last_game, raw.stdate) between 2 and 7 then 		'AWOL 1-7'
				when timestampdiff(day,  raw.last_game, raw.stdate) between 8 and 13 then	 'AWOL 8-14'
				when timestampdiff(day,  raw.last_game, raw.stdate) between 14 and 30 then  'AWOL 15-30'
				else 'AWOL 30+'
				end) AWOL_TYPE



			from (

					select m.uid m_uid, case when m.open_count >= 1 then 1 else 0 end open_count, m.first_open_date,
					case when a.uid is null then 0 else 1 end activated, u.regdate, tr.tdate, tr.before_event, tr.amt_usd, tr.md_usd, b.min_dep_date,
					gs.* , md.*,
                    f.dep_freq,
						   case when ((b.min_dep_date - interval 1 hour <= tr.tdate) or b.min_dep_date is null or (b.min_dep_date - interval 1 day - interval 1 hour <= tr.tdate))
                           and (tr.before_event = 0) then 1 else 0 end dep_after_act,

                           CB.CB_amt_usd

					from mailings m inner join mail_id mi on m.psource = mi.psource and m.mail_id = mi.mid
					-- ACTIVATIONS(uid, deposit_sum)
					left join activations a on m.uid = a.uid and a.psource = m.psource and mi.event_name = a.mail_name and a.present_type = mi.type
					-- MIN DEPOSITS (min_dep, ptype, stdate, enddate)
					left join min_deposits md on mi.event_name = md.mail_name and md.psource = mi.psource and mi.type = md.ptype
					-- USERS (regdate, currency)
					inner join users u on m.uid = u.uid and m.psource = u.psource and u.currency = md.currency
					-- transactions_miliseconds (total, BEFORE and DURING event)-----------------------------------------------------------------
					left join (
							select t.psource, t.uid, t.stdate tdate, t.currency, t.amt_usd/100 amt_usd, md2.mail_name, md2.ptype,
							md2.stdate, md2.enddate, md2.min_dep, md2.min_dep_usd md_usd,
							case
							when t.stdate < md2.stdate then 1 else 0
							end before_event

							from transactions_miliseconds t
							inner join min_deposits md2 on md2.currency = t.currency and md2.psource = t.psource

							where t.psource = @ps and t.type = 'payin'
							and t.stdate between md2.stdate - interval @time_frame day and md2.enddate
							and md2.min_dep > 0
                            # and match(md2.mail_name) against(@mname in boolean mode)
                            and md2.mail_name like @mname

					) tr on tr.uid = m.uid and tr.currency = md.currency and tr.ptype = mi.type and tr.psource = m.psource and tr.mail_name = md.mail_name
					-- BONUSES (TO GET MIN BONUS DATE) ------------------------------------------------------------------------------
					left join
							(select m.psource, m.mail_name, b.uid, b.currency, min(b.date) min_dep_date
							 from min_deposits m inner join bonuses b
								on b.type = m.ptype2  and m.psource = b.psource
								and m.currency = b.currency  and b.date between m.stdate and m.enddate
							 where b.psource=@ps  and m.ptype2 in ('freespin','cash_bonus') and m.psource =@ps  and m.min_dep > 0

                             #and match(m.mail_name) against(@mname in boolean mode)
                             and m.mail_name like @mname

							 group by 1,2,3,4
							 ) b on b.mail_name = mi.event_name and b.uid = m.uid and b.psource = mi.psource and b.currency = md.currency
					-- GAMES (TO GET LAST GAME PLAYED AND BALANCE)---------------------------------------------------------------------------------------------------------------
					left join
							(select m.*, g.balance_after/100 balance
							from  games g inner join
								(select md.psource s, uid, md.currency cur, max(a.date) last_game
								from games a inner join min_deposits md on md.psource = a.psource and md.currency = a.currency and a.date < md.stdate
								where md.psource  =@ps and a.psource =@ps
                                and md.min_dep > 0
                                #and match(md.mail_name) against(@mname in boolean mode)
                                and md.mail_name like @mname

								group by 1,2,3) m   on m.uid = g.uid and m.last_game = g.date and m.s = g.psource and m.cur = g.currency
								where g.psource =@ps
						group by 1,2,3) gs  on gs.s = mi.psource and gs.uid = m.uid and gs.cur = md.currency
					-- DEP FREQUENCY (based on total Lifetime) -- --------------------------------------------------------------------------------------------
					left join
							(select psource, uid, currency, timestampdiff(day, min(stdate), max(stdate)) / count(1) dep_freq
							from transactions_miliseconds
							where psource = @ps and type = 'payin'
							group by 1,2,3) f on m.uid = f.uid and f.psource = m.psource and md.currency = f.currency
								-- -----------------------------------------------------------------------------------------------------------------
					-- GET REAL MIN_DEPS FOR CASH_BONUSES (DIFFERENCT APPROACH) --------------------------------------------------------------------
                    left join
                    (select raw.*, (avg(t.amt_usd/100)*3/4 + min(t.amt_usd/100)*1/4) CB_amt_usd
						 from

						(select a.*, md.stdate, md.enddate, md.ptype2, md.min_dep_usd, min(b.date) bdate
						from

							(select psource, mail_name, present_type, uid, currency
							from activations
							where psource = @ps
							-- and mail_name = @mname
							and present_type = 'CB') a

							left join min_deposits md on md.psource = a.psource and md.mail_name = a.mail_name and md.currency = a.currency and md.ptype = a.present_type

							left join bonuses b on b.uid = a.uid and b.type = md.ptype2 and b.date between md.stdate and md.enddate
							where b.psource = @ps  and b.type = 'cash_bonus'
							group by 1,2,3,4,5,6,7,8,9
						) raw

						left join transactions_miliseconds t on raw.uid = t.uid and raw.currency = t.currency and raw.psource = t.psource

						where  t.type = 'payin' and t.stdate between raw.stdate and raw.enddate

						 -- and raw.uid = '5119035677efcddda75aee36'
						 group by 1,2,3,4,5,6,7,8,9) CB

                         on CB.uid = m.uid and CB.psource = m.psource and CB.mail_name = mi.event_name  and CB.currency = a.currency and CB.present_type = mi.type

                    -- ---------------------------------------------------------------------------------------------------


					where mi.psource =@ps  and m.psource =@ps   and u.psource=@ps
                    and md.min_dep > 0

                    #and match(mi.event_name) against(@mname in boolean mode)
                    and mi.event_name like @mname
                    group by 1,2,3,4,5,6

			) raw

			 group by 1,2,3,4
		) TOTAL
) MT  -- MAIL TOTALS
group by 1,2,3 ) MT2


left join -- ---------------------------------------------------------
        -- PROMO HEADERS
        (
        select event, event_type, segment, user_count, open, no_of_act, theme, dep_sum_traced
         from promo_header
         where locale = 'Totals'
         and psource=@ps
         ) p
         on MT2.mail_name = p.event and p.event_type = MT2.ptype

left join  -- --------------------------------------------------------------
         -- MIN DEPOSITS USD
          (
			select mail_name, ptype, min_dep, no_of_fs
           from min_deposits
           where psource=@ps and currency = 'USD' and min_dep > 0) md2
		on MT2.mail_name = md2.mail_name and MT2.ptype = md2.ptype
'''


def get_mailings(date, ps, report):  # type = ["weekly", "monthly"]
    conn = connect(host='localhost', user='root', password=get_pass(), db='mailing', charset='utf8mb4',
                   cursorclass=cursors.DictCursor)
    c = conn.cursor()
    sql = {"weekly": '''select distinct event
        from promo_header
        where sent_date between
        date(str_to_date('11-02-2016','%d-%m-%Y') + interval (6 - weekday(str_to_date('11-02-2016','%d-%m-%Y'))) day
        - interval 7 day) - interval 4 week + interval 1 day
        and date(str_to_date('11-02-2016','%d-%m-%Y') + interval (6 - weekday(str_to_date('11-02-2016','%d-%m-%Y'))) day
        - interval 7 day)
        and locale = 'totals' and psource = @ps order by sent_date desc;''',

        "monthly":  '''select distinct event
        from promo_header
        where sent_date between last_day(date_add(str_to_date('11-02-2016','%d-%m-%Y'), interval -5 month)) + interval 1 day and
        last_day(date_add(str_to_date('11-02-2016','%d-%m-%Y'), interval -1 month))
        and locale = 'totals' and psource = @ps order by sent_date desc;'''
           }
    query = sql.get(report)
    query = str.replace(query, '11-02-2016', date)
    query = str.replace(query, '@ps', str(ps))
    #  print(sql)
    c.execute(query)
    r = c.fetchall()
    output = []
    for elem in r:
        output.append(elem['event'])
    return output


def get_one_mailing_stats(event_name, time_frame, ps):
    #  Connect to the database 'mailing'
    conn = connect(host='localhost', user='root', password=get_pass(), db='mailing', charset='utf8mb4',
                   cursorclass=cursors.DictCursor)
    c = conn.cursor()
    sql = get_query().replace('@ps', str(ps))
    sql = sql.replace('@mname', "'"+event_name+"'")
    sql = sql.replace('@time_frame', str(time_frame))
    c.execute(sql)
    #  print("query executed...")
    return c.fetchall()


st_time = time.time()
psource = 1 # casino ID (1= Casino-X, 2 = JoyCasino)
report_type = 'weekly' #  weekly or monthly (last 4 weeks, last 4 month)
cur_date = '30-04-2017'  # to get stats for prev weeks/months put ANY data of CURRENT week/month
path = "G:\DUMP_ALL\mail_stats_psource"

mailings = get_mailings(cur_date, psource, report_type)  #  for prev week stats, insert ANY datetime of current week, say 'dd-mm-yyyy'

headers = ['stdate', 'parent', 'mail_name', 'ptype', 'segment', 'min_dep_usd', 'no_of_fs', 'user_count',
           'open', 'act_cnt', 'true_act', 'auto_act', 'act_deps_usd', 'min_deps_usd', 'true_effect_usd', 'nact_balance',
           'nact_maxdep', 'nact_empty', 'nact_early', 'nact_awol', 'nact_other', 'awol 0', 'awol 1-7',
           'awol 8-14', 'awol 15-30', 'awol 30+', 'theme', 'dep_sum_traced']

with open(path + str(psource) + "_" + report_type + '_' + str(datetime.utcnow())[0:11]
                  + ".csv", 'w') as f:
    cw = csv.writer(f, delimiter=';', dialect='excel', lineterminator='\n')
    cw.writerow(headers)
    data = [""]*len(headers)
    count = 0
    for m in mailings:
        t = get_one_mailing_stats(m, 30, psource)
        if len(t) and len(t[0]):
            print("writing " + m + " info...")
            for k in t[0]:
                data[headers.index(k)] = str(t[0].get(k))
            cw.writerow(data)
            count += 1
        else:
            print("no info for " + m)

print("%s / %s mailings were parsed" % (count, len(mailings)))
print('elapsed time: %s seconds' % str(time.time() - st_time))
