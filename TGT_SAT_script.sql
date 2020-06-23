select convert_tz(date_format(concat(date, ' ', intervals), '%Y-%m-%d %H:%i:%s'), 'UTC', 'US/Pacific') as clock_local, 'Medford, OR' as site_name, 
# EST US/Eastern (CT, FL, DE, GA, IN, KY, ME, MD, MS NH, NJ, NY, NC, OH, PA, RI, SC, TN, VR, VA, WV)
# CST US/Central (IA, IL, MO, AR, OK, TX, LA, ND, SD, WI, AL, MS, KS, MN, NE)
# GMT US/Pacific (WA, OR, NV, CA)
# UTC US/Mountain (MT, ID, WY, UT, CO, AZ, MN, NE, SD)

avg(case when type = 'active_power_total' then mean end) as meter_kw,
avg(case when type = 'GHI_irradiance (W/m2)' then mean end) as ws_poa,
avg(case when type = 'wx_cell_temp_1 (C)' then mean end) as ws_tbom,
avg(case when type = 'wx_amb_temp (C)' then mean end) as ws_tamb

from
(select * , date(clock) as date, sec_to_time(time_to_sec(clock)- time_to_sec(clock)%(15*60)) AS intervals 
from sdsresults.ppc_events
where site_id = 683

and type in ('wx_cell_temp_1 (C)', 'wx_amb_temp (C)', 'GHI_irradiance (W/m2)', 'active_power_total')
#and device_type = 'meter'
#order by clock desc limit 100
) t11s_raw006_siteID_old
group by date, intervals;
