#503_monthly_meter v01                                                                                                                                                                            
#SELECT sid.site_name, a.device_type, sid.OEM, a.site_id, a.device_id, (a.max_clock) max_cl, a.type, (a.max_value), a.report_month, (a.count)                                                                                                                                                                            
set @m1 = 201912;                                                                                                                                                                          
set sql_mode = " ";  

(SELECT 
    CONCAT(DATE_FORMAT(MIN(a.max_clock), '%Y'),
            '_',
            DATE_FORMAT(MIN(a.max_clock), '%m'),
            '_',
            a.site_id) yyyy_mm_id,
    a.device_id,
    '' AS orig,
    DATE_FORMAT(MIN(a.max_clock), '%Y') year,
    sid.site_name,
    a.device_type,
    sid.OEM,
    '' AS signal_type,
    MAX(a.max_clock) max_cl,
    a.type,
	a.site_id,
    a.device_id as dev_id,
    a.device_id as did_adj,
    '' AS status,
    a.report_month,
    '' AS 'index',
	'' AS 'alt_prod',
	'' AS 'parked reading',
    '' AS 'cumla_prod kWh',
    '' AS 'meter offset kWh',
    '' AS 'cumla_prod kWh',
    FORMAT(SUM(a.max_value), '#') m_prod,
	'' AS 'notes',
	'' AS 'device_id check',
	'' AS 'site %prod',
	'' AS 'prev_month',
	'' AS 'yrs since COD',
	'' AS 'meter_reporting',
	'' AS 'meters required',
    COUNT(a.count) count,
    DAY(LAST_DAY(a.max_clock)) days_in_month,
    (max(a.max_clock)) meter_max_day,
    a.ip_address
FROM
    (SELECT 
        d.site_id,
            mp.device_id,
            d.device_type,
            mp.max_clock,
            mp.type,
            (MAX(ABS(mp.value))) max_value,
            mp.report_month,
            COUNT(mp.value) count,
            d.ip_address
    FROM
        sdsprod.503_monthly_production mp
    JOIN devices d ON d.id = mp.device_id
    WHERE
        mp.report_month = @m1
            AND mp.type IN ('Accumulated Real Energy Net (kWh)' ,
            'KWH_DEL', 
            'Real Eergy Import (KWHdel)',
            'Real Energy (KWHnet)', 
            'Total Energy Consumption (kWh)', 
            'W-hours, Delivered (kWh)', 
            'Total Export Energy (Revenue) (kWh)',
            'Real Energy Export (KWHrec)', 
            'Delivered Energy (kWh)', 
            'Total Import Energy (Revenue) (kWh)',
            'Net Total Energy (Revenue) (kWh)',
            'Total Import Energy (Revenue) (kWh)', 
            'Import Energy (kWh)')
    GROUP BY mp.report_month , d.id) a
        LEFT JOIN
    006_siteID_new sid ON sid.id = a.site_id
GROUP BY a.report_month , a.device_id , sid.site_name_2 , a.type)
