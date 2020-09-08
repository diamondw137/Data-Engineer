(SELECT 
    a.device_id, a.site_id, a.report_month
FROM
    (SELECT 
        d.site_id,
            mp.device_id,
            d.device_type,
            mp.type,
            mp.report_month
    FROM
        sdsprod.503_monthly_production mp
    JOIN devices d ON d.id = mp.device_id
    GROUP BY mp.report_month , d.id) a
        LEFT JOIN
    sdsresults.ppc_events ppc ON ppc.site_id = a.site_id)
