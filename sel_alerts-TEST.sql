SELECT 
    t5.site_name,
    t5.site_id,
    t5.device_type,
    last_seen_local AS last_seen,
    IF(t6.start_time IS NULL,
        t5.last_seen_local,
        t6.start_time) AS last_closed,
    t5.ip_address,
    IF(last_seen_local > NOW() - INTERVAL 1 HOUR,
        t5.mean,
        'no comm') AS status
FROM
    (SELECT 
        CONVERT_TZ(t3.max_clock, 'UTC', t4.l_tz) AS last_seen_local,
            t3.mean,
            t4.ip_address,
            t4.site_name,
            t4.site_id,
            t3.device_type
    FROM
        (SELECT 
        t1.max_clock, t2.site_id, t2.mean, t1.device_type
    FROM
        (SELECT 
        MAX(clock) AS max_clock, site_id, device_type, type
    FROM
        sdsresults.ppc_events
    WHERE
        type = 'relay_status'
            AND device_type LIKE 'sel%'
            AND site_id != 807
            AND CONCAT(device_type, site_id) != 'sel807'
    GROUP BY site_id , device_type) t1
    LEFT JOIN sdsresults.ppc_events t2 ON t1.max_clock = t2.clock
        AND t1.site_id = t2.site_id
        AND t1.type = t2.type
        AND t1.device_type = t2.device_type) t3
    LEFT JOIN (SELECT 
        site_id,
            site_name,
            l_tz,
            CONCAT(ip_address, SPACE(18 - LENGTH(ip_address))) AS ip_address
    FROM
        sdsprod.002_devices_new
    WHERE
        device_type = 'meter' AND status = 'ok'
    GROUP BY site_id , site_name , l_tz , ip_address) t4 ON t3.site_id = t4.site_id) t5
        LEFT JOIN
    (SELECT 
        start_time, event_type, device_type, site_name, site_id
    FROM
        sdsresults.sel_journal
    WHERE
        status = 'open'
            AND event_type = 'sel_open') t6 ON t5.site_name = t6.site_name
        AND t5.device_type = t6.device_type
WHERE
    IF(t5.site_name = 'DPC Warren, WI'
            OR 'NJS02_HU',
        t5.device_type != 'selA',
        1 = 1)
ORDER BY status DESC;
