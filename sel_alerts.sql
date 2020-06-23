select 
t5.site_name, 
  t5.device_type, 
  last_seen_local as last_seen, 
  if(
    t6.start_time is null, t5.last_seen_local, 
    t6.start_time
  ) as last_closed, 
  t5.ip_address, 
  if(
    last_seen_local > now() - interval 1 hour, 
    t5.mean, "no comm"
  ) as status 
from 
  (
    select 
      convert_tz(t3.max_clock, "UTC", t4.l_tz) as last_seen_local, 
      t3.mean, 
      t4.ip_address, 
      t4.site_name, 
      t3.device_type 
    from 
      (
        select 
          t1.max_clock, 
          t2.site_id, 
          t2.mean, 
          t1.device_type 
        from 
          (
            select 
              max(clock) as max_clock, site_id, 
              device_type, 
              type 
            from 
              sdsresults.ppc_events 
            where 
              type = "relay_status" 
              and device_type like "sel%" 
              and site_id != 593 
              and concat(device_type, site_id) != "sel611" 
            group by 
              site_id, 
              device_type
          ) t1 
          left join sdsresults.ppc_events t2 on t1.max_clock = t2.clock 
          and t1.site_id = t2.site_id 
          and t1.type = t2.type 
          and t1.device_type = t2.device_type
      ) t3 
      left join (
        select 
          site_id, 
          site_name, 
          l_tz, 
          concat(
            ip_address, 
            space(
              18 - length(ip_address)
            )
          ) as ip_address 
        from 
          sdsprod.002_devices_new 
        where 
          device_type = "meter" 
          and status = "ok" 
        group by 
          site_id, 
          site_name, 
          l_tz, 
          ip_address
      ) t4 on t3.site_id = t4.site_id
  ) t5 
  left join (
    select 
      start_time, 
      event_type, 
      device_type, 
      site_name, 
      site_id 
    from 
      sdsresults.sel_journal 
    where 
      status = "open" 
      and event_type = "sel_open"
  ) t6 on t5.site_name = t6.site_name 
  and t5.device_type = t6.device_type 
where 
  if(
    t5.site_name = "DPC Warren, WI", t5.device_type != "selA", 
    1 = 1
  ) 
order by 
  status desc;
