SELECT
    dt as DATA_DATE,
    case when t2.name is not null then t2.name when substring(author_id, 1, 5) = '10079' then 'ins' when substring(author_id, 1, 5) = '10395' then '机构-烟台' when substring(author_id, 1, 5) = '10372' then 'kuaishou' when author_id = '326' then 'fb' else '微信群' end as AUTHOR,
    count(
        distinct case when lower(event) not like '%push%'
        and event != 'corner_mark_show' then udid else null end
    ) as DAU,
    count(
        case when event = 'client_show' then 1 else null end
    ) as TOTAL_VSHOW,
    count(
        distinct case when event = 'client_show' then vid else null end
    ) as TOTAL_VIDEO_VSHOW,
    count(
        case when event = 'video_play' then 1 else null end
    ) as TOTAL_VV_VP,
    count(
        distinct case when event = 'video_play' then udid else null end
    ) as TOTAL_USERS_VP,
    count(
        case when event = 'effective_play' then 1 else null end
    ) as TOTAL_VV_EP,
    count(
        distinct case when event = 'effective_play' then udid else null end
    ) as TOTAL_USERS_EP,
    sum(
        case when event = 'video_over' then duration else 0 end
    ) as TOTAL_DURATION,
    count(
        case when event = 'video_over' then 1 else null end
    ) as TOTAL_VOVER,
    sum(
        case when event = 'video_over' then play_cnts else 0 end
    ) as TOTAL_VOVER_PCNTS,
    count(
        case when event = 'push_video_clk' then 1 else null end
    ) as TOTAL_PUSH_VC,
    count(
        distinct case when event = 'app_start'
        and lower(body_source) = 'push' then udid else null end
    ) as TOTAL_PUSH_START,
    count(
        case when event = 'post_comment' then 1 else null end
    ) as TOTAL_REPLY,
    count(
        distinct case when event = 'post_comment' then udid else null end
    ) as TOTAL_USERS_REPLY
FROM
    (
        select
            *
        FROM
            dwb_v8sp.event_column_info_new_hour
        WHERE
            dt = '${date}'
            and source = 'android'
            and pname = 'com.melon.lazymelon'
            and substring(author_id, 1, 1) != '2'
            and author_type != 1
    ) t1
    left join (
        select
            distinct authorid,
            name
        from
            dwd_v8sp.author_info
    ) t2 on t1.author_id = t2.authorid
group by
    dt,
    case when t2.name is not null then t2.name when substring(author_id, 1, 5) = '10079' then 'ins' when substring(author_id, 1, 5) = '10395' then '机构-烟台' when substring(author_id, 1, 5) = '10372' then 'kuaishou' when author_id = '326' then 'fb' else '微信群' end
SELECT
    dt as DATA_DATE,
    'ugc' as AUTHOR,
    count(
        distinct case when lower(event) not like '%push%'
        and event != 'corner_mark_show' then udid else null end
    ) as DAU,
    count(
        case when event = 'client_show' then 1 else null end
    ) as TOTAL_VSHOW,
    count(
        distinct case when event = 'client_show' then vid else null end
    ) as TOTAL_VIDEO_VSHOW,
    count(
        case when event = 'video_play' then 1 else null end
    ) as TOTAL_VV_VP,
    count(
        distinct case when event = 'video_play' then udid else null end
    ) as TOTAL_USERS_VP,
    count(
        case when event = 'effective_play' then 1 else null end
    ) as TOTAL_VV_EP,
    count(
        distinct case when event = 'effective_play' then udid else null end
    ) as TOTAL_USERS_EP,
    sum(
        case when event = 'video_over' then duration else 0 end
    ) as TOTAL_DURATION,
    count(
        case when event = 'video_over' then 1 else null end
    ) as TOTAL_VOVER,
    sum(
        case when event = 'video_over' then play_cnts else 0 end
    ) as TOTAL_VOVER_PCNTS,
    count(
        case when event = 'push_video_clk' then 1 else null end
    ) as TOTAL_PUSH_VC,
    count(
        distinct case when event = 'app_start'
        and lower(body_source) = 'push' then udid else null end
    ) as TOTAL_PUSH_START,
    count(
        case when event = 'post_comment' then 1 else null end
    ) as TOTAL_REPLY,
    count(
        distinct case when event = 'post_comment' then udid else null end
    ) as TOTAL_USERS_REPLY
FROM
    dwb_v8sp.event_column_info_new_hour
WHERE
    dt = '${date}'
    and source = 'android'
    and pname = 'com.melon.lazymelon'
    and author_type = 1
    and vid not in (
        select
            distinct videoid
        from
            dwb_v8sp.talent_audit_info
    )
group by
    dt ## 启动次数
SELECT
    dt,
    hour,
    body_source,
    count(
        distinct case when event = 'app_start' then udid else null end
    ) as ` 启动人数`,
    count(
        case when event = 'app_start' then 1 else null end
    ) as `启动次数`
from
    dwb_v8sp.event_column_info_new_hour
where
    dt = '${date}'
    and source = 'android'
    and pname = 'com.melon.lazymelon'
group by
    dt,
    hour,
    body_source
SELECT
    dt,
    category_id,
    count(
        DISTINCT case WHEN event = 'client_show' then udid ELSE NULL END
    ) as dau,
    count(
        DISTINCT case when event = 'audio_layer_show'
        and lower(body_source) = 'listen' then udid else null end
    ) as `zhudong`,
    count(
        DISTINCT case when event = 'audio_layer_show'
        and lower(body_source) = 'auto' then udid else null end
    ) as `zongji`,
    sum(
        case WHEN event = 'audio_over' then duration ELSE NULL END
    ) as `播放时长`,
    count(
        CASE WHEN event = 'audio_over' then 1 ELSE NULL end
    ) as `播放条数`
from
    dwb_v8sp.event_column_user_type
where
    user_type = 'new'
    and dt <= '2019-01-13'
    and dt >= '2019-01-10'
group by
    dt,
    category_id
SELECT
    t2.dt,
    t2.udid,
    t2.brand,
    t2.pcid_origin,
    t2.model,
    t2.pv,
    t1.duration
from
    (
        SELECT
            dt,
            udid,
            sum(duration) as duration
        from
            dwb_v8sp.event_column_user_type
        where
            dt = '2019-01-08'
            and event = 'video_over'
            and user_type = 'new'
            and udid not in (
                SELECT
                    udid
                from
                    dwb_v8sp.event_column_user_type
                WHERE
                    dt <= '2019-01-15'
                    and dt >= '2019-01-08'
                    and user_type = 'old'
            )
        GROUP BY
            udid,
            dt
    ) as t1
    join (
        SELECT
            dt,
            udid,
            brand,
            pcid_origin,
            model,
            count(1) as pv
        from
            dwb_v8sp.event_column_user_type
        where
            dt >= '2019-01-09'
            and dt <= '2019-01-15'
            and user_type = 'old'
            and event in (
                'push_video_receive',
                'push_video_show',
                'push_message_receive',
                'push_message_show'
            )
        GROUP BY
            udid,
            dt,
            brand,
            pcid_origin,
            model
    ) as t2 on t1.udid = t2.udid
WHERE
    t1.duration > 1200 -- limit 100
select
    *
from
    (
        select
            udid,
            dt,
            sum(duration) as duration
        from
            dwb_v8sp.event_column_user_type
        where
            udid not in (
                select
                    udid
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2019-01-09'
                    and dt <= '2019-01-15'
                    and event = 'app_start'
                group by
                    udid
            )
            and dt = '2019-01-08'
            and user_type = 'new'
    ) t1
    join (
        select
            udid,
            pcid_origin,
            brand,
            model,
            dt,
            count(1) as pv
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2019-01-09'
            and dt <= '2019-01-15'
            and event in (
                'push_video_receive',
                'push_video_show',
                'push_message_receive',
                'push_message_show'
            )
        group by
            udid,
            pcid_origin,
            brand,
            model,
            dt
    ) t2 on t1.udid = t2.udid 
    
    
    
    -- ###########################
select
    t.dt as DATA_DATE,
    case when t.diffday = 0 then t.channel_current else t.channel end as CHANNEL,
    case when t.diffday = 0 then 'd0' when t.diffday = 1 then 'd1' when t.diffday = 2 then 'd2' when t.diffday = 3 then 'd3' when t.diffday = 4 then 'd4' when t.diffday = 5 then 'd5' when t.diffday = 6 then 'd6' when t.diffday = 7 then 'd7' when t.diffday = 8 then 'd8' when t.diffday = 9 then 'd9' when t.diffday = 10 then 'd10' when t.diffday = 11 then 'd11' when t.diffday = 12 then 'd12' when t.diffday = 13 then 'd13' when t.diffday = 14 then 'd14' when t.diffday = 15 then 'd15' when t.diffday = 16 then 'd16' when t.diffday = 17 then 'd17' when t.diffday = 18 then 'd18' when t.diffday = 19 then 'd19' when t.diffday = 20 then 'd20' when t.diffday = 21 then 'd21' when t.diffday = 22 then 'd22' when t.diffday = 23 then 'd23' when t.diffday = 24 then 'd24' when t.diffday = 25 then 'd25' when t.diffday = 26 then 'd26' when t.diffday = 27 then 'd27' when t.diffday = 28 then 'd28' when t.diffday = 29 then 'd29' when t.diffday = 30 then 'd30' else null end as RETENTION_DAYS,
    '${date}' as `INSERT_DATE`,
    count(distinct t.udid) as USERS
from
    (
        select
            b.dt as dt,
            a.channel as channel,
            a.channel_current as channel_current,
            datediff(a.dt, b.dt) as diffday,
            a.udid as udid
        from
            (
                select
                    distinct dt,
                    pcid_origin as channel,
                    udid as udid,
                    pcid_current as channel_current
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt = '${date}'
                    and source = 'android'
                    and pname = 'com.melon.lazymelon'
                    and lower(event) not like '%push%'
                    and event != 'corner_mark_show'
            ) a
            join (
                select
                    substring(add_time, 1, 10) as dt,
                    udid as device_udid
                from
                    dim_v8sp.device_info
                where
                    substring(add_time, 1, 10) >= '${d30before}'
            ) b on a.udid = b.device_udid
    ) t
where
    t.diffday >= 0
    and t.diffday <= 30
group by
    t.dt,
    case when t.diffday = 0 then t.channel_current else t.channel end,
    case when t.diffday = 0 then 'd0' when t.diffday = 1 then 'd1' when t.diffday = 2 then 'd2' when t.diffday = 3 then 'd3' when t.diffday = 4 then 'd4' when t.diffday = 5 then 'd5' when t.diffday = 6 then 'd6' when t.diffday = 7 then 'd7' when t.diffday = 8 then 'd8' when t.diffday = 9 then 'd9' when t.diffday = 10 then 'd10' when t.diffday = 11 then 'd11' when t.diffday = 12 then 'd12' when t.diffday = 13 then 'd13' when t.diffday = 14 then 'd14' when t.diffday = 15 then 'd15' when t.diffday = 16 then 'd16' when t.diffday = 17 then 'd17' when t.diffday = 18 then 'd18' when t.diffday = 19 then 'd19' when t.diffday = 20 then 'd20' when t.diffday = 21 then 'd21' when t.diffday = 22 then 'd22' when t.diffday = 23 then 'd23' when t.diffday = 24 then 'd24' when t.diffday = 25 then 'd25' when t.diffday = 26 then 'd26' when t.diffday = 27 then 'd27' when t.diffday = 28 then 'd28' when t.diffday = 29 then 'd29' when t.diffday = 30 then 'd30' else null end



select
    t.dt as DATA_DATE,
    case when t.diffday = 0 then 'd0' when t.diffday = 1 then 'd1' when t.diffday = 2 then 'd2' when t.diffday = 3 then 'd3' when t.diffday = 4 then 'd4' when t.diffday = 5 then 'd5' when t.diffday = 6 then 'd6' when t.diffday = 7 then 'd7' when t.diffday = 8 then 'd8' when t.diffday = 9 then 'd9' when t.diffday = 10 then 'd10' when t.diffday = 11 then 'd11' when t.diffday = 12 then 'd12' when t.diffday = 13 then 'd13' when t.diffday = 14 then 'd14' when t.diffday = 15 then 'd15' when t.diffday = 30 then 'd30' else null end as RETENTION_DAYS,
    '${date}' as `INSERT_DATE`,
    count(distinct t.udid) as USERS
from
    (
        select
            b.dt as dt,
            datediff(a.dt, b.dt) as diffday,
            a.udid as udid
        from
            (
                select
                    distinct dt,
                    udid as udid
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt = '${date}'
                    and source = 'android'
                    and pname = 'com.melon.lazymelon'
                    and lower(event) not like '%push%'
                    and event != 'corner_mark_show'
            ) a
            join (
                select
                    substring(add_time, 1, 10) as dt,
                    udid as device_udid
                from
                    dim_v8sp.device_info
                where
                    substring(add_time, 1, 10) >= '${d30before}'
            ) b on a.udid = b.device_udid
    ) t
where
    t.diffday <= 15
    or t.diffday = 30
group by
    t.dt,
    case when t.diffday = 0 then 'd0' when t.diffday = 1 then 'd1' when t.diffday = 2 then 'd2' when t.diffday = 3 then 'd3' when t.diffday = 4 then 'd4' when t.diffday = 5 then 'd5' when t.diffday = 6 then 'd6' when t.diffday = 7 then 'd7' when t.diffday = 8 then 'd8' when t.diffday = 9 then 'd9' when t.diffday = 10 then 'd10' when t.diffday = 11 then 'd11' when t.diffday = 12 then 'd12' when t.diffday = 13 then 'd13' when t.diffday = 14 then 'd14' when t.diffday = 15 then 'd15' when t.diffday = 30 then 'd30' else null end





SELECT a.push as `推送拉起`,(a.liucun - a.push) as `非推送拉起`,a.receive_push as `推送到达`,
a.no_receive_push as `非推送到达`,(b.new_user_count - a.liucun-a.no_receive_push-a.receive_push) as `无事件人数`
from
(    
    SELECT t2.dt,
    count(DISTINCT case WHEN event = 'app_start' and body_source = 'push'then  t1.udid ELSE NULL END  )as push,
    count(DISTINCT case WHEN event = 'app_start' and body_source != 'push' 
        and event not in ('push_video_clk','push_message_clk','push_category_clk','push_activity_clk')  then  t1.udid ELSE NULL END  )as nopush,
    count(DISTINCT case WHEN  body_source != 'push' 
        and event not in ('corner_mark_show','push_video_clk','push_message_clk','push_category_clk','push_activity_clk')  then  t1.udid ELSE NULL END  )as nopush1,
    count(DISTINCT case WHEN event !='corner_mark_show' and lower(event) not like '%push%' 
        then  t1.udid ELSE NULL END  )as liucun,
    -- count(DISTINCT case WHEN event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive') 
    --     and event !='app_start' then  t1.udid ELSE NULL END  )as receive_push,
    -- count(DISTINCT case WHEN event != 'app_start' and event not in 
    --     ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive') 
    --     then  t1.udid ELSE NULL END  )as no_receive_push
    count(DISTINCT case WHEN event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive') 
        -- and event !='app_start'
         then  t1.udid ELSE NULL END  )as receive_push,
    count(DISTINCT case WHEN  event not in 
        ('app_start','push_video_receive','push_message_receive','push_category_receive','push_activity_receive') 
        then  t1.udid ELSE NULL END  )as no_receive_push
    from
    (
    SELECT event ,udid ,body_source,dt
    from dwb_v8sp.event_column_user_type
    where dt = '2019-01-09'
    and user_type = 'old'
    and source ='android'
    GROUP BY event ,udid ,body_source,dt
    )t1 
    join
    (
        SELECT udid ,substring (add_time,1,10)as dt
        from dim_v8sp.device_info
        where substring (add_time,1,10) = '2019-01-08'
        and plat =1
        GROUP BY udid,substring (add_time,1,10)
    ) t2
    on t1.udid =t2.udid 
    GROUP BY t2.dt
)a 
join 
(
    SELECT substring (add_time,1,10) as dt,  count( DISTINCT udid) as new_user_count
    from dim_v8sp.device_info
    where substring (add_time,1,10) = '2019-01-08'
    and plat =1
    GROUP BY substring (add_time,1,10)
)b
on a.dt =b.dt



##推送相关1.0
SELECT a.push as `推送拉起`,(a.liucun - a.push) as `非推送拉起`,(a.receive_push - a.push) as `推送到达`,
a.no_receive_push as `非推送到达`,(b.new_user_count - a.liucun-a.no_receive_push-a.receive_push) as `无事件人数`
from
(    
    SELECT t2.dt,
    count(DISTINCT case WHEN event = 'app_start' and body_source = 'push'then  t1.udid ELSE NULL END  )as push,
    count(DISTINCT case WHEN event = 'app_start' and body_source != 'push' 
        and event not in ('push_video_clk','push_message_clk','push_category_clk','push_activity_clk')  then  t1.udid ELSE NULL END  )as nopush,
    count(DISTINCT case WHEN  body_source != 'push' 
        and event not in ('corner_mark_show','push_video_clk','push_message_clk','push_category_clk','push_activity_clk')  then  t1.udid ELSE NULL END  )as nopush1,
    count(DISTINCT case WHEN event !='corner_mark_show' and lower(event) not like '%push%' 
        then  t1.udid ELSE NULL END  )as liucun,
    -- count(DISTINCT case WHEN event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive') 
    --     and event !='app_start' then  t1.udid ELSE NULL END  )as receive_push,
    -- count(DISTINCT case WHEN event != 'app_start' and event not in 
    --     ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive') 
    --     then  t1.udid ELSE NULL END  )as no_receive_push
    count(DISTINCT case WHEN event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive') 
        and event !='app_start'
         then  t1.udid ELSE NULL END  )as receive_push,
    count(DISTINCT case WHEN  event not in 
        ('app_start','push_video_receive','push_message_receive','push_category_receive','push_activity_receive') 
        then  t1.udid ELSE NULL END  )as no_receive_push
    from
    (
    SELECT event ,udid ,body_source,dt
    from dwb_v8sp.event_column_user_type
    where dt = '2019-01-09'
    and user_type = 'old'
    and source ='android'
    GROUP BY event ,udid ,body_source,dt
    )t1 
    join
    (
        -- SELECT udid ,substring (add_time,1,10)as dt
        -- from dim_v8sp.device_info
        -- where substring (add_time,1,10) = '2019-01-08'
        -- and plat =1
        -- GROUP BY udid,substring (add_time,1,10)

        select udid ,dt ,sum(case when event ='video_over' then duration else null end )as duration
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-08'
        and source ='android'
        and user_type ='new'
        group by udid ,dt 

    ) t2
    on t1.udid =t2.udid 
    where t2.duration >1200
    GROUP BY t2.dt
)a 
join 
(
    SELECT substring (add_time,1,10) as dt,  count( DISTINCT udid) as new_user_count
    from dim_v8sp.device_info
    where substring (add_time,1,10) = '2019-01-08'
    and plat =1
    GROUP BY substring (add_time,1,10)
)b
on a.dt =b.dt







