--黑名单
seLECT cnn, count(distinct udid)
FROM dwb_v8sp.client_log
WHERE 
    cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
    and cnn != ''
    AND cnn != '_oppo'
    and plat = '1'
    and dt >= '2018-12-01'
    and dt <= '2018-12-20'
group by cnn--黑名单


--白名单
seLECT cnn, count(distinct udid)
FROM dwb_v8sp.client_log
WHERE 
    cnn = '6385e22aa2a95b3779af2701c98a4478_yyb'
    and udid not in (
SELECT udid
    FROM dwb_v8sp.client_log
    WHERE cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
        and plat = '1'
        and dt >= '2018-12-01'
)
    and plat = '1'
    and dt >= '2018-12-01'
    and dt <= '2018-12-20'
group by cnn--白名单

--渠道IP交叉
SELECT t1.http_x_forwarded_for, t1.cnn, t2.pcid as pcid, count(distinct t1.udid)
from
    (
seLECT dt, cnn, http_x_forwarded_for, udid
    FROM dwd_v8sp.event_column_info
    WHERE 
    cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
        and cnn != ''
        and udid not in (
SELECT udid
        FROM dwd_v8sp.event_column_info
        WHERE cnn = '6385e22aa2a95b3779af2701c98a4478_yyb'
            and plat = '1'
            and dt >= '2018-12-01'
)
        and plat = '1'
        and dt >= '2018-12-01'
)as t1
    join (
SELECT pcid, udid
    FROM dim_v8sp.device_info
    where add_time >= '2018-12-01T00:00:00'
        AND plat = '1'
)as t2
    on t1.udid = t2.udid
group by pcid, t1.cnn, t1.http_x_forwarded_for--渠道IP交叉

--黑名单机型
SELECT manufacture, cnn, count(distinct udid)
from dwd_v8sp.event_column_info
where 
cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
    and cnn != ''
    AND cnn != '_oppo'
    and plat = '1'
GROUP by cnn, manufacture--黑名单机型

--黑名单渠道
SELECT t1.cnn, count(distinct t1.udid)
from
    (
seLECT dt, cnn, udid
    FROM dwb_v8sp.client_log
    WHERE 
    cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
        and cnn != ''
        and cnn != '_oppo'
        and plat = '1'
        and dt >= '2018-12-01'
        and dt <= '2018-12-20'
)as t1
    join (
SELECT pcid, udid
    FROM dim_v8sp.device_info
    where
    --add_time >= '2018-11-01T00:00:00'--AND 
    plat = '1'
)as t2
    on t1.udid = t2.udid
group by   t1.cnn
--黑名单渠道


-- 黑名单播放时长
seLECT sum(t1.duration), count(distinct t1.udid), t2.cnn,t1.dt
from
    (seLECT DISTINCT udid, cnn
    from dwb_v8sp.client_log
    where plat = '1'
        and imei=''
        and cnn != ''
        and cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
        and cnn != '_oppo'
        and dt <='2018-12-20'
        and dt >='2018-12-01'
)t2
    JOIN
    (seLECT udid, duration,dt
    from dwb_v8sp.event_column_info_new
    where source = 'android'
        -- and dt >='2018-12-01'
        and event = 'video_over'  --必须要加对应的event
    and dt <='2018-12-26'
    and dt >='2018-12-01'
)t1
    on t1.udid = t2.udid
GROUP by  t2.cnn,t1.dt


-- 查找黑名单用户的注册时间

SELECT t2.add_time,t1.cnn,count(distinct t1.udid)
from 
    (
SELECT cnn ,udid
    FROM dwb_v8sp.client_log
    where cnn != ''
        and cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
        and cnn != '_oppo'
        and plat = '1'
        and dt >='2018-12-01'
        and dt <='2018-12-20')t1
JOIN
(
SELECT substring(add_time,1,15)as add_time,udid
from dim_v8sp.device_info
where plat='1'
and substring(add_time,1,10)>='2018-11-01'
    ) t2 
     on t1.udid = t2.udid
GROUP by t2.add_time , t1.cnn

-- 应用启动次数
SELECT t2.dt
    sum(t2.pv)as pv,
    count( t1.udid) as uv
from 
    (
SELECT distinct udid
    FROM dwb_v8sp.client_log
    where cnn != ''
        and cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
        and cnn != '_oppo'
        and plat = '1'
        and dt >='2018-12-01'
        and dt <='2018-12-20')t1
JOIN
    (
    SELECT count(*)as pv,udid
    from dwb_v8sp.event_column_info_new
    where source='android'  and 
        event = 'app_start' and 
        dt >= '2018-12-01'
    GROUP BY udid 
    ) t2 
    on t1.udid = t2.udid
GROUP by t2.dt

-- 评论渗透率
SELECT
    t2.dt,t1.imei
    
from
    (
        seLECT
            
            distinct udid,imei
        FROM
            dwb_v8sp.client_log
        WHERE
            cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
            -- and imei = ''
            and cnn != ''
            and cnn != '_oppo'
            and plat = '1'
            and dt >= '2018-12-01'
            and dt <= '2018-12-20'
    ) as t1
    join (
        SELECT
            dt,
            udid,
            count(*) as pv
        FROM
            dwb_v8sp.event_column_info_new
        where
            dt >= '2018-12-01'
            and dt <= '2018-12-25'
            and event = 'post_comment'
        group by dt,udid
    ) as t2 on t1.udid = t2.udid
-- where t2.pv != 0
group by
    t2.dt,t1.imei


-- 新增留存
select
    t.dt as dt,(
        case when t.diffday = 0 then '0_day' when t.diffday = 1 then '1_day' when t.diffday = 2 then '2_day' when t.diffday = 3 then '3_day' when t.diffday = 4 then '4_day' when t.diffday = 5 then '5_day' when t.diffday = 6 then '6_day' when t.diffday = 7 then '7_day' else null end
    ) as RETENTION_DAYS,
    count(distinct t.udid) as RETENTION_USERS
from
    (
        select
            t2.dt as dt,
            t1.udid as udid,
            datediff(t1.dt, t2.dt) as diffday
        from
            (
                seLECT
                    udid,
                    dt
                FROM
                    dwb_v8sp.client_log
                WHERE
                    cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
                    and cnn != ''
                    and cnn != '_oppo'
                    and plat = '1'
                    and dt >= '2018-12-01'
                    and dt <= '2018-12-20'
            ) t1
            join (
                select
                    substring(add_time, 1, 10) as dt,
                    udid as udid
                from
                    dim_v8sp.device_info
                WHERE
                    substring(add_time, 1, 10) >= '2018-12-01'
                    and substring(add_time, 1, 10) <= '2018-12-25'
                    and plat = 1
                group by
                    substring(add_time, 1, 10),
                    udid
            ) t2 on t1.udid = t2.udid
    ) t
where
    diffday >= 0
    and diffday <= 7
group by
    t.dt,
    (
        case when t.diffday = 0 then '0_day' when t.diffday = 1 then '1_day' when t.diffday = 2 then '2_day' when t.diffday = 3 then '3_day' when t.diffday = 4 then '4_day' when t.diffday = 5 then '5_day' when t.diffday = 6 then '6_day' when t.diffday = 7 then '7_day' else null end
    )
order by
    dt;