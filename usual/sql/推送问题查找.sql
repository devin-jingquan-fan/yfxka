# 分版本
select
    t.dt as dt,
    t.brand as brand,
    t.dau as dau,
    t.cnt_push_video_receive as cnt_push_video_receive,
    t3.cnt_push_video_clk as cnt_push_video_clk
from
    (
        select
            t1.dt as dt,
            t1.brand as brand,
            t1.dau as dau,
            t2.cnt_push_video_receive as cnt_push_video_receive
        from
            (
                select
                    dt,
                    brand,
                    count(distinct udid) as dau
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2018-12-28'
                    and dt <= '2019-01-02'
                    and source = 'android'
                    and brand in (
                        'XIAOMI',
                        'VIVO',
                        'SAMSUNG',
                        'OPPO',
                        'MEIZU',
                        'HUAWEI',
                        'HONOR',
                        'GIONEE'
                    )
                    and event = 'app_start'
                group by
                    dt,
                    brand
            ) t1
            join (
                select
                    dt,
                    brand,
                    count(distinct udid) as cnt_push_video_receive
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2018-12-28'
                    and dt <= '2019-01-02'
                    and source = 'android'
                    and brand in (
                        'XIAOMI',
                        'VIVO',
                        'SAMSUNG',
                        'OPPO',
                        'MEIZU',
                        'HUAWEI',
                        'HONOR',
                        'GIONEE'
                    )
                    and event = 'push_video_receive' #（activity）
                group by
                    dt,
                    brand
            ) t2 on t1.dt = t2.dt
            and t1.brand = t2.brand
    ) t
    join (
        select
            dt,
            brand,
            count(distinct udid) as cnt_push_video_clk
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-28'
            and dt <= '2019-01-02'
            and source = 'android'
            and brand in (
                'XIAOMI',
                'VIVO',
                'SAMSUNG',
                'OPPO',
                'MEIZU',
                'HUAWEI',
                'HONOR',
                'GIONEE'
            )
            and event = 'push_video_clk'
        group by
            dt,
            brand
    ) t3 on t.dt = t3.dt
    and t.brand = t3.brand --     ## 新用户推送拉起情况
    --     ##dau
select
    t1.dt as dt,
    count(distinct t1.udid) as dau
from
    (
        select
            dt,
            udid
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-28'
            and dt <= '2019-01-02'
            and source = 'android'
            and event = 'app_start'
            and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
    ) t1
    join (
        select
            substring(add_time, 1, 10) as dt,
            udid
        from
            dim_v8sp.device_info
        where
            substring(add_time, 1, 10) >= '2018-12-28'
            and substring(add_time, 1, 10) <= '2019-01-02'
    ) t2 on t1.udid = t2.udid
    and t1.dt = t2.dt
group by
    t1.dt --     ## push_video_receive
select
    t1.dt as dt,
    count(distinct t1.udid) as push_video_receive
from
    (
        select
            dt,
            udid
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-28'
            and dt <= '2019-01-02'
            and source = 'android'
            and event = 'push_video_receive'
            and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
    ) t1
    join (
        select
            substring(add_time, 1, 10) as dt,
            udid
        from
            dim_v8sp.device_info
        where
            substring(add_time, 1, 10) >= '2018-12-28'
            and substring(add_time, 1, 10) <= '2019-01-02'
    ) t2 on t1.udid = t2.udid
    and t1.dt = t2.dt
group by
    t1.dt --     ## push_video_clk
select
    t1.dt as dt,
    count(distinct t1.udid) as push_video_clk
from
    (
        select
            dt,
            udid
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-28'
            and dt <= '2019-01-02'
            and source = 'android'
            and event = 'push_video_clk'
            and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
    ) t1
    join (
        select
            substring(add_time, 1, 10) as dt,
            udid
        from
            dim_v8sp.device_info
        where
            substring(add_time, 1, 10) >= '2018-12-28'
            and substring(add_time, 1, 10) <= '2019-01-02'
    ) t2 on t1.udid = t2.udid
    and t1.dt = t2.dt
group by
    t1.dt 
    --     ## 所有用户推送拉起情况
select
    t.dt as dt,
    t.dau as dau,
    t3.cnt_push_video_receive as cnt_push_video_receive,
    t.cnt_push_video_clk as cnt_push_video_clk
from
    (
        select
            t1.dt as dt,
            t1.dau as dau,
            t2.cnt_push_video_clk as cnt_push_video_clk
        from
            (
                select
                    dt,
                    count(distinct udid) as dau
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2018-12-28'
                    and dt <= '2019-01-02'
                    and source = 'android'
                    and event = 'app_start'
                    and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
                group by
                    dt
            ) t1
            join (
                select
                    dt,
                    count(distinct udid) as cnt_push_video_clk
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2018-12-28'
                    and dt <= '2019-01-02'
                    and source = 'android'
                    and event = 'push_video_clk'
                    and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
                group by
                    dt
            ) t2 on t1.dt = t2.dt
    ) t
    join (
        select
            dt,
            count(distinct udid) as cnt_push_video_receive
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-28'
            and dt <= '2019-01-02'
            and source = 'android'
            and event = 'push_video_receive'
            and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
        group by
            dt
    ) t3 on t.dt = t3.dt
select
    t.dt as dt,
    t.dau as dau,
    t.vapp,
    t3.cnt_push_video_receive as cnt_push_video_receive,
    t.cnt_push_video_clk as cnt_push_video_clk
from
    (
        select
            t1.dt as dt,
            t1.dau as dau,
            t1.vapp as vapp,
            t2.cnt_push_video_clk as cnt_push_video_clk
        from
            (
                select
                    dt,
                    vapp,
                    count(distinct udid) as dau
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2018-12-28'
                    and dt <= '2019-01-02'
                    and source = 'android'
                    and event = 'app_start'
                    and brand not in ('HUAWEI', 'HONOR')
                group by
                    dt,
                    vapp
            ) t1
            join (
                select
                    dt,
                    vapp,
                    count(distinct udid) as cnt_push_video_clk
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2018-12-28'
                    and dt <= '2019-01-02'
                    and source = 'android'
                    and event = 'push_video_clk'
                    and brand not in ('HUAWEI', 'HONOR')
                group by
                    dt,
                    vapp
            ) t2 on t1.dt = t2.dt
            and t1.vapp = t2.vapp
    ) t
    join (
        select
            dt,
            vapp,
            count(distinct udid) as cnt_push_video_receive
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-28'
            and dt <= '2019-01-02'
            and source = 'android'
            and event = 'push_video_receive'
            and brand not in ('HUAWEI', 'HONOR')
        group by
            dt,
            vapp
    ) t3 on t.dt = t3.dt
    and t.vapp = t3.vapp

#推送拉起率

select
    t1.dt as dt,
    t1.dau as dau,
    t1.vapp as vapp,
    t2.cnt_push_video_clk as `拉起人数`
from
    (
        select
            dt,
            vapp,
            count(distinct udid) as dau
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-20'
            and dt <= '2019-01-02'
            and source = 'android'
            and event = 'app_start'
            and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
        group by
            dt,
            vapp
    ) t1
    join (
        select
            dt,
            vapp,
            count(distinct udid) as cnt_push_video_clk
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-20'
            and dt <= '2019-01-02'
            and source = 'android'
            and event = 'app_start'
            and body_source = 'push'
            and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
        group by
            dt,
            vapp
    ) t2 on t1.dt = t2.dt
    and t1.vapp = t2.vapp

#一次性全部查找


    select
    t1.dt as dt,
    sum(t1.dau) as dau,
    t1.vos as vos,
    sum(t2.cnt_push_video_clk) as `拉起人数`,
    case t1.brand  when 'HUAWEI' then '华为'
                        when 'HONOR' then '华为'
                        when 'XIAOMI' then '小米'
                        ELSE '其他' END
from
    (
        select
            dt,
            vos,
            count(distinct udid) as dau ,
            brand 
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-31'
            and dt <= '2019-01-03'
            and source = 'android'
            and event = 'app_start'
            -- and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
            and vapp = '2740'
            -- and vos in ('4.4.4','5.1','5.1.1','6.0','6.0.1','7.1.1','8.1.0')
        group by
            dt,
            vos,
            brand
    ) t1
    join (
        select
            dt,
            vos,
            count(distinct udid) as cnt_push_video_clk,
            brand
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-31'
            and dt <= '2019-01-03'
            and source = 'android'
            and event = 'app_start'
            and body_source = 'push'
            -- and brand not in ('HUAWEI', 'XIAOMI', 'HONOR')
            and vapp ='2740'
            -- and vos in ('4.4.4','5.1','5.1.1','6.0','6.0.1','7.1.1','8.1.0')
        group by
            dt,
            vos,
            brand
    ) t2 on t1.dt = t2.dt
    and t1.vos = t2.vos
    and t1.brand = t2.brand

group by  t1.dt ,t1.vos ,
case t1.brand  when 'HUAWEI' then '华为'
                        when 'HONOR' then '华为'
                        when 'XIAOMI' then '小米'
                        ELSE '其他' END



                    