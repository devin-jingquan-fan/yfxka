-- 七日流失用户
SELECT dt,udid,event,count(1)as pv
from dwb_v8sp.event_column_user_type
where dt='2019-01-08'
and user_type ='new'
and udid not in (
    SELECT udid 
    from dwb_v8sp.event_column_user_type
    WHERE dt<='2019-01-15'
        and dt >='2019-01-08'
        and user_type = 'old'
        and event ='app_start'
)
GROUP BY udid,event,dt
ORDER BY event DESC
LIMIT 100

-- 七日连续留存用户
select t11.udid
from
(
    select t9.udid as udid
    from
    (
        SELECT t7.udid as udid 
        from
        (
            SELECT t5.udid as udid
            from 
            (
                SELECT t3.udid as udid
                from 
                (
                    SELECT t1.udid as udid
                    from 
                    (
                        SELECT udid
                        from dwb_v8sp.event_column_user_type
                        where dt='${date}'
                        and user_type ='new'
                        and udid  in (
                            SELECT udid 
                            from dwb_v8sp.event_column_user_type
                            WHERE dt= date_add('${date}',1)
                                and user_type = 'old'
                                and event ='app_start'
                            GROUP BY udid
                    )
                    GROUP BY udid
                    -- ORDER BY event DESC
                    LIMIT 1000)t1
                    join
                    (   
                        SELECT udid 
                        from dwb_v8sp.event_column_user_type
                        WHERE dt= date_add('${date}',2)
                            and user_type = 'old'
                            and event ='app_start'
                        GROUP BY udid
                        
                    )t2
                    on t1.udid =t2.udid
                )t3
                join
                (
                    SELECT udid 
                    from dwb_v8sp.event_column_user_type
                    WHERE dt= date_add('${date}',3)
                        and user_type = 'old'
                        and event ='app_start'
                    GROUP BY udid 
                )t4
                on t3.udid=t4.udid
            )t5
            join 
            (
            SELECT udid 
                from dwb_v8sp.event_column_user_type
                WHERE dt= date_add('${date}',4)
                    and user_type = 'old'
                    and event ='app_start'
                GROUP BY udid 
            )t6
            on t5.udid= t6.udid
        )t7
        join 
        (
            SELECT udid 
            from dwb_v8sp.event_column_user_type
            WHERE dt= date_add('${date}',5)
                and user_type = 'old'
                and event ='app_start'
            GROUP BY udid 
        )t8
        on t7.udid = t8.udid
    )t9
    join 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        WHERE dt= date_add('${date}',6)
            and user_type = 'old'
            and event ='app_start'
        GROUP BY udid 
    )t10
    on t9.udid=t10.udid
)t11
join 
(
    SELECT udid 
        from dwb_v8sp.event_column_user_type
        WHERE dt= date_add('${date}',7)
            and user_type = 'old'
            and event ='app_start'
        GROUP BY udid 
)t12
on t11.udid=t12.udid




SELECT dt,udid,count(1)as pv,sum(duration)as duration
from dwb_v8sp.event_column_user_type
where dt='2019-01-08'
event = 'video_over'
and user_type ='new'
and udid not in (
    SELECT udid 
    from dwb_v8sp.event_column_user_type
    WHERE dt<='2019-01-15'
        and dt >='2019-01-08'
        and user_type = 'old'
)
GROUP BY udid,dt
-- ORDER BY event DESC
-- LIMIT 100


##播放>20min的七日后推送到达情况
SELECT t1.udid,t2.dt,t2.brand,t2.pcid_origin,t2.model,t2.pv,t1.duration
from 
(
    SELECT dt,udid,sum(duration)as duration
    from dwb_v8sp.event_column_user_type
    where dt='2019-01-08'
    and event = 'video_over'
    and user_type ='new'
    and udid not in (
        SELECT DISTINCT udid 
        from dwb_v8sp.event_column_user_type
        WHERE dt<='2019-01-15'
            and dt >='2019-01-09'
            AND event = 'app_start'
            -- GROUP BY udid
            and user_type = 'old'
    )
    GROUP BY udid,dt
)as t1
join 
(
        SELECT dt,udid,brand,pcid_origin,model,count(1)as pv
    from dwb_v8sp.event_column_user_type
    where dt>='2019-01-09'
    and dt <='2019-01-15'
    and user_type ='old'
    -- and event in ('push_video_receive','push_message_show')
    and event in ('push_video_receive','push_message_receive')
    GROUP BY udid,dt,brand,pcid_origin,model
)as t2
on t1.udid = t2.udid
WHERE t1.duration >1200



#播放大于20min的用户的渠道机型
SELECT t1.udid,t2.dt,t2.brand,t2.pcid_origin,t2.model,t2.pv,t1.duration
from 
(
    SELECT dt,udid,sum(duration)as duration
    from dwb_v8sp.event_column_user_type
    where dt='2019-01-08'
    and event = 'video_over'
    and user_type ='new'
    and udid not in (
        SELECT DISTINCT udid 
        from dwb_v8sp.event_column_user_type
        WHERE dt<='2019-01-15'
            and dt >='2019-01-09'
            AND event = 'app_start'
            -- GROUP BY udid
            and user_type = 'old'
    )
    GROUP BY udid,dt
)as t1
join 
(
        SELECT dt,udid,brand,pcid_origin,model,count(1)as pv
    from dwb_v8sp.event_column_user_type
    where dt>='2019-01-08'
    -- and dt <='2019-01-15'
    -- and user_type ='old'
    and user_type='new'
    and event = 'app_start'
    -- and event in ('push_video_show','push_message_show')
    GROUP BY udid,dt,brand,pcid_origin,model
)as t2
on t1.udid = t2.udid
WHERE t1.duration >1200


#推送开关的开启和关闭
SELECT t1.udid,t2.dt,t1.duration,t2.body_source
from 
(
    SELECT dt,udid,sum(duration)as duration
    from dwb_v8sp.event_column_user_type
    where dt='2019-01-08'
    and event = 'video_over'
    and user_type ='new'
    and udid not in (
        SELECT DISTINCT udid 
        from dwb_v8sp.event_column_user_type
        WHERE dt<='2019-01-15'
            and dt >='2019-01-09'
            AND event = 'app_start'
            -- GROUP BY udid
            and user_type = 'old'
    )
    GROUP BY udid,dt
)as t1
join 
(
        SELECT dt,udid,body_source
    from dwb_v8sp.event_column_user_type
    where dt='2019-01-08'
    -- and dt <='2019-01-15'
    -- and user_type ='old'
    -- and event in ('push_video_receive','push_message_show')
    -- and event in ('push_video_receive','push_message_receive')
    and event = 'push_switch'
    GROUP BY udid,dt,body_source
)as t2
on t1.udid = t2.udid
WHERE t1.duration >1200









 














#无事件上报
SELECT dt,udid,sum(duration)as duration
    from dwb_v8sp.event_column_user_type
    where dt='2019-01-08'
    and event = 'video_over'
    and user_type ='new'
    and udid not in (
        SELECT DISTINCT udid 
        from dwb_v8sp.event_column_user_type
        WHERE dt<='2019-01-15'
            and dt >='2019-01-09'
            AND event = 'app_start'
            -- GROUP BY udid
            and user_type = 'old'
    )
    GROUP BY udid,dt

#查找nopush
SELECT t3.dt ,count(DISTINCT t.udid) as nopush 
from
(
    SELECT t1.udid as udid ,t2.event as event 
    from 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-15'
        and source = 'android'
        and body_source !='push'
        and event = 'app_start'
        and user_type='old'
        and pname = 'com.melon.lazymelon'
        GROUP by udid
    )t1 
    join
    (
        SELECT event ,udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-15'
        and source = 'android'
        and user_type='old'
        and pname = 'com.melon.lazymelon'
        and event not in ('push_video_clk','push_message_clk','push_category_clk','push_activity_clk')
        GROUP by udid ,event 
    )t2 
    on t1.udid=t2.udid
    GROUP by t1.udid ,t2.event 
)t
join
(
    SELECT udid ,substring(add_time,1,10)as dt
    from dim_v8sp.device_info
    where substring (add_time,1,10) = '2019-01-08'
    and plat = 1
    GROUP by udid,substring(add_time,1,10)
) t3
on t.udid =t3.udid 
GROUP BY t3.dt



#七日推送留存 
SELECT a.push as `推送拉起`,(a.liucun - a.push) as `非推送拉起`,(a.receive_push - a.push) as `推送到达`,
a.no_receive_push as `非推送到达`,(b.new_user_count - a.liucun-a.no_receive_push-a.receive_push) as `无事件人数`
from
(    
    SELECT t2.dt,

    -- count(case when (case when body_source in ('push') then ))

    count(DISTINCT case WHEN event = 'app_start' and body_source = 'push'then  t1.udid ELSE NULL END  )as push,
    count(DISTINCT case WHEN event = 'app_start' and body_source != 'push' 
        and event not in ('push_video_clk','push_message_clk','push_category_clk','push_activity_clk')  then  t1.udid ELSE NULL END  )as nopush,
    count(DISTINCT case WHEN  body_source != 'push' 
        and event not in ('corner_mark_show','push_video_clk','push_message_clk','push_category_clk','push_activity_clk')  then  t1.udid ELSE NULL END  )as nopush1,
    count(DISTINCT case WHEN event =  'app_start' 
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
    where dt = '2019-01-15'
    and user_type = 'old'
    and source ='android'
    and pname = 'com.melon.lazymelon'
    GROUP BY event ,udid ,body_source,dt
    )t1 
    join
    (
        SELECT udid ,substring (add_time,1,10)as dt
        from dim_v8sp.device_info
        where substring (add_time,1,10) = '2019-01-08'
        and plat =1
        GROUP BY udid,substring (add_time,1,10)

        -- select udid ,dt ,sum(case when event ='video_over' then duration else null end )as duration
        -- from dwb_v8sp.event_column_user_type
        -- where dt = '2019-01-08'
        -- and source ='android'
        -- and user_type ='new'
        -- and pname = 'com.melon.lazymelon'
        -- group by udid ,dt 

    ) t2
    on t1.udid =t2.udid 
    -- where t2.duration >1200
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





###########非推送启动人数
 SELECT  t2.dt ,count(DISTINCT t1.udid) as `非推送启动人数`
    from 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-09'
        and source = 'android'
        -- and body_source !='push'
        and udid not in (
            select udid 
            from dwb_v8sp.event_column_user_type
            where dt = '2019-01-09'
            and source = 'android'
            and event = 'app_start'
            and user_type = 'old'
            and body_source = 'push'
            and pname = 'com.melon.lazymelon'
            GROUP BY udid
        )
        and event = 'app_start'
        and user_type='old'
        and pname = 'com.melon.lazymelon'
        GROUP by udid
    )t1 
    -- t1是非推送启动的udid
    join
    (
        SELECT DISTINCT udid ,substring(add_time,1,10)as dt
        from dim_v8sp.device_info
        where substring (add_time,1,10) = '2019-01-08'
        and plat = 1
        -- GROUP by udid,substring(add_time,1,10)
    ) t2
on t1.udid =t2.udid 
GROUP BY t2.dt

################## 推送收到未打开
SELECT  t2.dt ,count(DISTINCT t1.udid) as `推送到达未打开`
    from 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-09'
        and source = 'android'
        and event in  ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
        and user_type='old'
        and pname = 'com.melon.lazymelon'
        GROUP by udid
    )t1 
    -- t1是推送到达的udid
    join
    (
        SELECT DISTINCT udid ,substring(add_time,1,10)as dt
        from dim_v8sp.device_info
        where substring (add_time,1,10) = '2019-01-08'
        and plat = 1
        and udid not in (
             select udid 
            from dwb_v8sp.event_column_user_type
            where dt = '2019-01-09'
            and source = 'android'
            and event = 'app_start'
            and user_type = 'old'
            and pname = 'com.melon.lazymelon'
            GROUP BY udid
        )
        -- GROUP by udid,substring(add_time,1,10)
    ) t2
on t1.udid =t2.udid 
GROUP BY t2.dt


################## 推送收到未打开加时长限制
SELECT  t2.dt ,count(DISTINCT t1.udid) as `推送到达未打开`
    from 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-09'
        and source = 'android'
        and event in  ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
        and user_type='old'
        and pname = 'com.melon.lazymelon'
        GROUP by udid
    )t1 
    -- t1是非推送启动的udid
    join
    (
        SELECT  udid ,dt 
        ,sum(case when event ='video_over' then duration else null end )as duration
        from dwb_v8sp.event_column_user_type
        where dt  = '2019-01-08'
        and source = 'android'
        and user_type = 'new'
        and pname= 'com.melon.lazymelon'
        and udid not in (
             select udid 
            from dwb_v8sp.event_column_user_type
            where dt = '2019-01-09'
            and source = 'android'
            and event = 'app_start'
            and user_type = 'old'
            and pname = 'com.melon.lazymelon'
            GROUP BY udid
        )
        GROUP by udid,dt
    ) t2
on t1.udid =t2.udid 
where t2.duration >1200
GROUP BY t2.dt


select count(distinct t.udid ) ,t.dt from 
(
select udid ,dt ,sum(case when event ='video_over' then duration else null end )as duration
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-08'
        -- and dt <='2019-01-11'
        and source ='android'
        and user_type ='new'
        group by udid ,dt ) t 
        
        where t.duration >1200
        group by dt 

#未收到推送以及未开启用户
SELECT  t2.dt ,count(DISTINCT t1.udid) as `推送未到达未打开`
    from 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-11'
        and source = 'android'
        -- and event  in  ('app_start','push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
        and udid not in 
        (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-11'
            and source = 'android'
            and event  in  ('app_start','push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
            and user_type='old'
            and pname = 'com.melon.lazymelon'
        GROUP by udid

        )
        and user_type='old'
        and pname = 'com.melon.lazymelon'
        GROUP by udid
    )t1 
    -- t1是非推送启动的udid
    join
    (
        SELECT DISTINCT udid ,dt as dt 
        from dwb_v8sp.event_column_user_type
        where dt  = '2019-01-08'
        and source = 'android'
        and user_type = 'new'
        and pname= 'com.melon.lazymelon'
        -- and udid not in (
        --      select udid 
        --     from dwb_v8sp.event_column_user_type
        --     where dt = '2019-01-11'
        --     and source = 'android'
        --     and event = 'app_start'
        --     and user_type = 'old'
        --     and pname = 'com.melon.lazymelon'
        --     GROUP BY udid
        -- )
        -- GROUP by udid,substring(add_time,1,10)
    ) t2
on t1.udid =t2.udid 
GROUP BY t2.dt


#未收到推送以及未开启用户限制播放时长
SELECT  t2.dt ,count(DISTINCT t1.udid) as `推送未到达未打开`
    from 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-11'
        and source = 'android'
        -- and event  in  ('app_start','push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
        and udid not in 
        (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where dt = '2019-01-11'
            and source = 'android'
            and event  in  ('app_start','push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
            and user_type='old'
            and pname = 'com.melon.lazymelon'
        GROUP by udid

        )
        and user_type='old'
        and pname = 'com.melon.lazymelon'
        GROUP by udid
    )t1 
    -- t1是非推送启动的udid
    join
    (
         select dt,udid from 
            (
            select udid ,dt ,sum(case when event ='video_over' then duration else null end )as duration
            from dwb_v8sp.event_column_user_type
            where dt = '2019-01-08'
            -- and dt <='2019-01-11'
            and source ='android'
            and user_type ='new'
            group by udid ,dt 
            ) t 
        
        where t.duration >1200
        group by dt ,udid
    ) t2
on t1.udid =t2.udid 
GROUP BY t2.dt



##主动打开
select 
count(distinct udid )as zhudong_open
from dwb_v8sp.event_column_user_type
where 
event = 'app_start'
and body_source !='push'
and dt ='2019-01-11'
and user_type = 'old'
and udid in (
    select udid 
    from dwb_v8sp.event_column_user_type
    where user_type = 'new'
    and dt = '2019-01-10'
    and event = 'app_start'
)


##有推送到达

select  t1.udid as udid
from
(select  udid
from dwb_v8sp.event_column_user_type
where 
udid not in (
    select udid 
    from dwb_v8sp.event_column_user_type
    where user_type = 'old'
    and dt = date_add('${date}',3)
    and event = 'app_start'
    )
and user_type ='new'
and  dt = '${date}'
-- and  event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
group by udid
)   t1
join 
(select udid 
    from dwb_v8sp.event_column_user_type
    where user_type = 'old'
    and dt = date_add('${date}',3)
    and  event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
    -- and event in ('push_video_show','push_message_show','push_category_show','push_activity_show')
    group by udid
) t2
    
on t1.udid = t2.udid

##关闭推送开关
select count(distinct t1.udid)as uv
from
(select  udid
--  (case when event in ('push_show') then zhanxian 
-- else wuzhanxian  end   )
from dwb_v8sp.event_column_user_type
where 
udid not in (
    select udid 
    from dwb_v8sp.event_column_user_type
    where user_type = 'old'
    and dt = '2019-01-11'
    and  event not in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive',
                        'push_video_show','push_message_show','push_category_show','push_activity_show','app_start')
    )
and user_type ='new'
and  dt = '2019-01-10'
-- and  event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
group by udid
)   t1
join 
(select udid 
    from dwb_v8sp.event_column_user_type
    where user_type = 'new'
    and dt = '2019-01-10'
    -- and  event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
    -- and event in ('push_video_show','push_message_show','push_category_show','push_activity_show')
    and event = 'push_guide_result'
    and body_source ='close'
    group by udid
) t2
    
on t1.udid = t2.udid


###无到达但是有事件
select count(distinct t1.udid)as uv
from
(select  udid
--  (case when event in ('push_show') then zhanxian 
-- else wuzhanxian  end   )
from dwb_v8sp.event_column_user_type
where 
udid not in (
    select udid 
    from dwb_v8sp.event_column_user_type
    where user_type = 'old'
    and dt = date_add('${date}',3)
    and  event  in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive',
                        -- 'push_video_show','push_message_show','push_category_show','push_activity_show',
                        'app_start')
    )
and user_type ='new'
and  dt = '${date}'
-- and  event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
group by udid
)   t1
join 
(select udid 
    from dwb_v8sp.event_column_user_type
    where user_type = 'old'
    and dt = date_add('${date}',3)
    -- and  event in ('push_video_receive','push_message_receive','push_category_receive','push_activity_receive')
    -- and event in ('push_video_show','push_message_show','push_category_show','push_activity_show')
    -- and event = 'push_guide_result'
    -- and body_source ='close'
    group by udid
) t2
    
on t1.udid = t2.udid


##生命周期核心数据
SELECT t1.udid,t1.dt,t2.vv,t2.`展现数`,t2.`有效播放数`,t2.`启动次数`,t2.`视频播放时长`,t2.`播放完成度`
from
(
    SELECT udid,
    count(case when event = 'client_show' then 1 else null end) as client_show, 
    dt
    from dwb_v8sp.event_column_user_type
    where dt ='${date}'
    and user_type ='new'
    -- and body_source in ('default','down','right','barselect')
    and udid  in 
        (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where user_type ='old'
        and dt = date_add('${date}',1)
        GROUP BY udid
        )
    GROUP BY udid,dt
) t1
join 
(
SELECT udid,
count(case when event = 'app_start' then 1 else null end) as `启动次数`,
count(case when event = 'video_play' then 1 else null end) as `vv`,  
count(case when event = 'client_show' then 1 else null end) as `展现数`, 
count(case when event = 'effective_play' then 1 else null end) as `有效播放数`,
avg(case when event = 'video_over' then play_cnts else null end) as `播放完成度`,
sum(case when event = 'video_over' and duration < 86400 then duration else null end) as `视频播放时长`
from
dwb_v8sp.event_column_user_type
where dt ='${date}'
and user_type = 'new'
GROUP BY udid
)t2
on t1.udid=t2.udid
where t1.client_show <=10

##生命周期人数（下载到接触）
SELECT 
-- count(DISTINCT case when client_show <= 10 then udid ELSE null end )as S_liushi,
-- count(DISTINCT case when client_show > 10 then udid ELSE null end )as B_liushi,
-- sum(case when client_show <= 10 then client_show ELSE null end)as S_liushi_num,
-- sum(case when client_show > 10 then client_show ELSE null end)as B_liushi_num
count(DISTINCT case when client_show <= 10 then udid ELSE null end )as S_liucun,
count(DISTINCT case when client_show > 10 then udid ELSE null end )as B_liucun,
sum(case when client_show <= 10 then client_show ELSE null end)as S_liucun_num,
sum(case when client_show > 10 then client_show ELSE null end)as B_liucun_num
from
(
    SELECT udid,
    count(case when event = 'client_show' then 1 else null end) as client_show, 
    dt
    from dwb_v8sp.event_column_user_type
    where dt ='${date}'
    and user_type ='new'
    -- and body_source in ('default','down','right','barselect')
    and udid not in 
        (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where user_type ='old'
        and event = 'app_start'
        and dt = date_add('${date}',1)
        GROUP BY udid
        )
    GROUP BY udid,dt
) t1


##生命周期人数2
select count(distinct t1.udid)
from
(
    select  udid,
    count( case when event ='client_show' then 1 else null end )as client_show,
    sum( case when event ='video_over' then duration else null end )as duration 
    from dwb_v8sp.event_column_user_type
    where
    dt = '${date}'
    and user_type ='new'
    and udid  in 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where user_type ='old'
            and event = 'app_start'
            and dt = date_add('${date}',1)
        GROUP BY udid
    )
    group by udid
 ) t1
 join 
(
    select  udid
    from dwb_v8sp.event_column_user_type
    where
    dt = '${date}'
    and user_type ='new'
    and udid  in 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where user_type ='old'
            and event = 'app_start'
            and dt = date_add('${date}',1)
        GROUP BY udid
    )
    and event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        ,'share','side_enter','author_page','ugc_start','barpage')
    and style not in ('auto_clk','auto_next')
    group by udid
 ) t2
 on t1.udid=t2.udid
--  where t1.client_show>50
--  and t1.duration >1500
 
 where t1.client_show>10
--  and t1.duration >600
