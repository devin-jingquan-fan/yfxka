-- dau
select t.dt as dt, t.ab_group_id,
           (case when t.diffday=0 then 'd0' when t.diffday=1 then 'd1' else 'other' end) as reg_day,
           count(distinct t.udid) as app_start_user
from
        (select a.dt as dt,a.udid as udid,datediff(a.dt,b.dt) as diffday, a.ab_group_id as ab_group_id
         from (select dt,udid,ab_group_id
               from dwb_v8sp.event_column_info_new_hour 
               where source='android'
                    and dt='2018-12-30'
                    and lower(event) not like '%push%' 
                    and event!='corner_mark_show'
                    and ab_group_id LIKE '97_%') a
         join 
        (select substring(add_time,1,10) as dt, udid from dim_v8sp.device_info where substring(add_time,1,10)<='2018-12-30') b
         on a.udid=b.udid) t
group by t.dt,t.ab_group_id,(case when t.diffday=0 then 'd0' when t.diffday=1 then 'd1' else 'other' end)
order by dt,reg_day;



-- 播放数据
SELECT dt as dt, 
    count(distinct case when lower(event) not like '%push%' and event!='corner_mark_show' then udid else null end) as dau,
    count(case when event='client_show' then 1 else null end) as client_show,
    count(case when event='video_play' then 1 else null end) as video_play,
    count(distinct case when event='effective_play' then udid else null end) as effective_play_uv,
    count(case when event='effective_play' then 1 else null end) as effective_play_pv,
    sum(case when event='video_over' then duration else 0 end) as duration
    FROM dwb_v8sp.event_column_info_new_hour
    WHERE dt = '2018-12-30'
        and source='android' 
        and pname='com.melon.lazymelon'
        and ab_group_id LIKE '97_%'
        and udid in (select udid from dim_v8sp.device_info where substring(add_time,1,10) = '2018-12-30')
    group by dt


select dt, ab_group_id,
    count(distinct udid) as dau
from event_column_info_new_hour
where source = 'android'
    and dt = '2018-12-30'
    and ab_group_id LIKE '97_%'
    and lower(event) not like '%push%' 
    and event!='corner_mark_show'
    and udid in (select udid from dim_v8sp.device_info where substring(add_time,1,10)<'2018-12-30')
group by dt, ab_group_id


select dt, ab_group_id,
    count(distinct udid) as stay
from event_column_info_new_hour
where source = 'android'
    and dt = '2018-12-30'
    and ab_group_id LIKE '97_%'
    and lower(event) not like '%push%' 
    and event!='corner_mark_show'
    and udid in (select udid
                 from event_column_info_new_hour
                 where source = 'android'
                    and dt = '2018-12-29'
                    and ab_group_id LIKE '97_%'
                    and lower(event) not like '%push%' 
                    and event!='corner_mark_show'
                    and udid in (select udid from dim_v8sp.device_info where substring(add_time,1,10)<'2018-12-29')
                 group by udid)
group by dt, ab_group_id




SELECT dt as DATA_DATE,
    count(distinct case when lower(event) not like '%push%' and event!='corner_mark_show' then udid else null end) as DAU,
    count(distinct case when event = 'audio_play' then udid else null end) as AUDIO_PLAY_UV,
    count(case when event = 'audio_play' then 1 else null end) as AUDIO_PLAY_PV,
    sum(case when event = 'audio_over' then duration else 0 end) as DURATION,
    count(distinct case when event = 'enter_audio' then udid else null end) as ENTER_AUDIO_UV,
    count(case when event = 'enter_audio' then 1 else null end) as ENTER_AUDIO_PV,
    count(distinct case when event = 'post_audio' then udid else null end) as POST_AUDIO_UV,
    count(case when event = 'post_audio' then 1 else null end) as POST_AUDIO_PV,
    count(distinct case when event = 'audio_cancel' then udid else null end) as AUDIO_CANCEL_UV,
    count(case when event = 'audio_cancel' then 1 else null end) as AUDIO_CANCEL_PV,
    count(distinct case when event = 'audio_short' then udid else null end) as AUDIO_SHORT_UV,
    count(case when event = 'audio_short' then 1 else null end) as AUDIO_SHORT_PV,
    count(distinct case when event = 'audio_success' then udid else null end) as AUDIO_SUCCESS_UV,
    count(case when event = 'audio_success' then 1 else null end) as AUDIO_SUCCESS_PV,
    count(distinct case when event = 'audio_fail' then udid else null end) as AUDIO_FAIL_UV,
    count(case when event = 'audio_fail' then 1 else null end) as AUDIO_FAIL_PV,
    count(case when event = 'login_page' and lower(body_source) = 'audio' then 1 else null end) as LOGIN_PAGE_PV,
    count(distinct case when event = 'login_page' and lower(body_source) = 'audio' then udid else null end) as LOGIN_PAGE_UV,
    count(distinct case when event = 'login_success' and lower(body_from) = 'audio' then udid else null end) as LOGIN_SUCCESS_UV
FROM dwb_v8sp.event_column_info_new 
WHERE dt in ('2018-12-30','2018-12-31','2019-01-01') and source='android' and vapp >= '2500' and ab_group_id LIKE '97_%'
group by dt



select tt1.DATA_DATE as DATA_DATE, tt1.REGTIME as REGTIME, tt1.DAU as DAU,
        tt1.AUDIO_PLAY_UV as AUDIO_PLAY_UV, tt1.AUDIO_PLAY_PV as AUDIO_PLAY_PV, tt1.DURATION as DURATION, 
        tt1.ENTER_AUDIO_UV as ENTER_AUDIO_UV, tt1.ENTER_AUDIO_PV as ENTER_AUDIO_PV,
        tt1.POST_AUDIO_UV as POST_AUDIO_UV, tt1.POST_AUDIO_PV as POST_AUDIO_PV, tt1.AUDIO_CANCEL_UV as AUDIO_CANCEL_UV, 
        tt1.AUDIO_CANCEL_PV as AUDIO_CANCEL_PV, tt1.AUDIO_SHORT_UV as AUDIO_SHORT_UV, tt1.AUDIO_SHORT_PV as AUDIO_SHORT_PV, 
        tt1.AUDIO_SUCCESS_UV as AUDIO_SUCCESS_UV, tt1.AUDIO_SUCCESS_PV as AUDIO_SUCCESS_PV, tt1.AUDIO_FAIL_UV as AUDIO_FAIL_UV, 
        tt1.AUDIO_FAIL_PV as AUDIO_FAIL_PV, tt1.LOGIN_PAGE_PV as LOGIN_PAGE_PV, 
        tt1.LOGIN_PAGE_UV as LOGIN_PAGE_UV, tt1.LOGIN_SUCCESS_UV as LOGIN_SUCCESS_UV
from
    (
      SELECT dt as DATA_DATE, case when substring(t2.add_time, 1, 10)='${date}' then 'new' else 'old' end as REGTIME,
            count(distinct case when lower(event) not like '%push%' and event!='corner_mark_show' then udid else null end) as DAU,
            count(distinct case when event = 'audio_play' then udid else null end) as AUDIO_PLAY_UV,
            count(case when event = 'audio_play' then 1 else null end) as AUDIO_PLAY_PV,
            sum(case when event = 'audio_over' then duration else 0 end) as DURATION,
            count(distinct case when event = 'enter_audio' then udid else null end) as ENTER_AUDIO_UV,
            count(case when event = 'enter_audio' then 1 else null end) as ENTER_AUDIO_PV,
            count(distinct case when event = 'post_audio' then udid else null end) as POST_AUDIO_UV,
            count(case when event = 'post_audio' then 1 else null end) as POST_AUDIO_PV,
            count(distinct case when event = 'audio_cancel' then udid else null end) as AUDIO_CANCEL_UV,
            count(case when event = 'audio_cancel' then 1 else null end) as AUDIO_CANCEL_PV,
            count(distinct case when event = 'audio_short' then udid else null end) as AUDIO_SHORT_UV,
            count(case when event = 'audio_short' then 1 else null end) as AUDIO_SHORT_PV,
            count(distinct case when event = 'audio_success' then udid else null end) as AUDIO_SUCCESS_UV,
            count(case when event = 'audio_success' then 1 else null end) as AUDIO_SUCCESS_PV,
            count(distinct case when event = 'audio_fail' then udid else null end) as AUDIO_FAIL_UV,
            count(case when event = 'audio_fail' then 1 else null end) as AUDIO_FAIL_PV,
            count(case when event = 'login_page' and lower(body_source) = 'audio' then 1 else null end) as LOGIN_PAGE_PV,
            count(distinct case when event = 'login_page' and lower(body_source) = 'audio' then udid else null end) as LOGIN_PAGE_UV,
            count(distinct case when event = 'login_success' and lower(body_from) = 'audio' then udid else null end) as LOGIN_SUCCESS_UV
      FROM 
      (
          select * from dwb_v8sp.event_column_info_new_hour
          WHERE dt='${date}' and source='android' and pname='com.melon.lazymelon' and ab_group_id LIKE '97_%'
      ) t1 
      left join
      (
          select udid as device_udid,add_time from dim_v8sp.device_info
          where add_time<'${d1after}' and add_time>='${date}'
      ) t2 
      on t1.udid=t2.device_udid 
      group by dt,case when substring(t2.add_time, 1, 10)='${date}' then 'new' else 'old' end, ab_group_id
    ) tt1






select
    t.dt as dt,
    t.dau as dau,
    t3.vos,
    t3.cnt_push_activity_receive as cnt_push_activity_receive,
    t.cnt_push_activity_clk as cnt_push_activity_clk
from
    (
        select t1.dt as dt,
            t1.dau as dau,
            t1.vos as vos,
            t2.cnt_push_activity_clk as cnt_push_activity_clk
        from
            (
                select
                    dt,vos
                    count(distinct udid) as dau
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2018-12-25'
                    and dt <= '2019-01-02'
                    and source = 'android'
                    and event = 'app_start'
                    and brand not in ('HUAWEI',  'HONOR')
                group by
                    dt,vos
            ) t1
            join (
                select
                    dt,vos,
                    count(distinct udid) as cnt_push_activity_clk
                from
                    dwb_v8sp.event_column_info_new_hour
                where
                    dt >= '2018-12-25'
                    and dt <= '2019-01-02'
                    and source = 'android'
                    and event = 'push_video_clk'
                    and brand not in ('HUAWEI',  'HONOR')
                group by
                    dt,vos
            ) t2 on t1.dt = t2.dt and t1.vos=t2.vos
    ) t
    join (
        select
            dt,vos,
            count(distinct udid) as cnt_push_activity_receive
        from
            dwb_v8sp.event_column_info_new_hour
        where
            dt >= '2018-12-25'
            and dt <= '2019-01-02'
            and source = 'android'
            and event = 'push_video_receive'
            and brand not in ('HUAWEI',  'HONOR')
        group by
            dt,vos
    ) t3 on t.dt = t3.dt and t.vos=t3.vos




