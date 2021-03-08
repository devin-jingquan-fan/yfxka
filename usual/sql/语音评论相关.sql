select dt, vapp,sum(duration),
    count(distinct udid) as uv,
    count(*) as pv
from dwb_v8sp.event_column_info_new
where dt = '2018-12-23' 
    and source = 'android'
    and event = 'audio_over'
    and vapp = '2620'
    and cast(duration as int) <= 86400
    -- and cast(duration as int)>

    -- nvl(duration,0)

group by dt, vapp
-- 语音播放时长

select dt, vapp,
    count(distinct udid) as uv,
    count(*) as pv
from dwb_v8sp.event_column_info_new
where dt = '2018-12-23' 
    and source = 'android'
    and event = 'login_page'
    and body_source = 'audio'
    and vapp = '2620'
   
group by dt, vapp
-- 从语音播放跳出登录页面

select dt, vapp,
    count(distinct udid) as uv,
    count(*) as pv
from dwb_v8sp.event_column_info_new
where dt = '2018-12-23' 
    and source = 'android'
    and event = 'login_success'
    and body_from = 'audio'
    and vapp = '2620'
   
group by dt, vapp
-- 从语音播放跳出登陆并且登录成功


select dt, vapp,
    count(distinct udid) as uv,
    count(*) as pv
from dwb_v8sp.event_column_info_new
where dt = '2018-12-24' 
    and source = 'android'
    and event = 'enter_audio'
    -- and body_source = 'audio'
    and pcid_current in ('cmldyhd1','cmldyhd2')
    and vapp = '2620'
group by dt ,vapp 
-- 限制渠道

-- 2.7.4新老用户数据
SELECT t2.dt,
count(distinct t1.udid)
      from 
(
select distinct udid 
from dim_v8sp.device_info
where substring(add_time,1,10) = '2018-12-26' 
and pcid = '1'
-- and app_version='2.7.4'  不能加限制

) t1
join 
(
select * from dwb_v8sp.udid_stat_info
where source ='android'
and pname = 'com.melon.lazymelon'
)t2
on t1.udid=t2.udid
GROUP by dt 

-- 查询语音整体相关数据

select dt,event,vapp,
sum(duration)as duration,
    count(distinct udid) as uv,
    count(*) as pv
from dwb_v8sp.event_column_info_new
where dt >= '2018-12-28' 
    and dt <= '2018-12-28'
    and source = 'android'
    -- and event = ''
    -- and vapp = '2620'
    and event in ('app_start','audio_play','enter_audio','audio_success')
    
    -- and body_source  = 'audio'
    -- nvl(duration,0)
    -- and cast(duration as int) <= 86400

group by dt,event,vapp



#语音活跃留存
select
    t.dt as dt,(
        case when t.diffday = 0 then '0_day' 
        when t.diffday = 1 then '1_day' 
        when t.diffday = 2 then '2_day' 
        when t.diffday = 3 then '3_day' 
        when t.diffday = 4 then '4_day' 
        when t.diffday = 5 then '5_day' 
        when t.diffday = 6 then '6_day'
        when t.diffday = 7 then '7_day' else null end
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
                seLECT  udid,
                    dt
                FROM
                    dwb_v8sp.event_column_info_new_hour
                WHERE
                    dt >= '2018-12-14'
                    and dt <= '2018-12-21'
                    and event = 'app_start'
                    and substring(vapp,1,4)>=2500
                group by
                    dt,
                    udid
            ) t1
            left join (
                seLECT   udid,
                    dt
                FROM
                    dwb_v8sp.event_column_info_new_hour
                WHERE
                    dt >= '2018-12-14'
                    and dt <= '2018-12-20'
                    and event = 'app_start'
                    and substring(vapp,1,4)>=2500
                group by
                    dt,
                    udid
            ) t2 on t1.udid = t2.udid
    ) t
where
    diffday >= 0
    and diffday <= 7
group by
    t.dt,
    (
        case when t.diffday = 0 then '0_day' 
        when t.diffday = 1 then '1_day' 
        when t.diffday = 2 then '2_day' 
        when t.diffday = 3 then '3_day' 
        when t.diffday = 4 then '4_day' 
        when t.diffday = 5 then '5_day' 
        when t.diffday = 6 then '6_day'
        when t.diffday = 7 then '7_day' else null end
    ) 
order by
    dt;

#语音评论视频数，条数，所有展现视频数
select t.`有语音评论的视频数`,t.audio_comment_sum,t.dt,t3.vid_count
from
(select t1.dt as dt,
-- sum( t1.vid_count )as `展现视频数`,
count(DISTINCT case when t2.audio_comment_sum > 0 then  t1.vid else null end)as `有语音评论的视频数`,
count(distinct t2.comment_id )as audio_comment_sum
-- sum(audio_comment_sum)
from

(
select  vid, dt 
from dwb_v8sp.event_column_info_new_hour
where dt='2018-12-19' 
and event = 'client_show'
and source='android' 
and pname='com.melon.lazymelon' 
and vapp >= 2500
group by dt,vid 
)t1
join
(
    sELECT cid,
    comment_id,
    count(distinct comment_id) as audio_comment_sum
    from dim_v8sp.comment_info
    where status=2
    and comment_type =4
and substring(add_time,1,10)<='2018-12-19' 
group by cid,comment_id
) t2
on t1.vid = t2.cid
group by t1.dt) t 
join 
(
select  dt ,count(distinct vid)as vid_count
from dwb_v8sp.event_column_info_new_hour
where dt='2018-12-19' 
and event = 'client_show'
and source='android' 
and pname='com.melon.lazymelon' 
and vapp >= 2500
group by dt
)t3
on t.dt=t3.dt


###语音评论ugc的条数和人数
select
    t.dt as dt,(
        case when t.diffday = 0 then '0_day' 
        when t.diffday = 1 then '1_day' 
        when t.diffday = 2 then '2_day' 
        when t.diffday = 3 then '3_day' 
        when t.diffday = 4 then '4_day' 
        when t.diffday = 5 then '5_day' 
        when t.diffday = 6 then '6_day'
        when t.diffday = 7 then '7_day' else null end
    ) as RETENTION_DAYS,
    count(distinct t.udid) as RETENTION_USERS
from 
(
    sELECT 
    
    from dim_v8sp.comment_info
    where status= 1
    and comment_type = 4
    
    and substring(add_time,1,10)<='2019-01-15' 
    -- group by cid,comment_id
)
group by
    t.dt,
    (
        case when t.diffday = 0 then '0_day' 
        when t.diffday = 1 then '1_day' 
        when t.diffday = 2 then '2_day' 
        when t.diffday = 3 then '3_day' 
        when t.diffday = 4 then '4_day' 
        when t.diffday = 5 then '5_day' 
        when t.diffday = 6 then '6_day'
        when t.diffday = 7 then '7_day' else null end
    ) 

select count(1)as pv ,hour,dt
from dwb_v8sp.event_column_info_new_hour
where dt >='2019-01-16'
and dt <='2019-01-17'
and event = 'audio_success'
group by dt ,hour 


##分半小时的语音上传数据

select  (case when	substring(time_local,13,5)	between	'00:00'	and	'00:30'	then	'00:30'
when	substring(time_local,13,5)	between	'00:31'	and	'01:00'	then	'01:00'
when	substring(time_local,13,5)	between	'01:01'	and	'01:30'	then	'01:30'
when	substring(time_local,13,5)	between	'01:31'	and	'02:00'	then	'02:00'
when	substring(time_local,13,5)	between	'02:01'	and	'02:30'	then	'02:30'
when	substring(time_local,13,5)	between	'02:31'	and	'03:00'	then	'03:00'
when	substring(time_local,13,5)	between	'03:01'	and	'03:30'	then	'03:30'
when	substring(time_local,13,5)	between	'03:31'	and	'04:00'	then	'04:00'
when	substring(time_local,13,5)	between	'04:01'	and	'04:30'	then	'04:30'
when	substring(time_local,13,5)	between	'04:31'	and	'05:00'	then	'05:00'
when	substring(time_local,13,5)	between	'05:01'	and	'05:30'	then	'05:30'
when	substring(time_local,13,5)	between	'05:31'	and	'06:00'	then	'06:00'
when	substring(time_local,13,5)	between	'06:01'	and	'06:30'	then	'06:30'
when	substring(time_local,13,5)	between	'06:31'	and	'07:00'	then	'07:00'
when	substring(time_local,13,5)	between	'07:01'	and	'07:30'	then	'07:30'
when	substring(time_local,13,5)	between	'07:31'	and	'08:00'	then	'08:00'
when	substring(time_local,13,5)	between	'08:01'	and	'08:30'	then	'08:30'
when	substring(time_local,13,5)	between	'08:31'	and	'09:00'	then	'09:00'
when	substring(time_local,13,5)	between	'09:01'	and	'09:30'	then	'09:30'
when	substring(time_local,13,5)	between	'09:31'	and	'10:00'	then	'10:00'
when	substring(time_local,13,5)	between	'10:01'	and	'10:30'	then	'10:30'
when	substring(time_local,13,5)	between	'10:31'	and	'11:00'	then	'11:00'
when	substring(time_local,13,5)	between	'11:01'	and	'11:30'	then	'11:30'
when	substring(time_local,13,5)	between	'11:31'	and	'12:00'	then	'12:00'
when	substring(time_local,13,5)	between	'12:01'	and	'12:30'	then	'12:30'
when	substring(time_local,13,5)	between	'12:31'	and	'13:00'	then	'13:00'
when	substring(time_local,13,5)	between	'13:01'	and	'13:30'	then	'13:30'
when	substring(time_local,13,5)	between	'13:31'	and	'14:00'	then	'14:00'
when	substring(time_local,13,5)	between	'14:01'	and	'14:30'	then	'14:30'
when	substring(time_local,13,5)	between	'14:31'	and	'15:00'	then	'15:00'
when	substring(time_local,13,5)	between	'15:01'	and	'15:30'	then	'15:30'
when	substring(time_local,13,5)	between	'15:31'	and	'16:00'	then	'16:00'
when	substring(time_local,13,5)	between	'16:01'	and	'16:30'	then	'16:30'
when	substring(time_local,13,5)	between	'16:31'	and	'17:00'	then	'17:00'
when	substring(time_local,13,5)	between	'17:01'	and	'17:30'	then	'17:30'
when	substring(time_local,13,5)	between	'17:31'	and	'18:00'	then	'18:00'
when	substring(time_local,13,5)	between	'18:01'	and	'18:30'	then	'18:30'
when	substring(time_local,13,5)	between	'18:31'	and	'19:00'	then	'19:00'
when	substring(time_local,13,5)	between	'19:01'	and	'19:30'	then	'19:30'
when	substring(time_local,13,5)	between	'19:31'	and	'20:00'	then	'20:00'
when	substring(time_local,13,5)	between	'20:01'	and	'20:30'	then	'20:30'
when	substring(time_local,13,5)	between	'20:31'	and	'21:00'	then	'21:00'
when	substring(time_local,13,5)	between	'21:01'	and	'21:30'	then	'21:30'
when	substring(time_local,13,5)	between	'21:31'	and	'22:00'	then	'22:00'
when	substring(time_local,13,5)	between	'22:01'	and	'22:30'	then	'22:30'
when	substring(time_local,13,5)	between	'22:31'	and	'23:00'	then	'23:00'
when	substring(time_local,13,5)	between	'23:01'	and	'23:30'	then	'23:30'
when	substring(time_local,13,5)	between	'23:31'	and	'23:59'	then	'23:59'  else null end ),
dt ,count(1)as pv 
from dwb_v8sp.event_column_info_new_hour
where dt >='2019-01-16'
and dt <='2019-01-17'
and event = 'audio_success'
group by dt,(
case when	substring(time_local,13,5)	between	'00:00'	and	'00:30'	then	'00:30'
when	substring(time_local,13,5)	between	'00:31'	and	'01:00'	then	'01:00'
when	substring(time_local,13,5)	between	'01:01'	and	'01:30'	then	'01:30'
when	substring(time_local,13,5)	between	'01:31'	and	'02:00'	then	'02:00'
when	substring(time_local,13,5)	between	'02:01'	and	'02:30'	then	'02:30'
when	substring(time_local,13,5)	between	'02:31'	and	'03:00'	then	'03:00'
when	substring(time_local,13,5)	between	'03:01'	and	'03:30'	then	'03:30'
when	substring(time_local,13,5)	between	'03:31'	and	'04:00'	then	'04:00'
when	substring(time_local,13,5)	between	'04:01'	and	'04:30'	then	'04:30'
when	substring(time_local,13,5)	between	'04:31'	and	'05:00'	then	'05:00'
when	substring(time_local,13,5)	between	'05:01'	and	'05:30'	then	'05:30'
when	substring(time_local,13,5)	between	'05:31'	and	'06:00'	then	'06:00'
when	substring(time_local,13,5)	between	'06:01'	and	'06:30'	then	'06:30'
when	substring(time_local,13,5)	between	'06:31'	and	'07:00'	then	'07:00'
when	substring(time_local,13,5)	between	'07:01'	and	'07:30'	then	'07:30'
when	substring(time_local,13,5)	between	'07:31'	and	'08:00'	then	'08:00'
when	substring(time_local,13,5)	between	'08:01'	and	'08:30'	then	'08:30'
when	substring(time_local,13,5)	between	'08:31'	and	'09:00'	then	'09:00'
when	substring(time_local,13,5)	between	'09:01'	and	'09:30'	then	'09:30'
when	substring(time_local,13,5)	between	'09:31'	and	'10:00'	then	'10:00'
when	substring(time_local,13,5)	between	'10:01'	and	'10:30'	then	'10:30'
when	substring(time_local,13,5)	between	'10:31'	and	'11:00'	then	'11:00'
when	substring(time_local,13,5)	between	'11:01'	and	'11:30'	then	'11:30'
when	substring(time_local,13,5)	between	'11:31'	and	'12:00'	then	'12:00'
when	substring(time_local,13,5)	between	'12:01'	and	'12:30'	then	'12:30'
when	substring(time_local,13,5)	between	'12:31'	and	'13:00'	then	'13:00'
when	substring(time_local,13,5)	between	'13:01'	and	'13:30'	then	'13:30'
when	substring(time_local,13,5)	between	'13:31'	and	'14:00'	then	'14:00'
when	substring(time_local,13,5)	between	'14:01'	and	'14:30'	then	'14:30'
when	substring(time_local,13,5)	between	'14:31'	and	'15:00'	then	'15:00'
when	substring(time_local,13,5)	between	'15:01'	and	'15:30'	then	'15:30'
when	substring(time_local,13,5)	between	'15:31'	and	'16:00'	then	'16:00'
when	substring(time_local,13,5)	between	'16:01'	and	'16:30'	then	'16:30'
when	substring(time_local,13,5)	between	'16:31'	and	'17:00'	then	'17:00'
when	substring(time_local,13,5)	between	'17:01'	and	'17:30'	then	'17:30'
when	substring(time_local,13,5)	between	'17:31'	and	'18:00'	then	'18:00'
when	substring(time_local,13,5)	between	'18:01'	and	'18:30'	then	'18:30'
when	substring(time_local,13,5)	between	'18:31'	and	'19:00'	then	'19:00'
when	substring(time_local,13,5)	between	'19:01'	and	'19:30'	then	'19:30'
when	substring(time_local,13,5)	between	'19:31'	and	'20:00'	then	'20:00'
when	substring(time_local,13,5)	between	'20:01'	and	'20:30'	then	'20:30'
when	substring(time_local,13,5)	between	'20:31'	and	'21:00'	then	'21:00'
when	substring(time_local,13,5)	between	'21:01'	and	'21:30'	then	'21:30'
when	substring(time_local,13,5)	between	'21:31'	and	'22:00'	then	'22:00'
when	substring(time_local,13,5)	between	'22:01'	and	'22:30'	then	'22:30'
when	substring(time_local,13,5)	between	'22:31'	and	'23:00'	then	'23:00'
when	substring(time_local,13,5)	between	'23:01'	and	'23:30'	then	'23:30'
when	substring(time_local,13,5)	between	'23:31'	and	'23:59'	then	'23:59' else null end 
)



