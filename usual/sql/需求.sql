
#上传者分成
select
t1.talent,t1.talent_video,t2.current_show,
t1.current_vshow,t1.real_current_vshow,
t1.current_vv_ep,t1.real_current_vv_ep,
t1.profit,
t2.category_id,t2.body_comm_cnts
from 
(
    select talent,talent_video,
    sum(current_vshow)as current_vshow,
    sum(real_current_vshow) as real_current_vshow,
    sum(current_vv_ep)as current_vv_ep,
    sum(real_current_vv_ep) as real_current_vv_ep,
    sum(current_profit)as profit
    from  dwb_v8sp.talent_video_basic_metrcs_original_clo
    where dt >='2019-01-08'
        and dt <='2019-01-14'
        and current_vv_ep > 0
    group by talent,talent_video
)t1
join 
(
    select author_id ,vid,body_comm_cnts,category_id,count(1)as current_show
    from dwb_v8sp.event_column_info_new_hour
    where dt >='2019-01-08'
        and dt <='2019-01-14'
        and author_type ='5'
        and event = 'client_show'
        -- and strategy in ('hot','new','good','random')
    group by author_id ,vid,body_comm_cnts,category_id
)t2
on t1.talent = t2.author_id
    and t1.talent_video = t2.vid
    



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


##语音播放相关数据
select  body_au_message_id,
count(distinct case when event = 'audio_play' then udid else null end) as Audio_play_uv, 
count(case when event = 'audio_play' then 1 else null end) as Audio_play_pv, 
sum(case when event = 'audio_over' then body_duration else null end) as  audio_duration, 
count(case when event = 'like_audio' then 1 else null end) as like_audio, 
count(case when event = 'audio_success' and body_style = 'reply' then 1 else null end) as Audio_reply_pv 

from dwd_v8sp.event_column_info_hour
where 
dt >='2019-01-17'
and body_au_message_id in(
''
)
group by body_au_message_id



#一段时间内新增的，且有播放的语音条数
SELECT count(DISTINCT t1.comment_id),sum(t2.pv)as pv,max(t2.pv)as max
from
(SELECT comment_id
-- count(DISTINCT comment_id)
from dim_v8sp.comment_info
where
status =2
and comment_type = 4
and substring(add_time,1,10)>='2019-02-05'
and substring(add_time,1,10)<='2019-02-12'
-- limit 10
and comment_id !='0'
GROUP BY comment_id
)t1
join
(
SELECT body_au_message_id,count(1)as pv
-- count(DISTINCT body_au_message_id)
from dwd_v8sp.event_column_info_hour
where 
event ='like_audio'
and
dt >='2019-02-05'
-- and dt <='2019-02-12'
GROUP BY body_au_message_id
-- LIMIT 100
)t2
on t1.comment_id=t2.body_au_message_id
-- where t2.pv>10
-- where t1.digg_num>0



#语音评论入库数，按小时的数据,依赖后台接口的
SELECT 
substring(add_time,1,10)as dt,
substring(add_time,12,2)as hour1,
count(DISTINCT comment_id) as num
from dim_v8sp.comment_info
where 
substring(add_time,1,10)in('2019-01-16','2019-01-23','2019-02-12','2019-02-13')
and comment_type = 4
GROUP BY 
substring(add_time,1,10),
substring(add_time,12,2)

#ab分组核心数据
SELECT  t1.dt,t2.body_ab,
        count(t2.udid) as dau,
        sum(t1.video_play) as video_play,  
        sum(t1.client_show) as client_show, 
        sum(t1.effective_play) as effective_play,
        sum(case when  video_duration <86400 then video_duration else null end) as video_duration,
        sum(t1.audio_play) as audio_play,  
        sum(case when  audio_duration <86400 then audio_duration else null end) as audio_duration,
        sum(t1.audio_success) as audio_success,
        sum(t1.audio_success_reply) as audio_success_reply,
        sum(t1.like_audio) as like_audio,
        sum(case when  app_exit <86400 then app_exit else null end) as app_exit
    FROM (
    SELECT udid,dt,
        count(case when event = 'video_play' then 1 else null end) as video_play,  
        count(case when event = 'client_show' then 1 else null end) as client_show, 
        count(case when event = 'effective_play' then 1 else null end) as effective_play,
        sum(case when event = 'video_over' and duration < 86400 then duration else null end) as video_duration,
        count(case when event = 'audio_play' then 1 else null end) as audio_play,  
        sum(case when event = 'audio_over' and duration <1000 then duration else null end) as audio_duration,
        count(case when event = 'audio_success' then 1 else null end) as audio_success   ,
        count(case when event = 'like_audio' then 1 else null end) as like_audio   ,
        count(CASE WHEN event ='audio_success' and style ='reply' then 1 ELSE null end)as audio_success_reply,
        sum(case when event = 'app_exit' and duration < 86400 then duration else null end) as app_exit
        FROM dwb_v8sp.event_column_user_type
        WHERE dt ='${date}' 
        and source='android' and pname='com.melon.lazymelon' 
        AND cast(vapp AS BIGINT) >= 3000
        and event in ('app_start','video_play','client_show','effective_play','video_over','like_audio','audio_play','audio_over','audio_success','app_exit')
        GROUP BY udid,dt
    ) AS t1
    JOIN (
    SELECT udid,body_ab
    FROM dwb_v8sp.event_column_user_type
        WHERE dt ='${date}' 
        and event = 'app_start'
        and source='android' and pname='com.melon.lazymelon' 
        and body_ab in('151_default_0','151_default_1','151_audio_rec_0','151_audio_rec_1') 
        AND cast(vapp AS BIGINT) >= 3000
        GROUP BY udid,body_ab
        
    ) AS t2 ON t2.udid = t1.udid
    GROUP BY dt,t2.body_ab
    
#次留
select t1.body_ab,count(t1.udid)as liucun
from
(
select udid ,body_ab
from dwb_v8sp.event_column_user_type
where dt ='${date}'
-- and event ='audio_play'
and user_type = 'new'
and body_ab in('151_default_0','151_default_1','151_audio_rec_0','151_audio_rec_1') 
-- and event in ('app_start','video_play','client_show','effective_play','video_over','audio_play','audio_over','audio_success','app_exit')
AND cast(vapp AS BIGINT) >= 3000
group by udid,body_ab
) t1
join 
(
select udid 
from dwb_v8sp.event_column_user_type
where 
 dt ='2019-02-23'
and user_type = 'old'
and event ='app_start'
-- and body_ab in('151_default_0','151_default_1','151_audio_rec_0','151_audio_rec_1') 
AND cast(vapp AS BIGINT) >= 3000
group by udid
) t2
on t1.udid = t2.udid
group by t1.body_ab



