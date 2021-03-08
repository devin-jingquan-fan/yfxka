select t.dt,t.audio_comment_vid_count as `有语音评论的视频数`,t.audio_comment_vid_client_count as `有语音评论的视频展现数`,
t3.total_vid_count as `展现视频数`,t3.total_client_count as `展现次数`,
t.audio_comment_sum as  `语音评论数量`
from
(
    select t1.dt as dt,
    count(DISTINCT case when t2.audio_comment_sum_test > 0 then  t1.vid else null end)as audio_comment_vid_count,
    sum(vid_client_num)as audio_comment_vid_client_count,
    sum( t2.audio_comment_sum_test )as audio_comment_sum
    from
        (
            select  vid, dt ,count(1)as vid_client_num
            from dwb_v8sp.event_column_info_new_hour
            where dt='${date}' 
            and event = 'client_show'
            and source='android' 
            and pname='com.melon.lazymelon' 
            and nvl(cast(vapp as int),0) >= 2500
            group by dt,vid 
        )t1
        join
        (
            sELECT cid,
            -- comment_id,
            count(distinct comment_id) as audio_comment_sum_test
            from dim_v8sp.comment_info
            where status=2
            and comment_type =4
            and substring(add_time,1,10)<='${date}' 
            group by cid
        ) t2
    on t1.vid = t2.cid
    group by t1.dt
) t 
-- 有语音评论和整体数据取交集，得到有语音评论的播放数据
join 
(
    select  dt ,count(distinct vid)as total_vid_count,count(1)as total_client_count
    from dwb_v8sp.event_column_info_new_hour
    where dt='${date}' 
    and event = 'client_show'
    and source='android' 
    and pname='com.melon.lazymelon' 
    and nvl(cast(vapp as int),0) >= 2500
    group by dt
)t3
-- 总的展现数，total数据
on t.dt=t3.dt



    ## 启动次数
SELECT dt,hour,body_source,
count(distinct case when event ='app_start' then  udid else null end) as ` 启动人数`,
count(case when event='app_start' then 1 else null end)as `启动次数`
from dwb_v8sp.event_column_info_new_hour
where dt='${date}' 
and source='android' 
and pname='com.melon.lazymelon' 
group by dt,hour,body_source

###播放详细数据
##ab数据
        SELECT dt as DATA_DATE, vapp as VERSION,
        -- count(DISTINCT case when  event='app_start' then udid else null end) as dau,
        count(DISTINCT case when lower(event) not like '%push%' and event!='corner_mark_show' then udid else null end) as dau,
        count(case when event = 'app_start' then 1 else null end) as `启动次数`,
        count(distinct case when event = 'client_show' then udid else null end) as `有展现人数`, 
        count(case when event = 'client_show' then 1 else null end) as `展现数`, 
        count(distinct case when event = 'video_play' then udid else null end) as `vv人数`, 
        count(case when event = 'video_play' then 1 else null end) as `vv`,  
        count(distinct case when event = 'effective_play' then udid else null end) as `有效播放人数`,
        count(case when event = 'effective_play' then 1 else null end) as `有效播放数`,
        -- count(case when event = 'video_over' then 1 else null end) as video_over_pv,
 
        
        
        
        -- count(distinct case when event = 'video_over' then udid else null end) as video_over_uv,
        -- count(distinct case when event = 'client_show' then vid else null end) as client_show_vid, 
        avg(case when event = 'video_over' then play_cnts else null end) as `播放完成度`,
        sum(case when event = 'video_over' and duration < 86400 then duration else null end) as `视频播放时长`,

        count(distinct case when event = 'audio_play' then udid else null end) as `语音播放人数`,
        count(case when event = 'audio_play' then 1 else null end) as `语音播放数`,  
         
        sum(case when event = 'audio_over' and duration <86400 then duration else null end) as `语音播放时长`,

        count(distinct case when event = 'audio_success' then udid else null end) as `语音发送人数`,
        count(case when event = 'audio_success' then 1 else null end) as `语音发送数`,   
        count(distinct case when event = 'audio_success' and style ='reply' then udid else null end) as `语音二级回复人数`,
        count(case when event = 'audio_success' and style ='reply' then 1 else null end) as `语音二级回复数`,  

        -- count(case when event = 'audio_reply' then 1 else null end) as `语音回复数`,
        -- count(distinct case when event = 'audio_reply' then udid else null end) as `语音回复人数`,
        count(distinct case when event = 'audio_fail' then udid else null end) as `audio_fail_uv`,
        count(case when event = 'audio_fail' then 1 else null end) as `audio_fail_pv`,
        count(distinct case when event = 'like_audio' then udid else null end) as `like_audio_uv`,
        count(case when event = 'like_audio' then 1 else null end) as `like_audio_pv`,
        count(distinct case when event = 'share_audio_success' then udid else null end) as `share_audio_uv`,
        count(case when event = 'share_audio_success' then 1 else null end) as `share_audio_pv`

        
        FROM dwb_v8sp.event_column_user_type
        WHERE dt='${date}' and source='android' and pname='com.melon.lazymelon' 
        and vapp in('2740','3010','3020','3030','3040') 
        and user_type ='new'
        and pcid_current  in ('cmldyhd1','cmldyhd2') 
        group by dt,vapp


###次留用户数
select t1.vapp,count(t1.udid)as liucun
from
(
select udid ,vapp
from dwb_v8sp.event_column_user_type
where dt ='${date}'
and user_type = 'new'
and vapp in ('3182','3183')
group by udid,vapp
) t1
join 
(
select udid 
from dwb_v8sp.event_column_user_type
where 
 dt ='2019-02-24'
and user_type = 'old'
and event ='app_start'
and vapp in ('3182','3183')
group by udid
) t2
on t1.udid = t2.udid
group by t1.vapp



