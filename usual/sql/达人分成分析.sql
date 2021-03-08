#分成视频的队列分布情况
SELECT t1.dt,t2.strategy,count( t1.talent_video)
from
(SELECT dt,talent_video
FROM dwb_v8sp.talent_video_basic_metrcs_clo
where dt>='2019-01-01'
and dt <= '2019-01-06'
and profit_type = 'underway'
and talent not in (
select original_id as talent from dim_v8sp.original_user
)
and current_vv_ep > 0
GROUP BY dt,talent_video )t1
join 
(select dt,vid,strategy
from dwb_v8sp.event_column_info_new_hour
WHERE dt>='2019-01-01'
and dt <= '2019-01-06'
GROUP BY dt,vid,strategy
)t2
on t1.dt=t2.dt
    and t1.talent_video = t2.vid
GROUP BY t1.dt,t2.strategy

#分成视频吧占比
SELECT t1.dt,t2.category_id,count( t1.talent_video)
from
(SELECT dt,talent_video
FROM dwb_v8sp.talent_video_basic_metrcs_clo
where dt>='2019-01-01'
and dt <= '2019-01-06'
and profit_type = 'underway'
and talent not in (
select original_id as talent from dim_v8sp.original_user
)
and current_vv_ep > 0
GROUP BY dt,talent_video )t1
join 
(select dt,vid,category_id
from dwb_v8sp.event_column_info_new_hour
WHERE dt>='2019-01-01'
and dt <= '2019-01-06'
GROUP BY dt,vid,category_id
)t2
on t1.dt=t2.dt
    and t1.talent_video = t2.vid
GROUP BY t1.dt,t2.category_id

-- ssssssss
#分成视频的上传日期
SELECT t1.dt as `上传日期` ,t2.dt as `分成日期`,t2.category_id,count( t1.videoid)as `视频数`
from
(
SELECT videoid,dt
from dwb_v8sp.talent_audit_info
WHERE dt >='2018-12-24'
)t1
join 
(SELECT dt,talent_video
FROM dwb_v8sp.talent_video_basic_metrcs_clo
where dt>='2019-01-01'
and dt <= '2019-01-06'
and profit_type = 'underway'
and talent not in (
select original_id as talent from dim_v8sp.original_user
)
and current_vv_ep > 0
GROUP BY dt,talent_video 
)t2
on  t1.videoid = t2.talent_video
GROUP BY t2.dt,t2.category_id



#达人上传的视频累计点赞、评论、vv数。

SELECT t.makedt as `上传日期`,
    t.moneydt as `分成日期`,
    sum(t.effective_play)as effective_play,
    sum(t3.like_count)as like_count,
    sum(t3.comm_count) as comm_count,
    sum (t3.vv)as vv,
    count(DISTINCT t.vid)as `视频数`
from
    (SELECT t1.dt as makedt,t2.dt as moneydt,t2.current_vv_ep as effective_play,
    t2.talent_video as vid
    from
            (
            SELECT DISTINCT videoid,dt
            from dwb_v8sp.talent_audit_info
            WHERE dt >='2018-12-24'
            )t1
        join 
            (SELECT  talent_video,current_vv_ep,dt
            FROM dwb_v8sp.talent_video_basic_metrcs_clo
            where dt>='2019-01-01'
            and dt <= '2019-01-09'
            and profit_type = 'underway'
            and talent not in (
            select original_id as talent from dim_v8sp.original_user
            )
        and current_vv_ep > 0
        GROUP BY dt,talent_video,current_vv_ep
        )t2
    on  t1.videoid = t2.talent_video
    GROUP BY t1.dt,t2.dt,t1.videoid,t2.current_vv_ep,t2.talent_video
)t
join 
    (
    SELECT  vid,dt,
    count(case when event='like' then 1 else null end) as like_count,
    count(case when event='comment_success' then 1 else null end) as comm_count,
    count(case when event='video_play' then 1 else null end) as vv
    from dwb_v8sp.event_column_info_new_hour
    where dt>='2019-01-01'
    and dt <= '2019-01-06'
    and event in ('comment_success','like','video_play')
    group by vid,dt
    )t3
on t.moneydt = t3.dt
    and t.vid=t3.vid
GROUP BY t.makedt,t.moneydt
