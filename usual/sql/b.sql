SELECT
    t1.cnn,
    t2.manufacture,
    count(distinct t2.udid)
from
    (
        seLECT
            cnn
        FROM
            dwb_v8sp.client_log
        WHERE
            cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
            and cnn != ''
            and cnn != '_oppo'
            and plat = '1'
            and dt >= '2018-12-01'
            and dt <= '2018-12-20'
    ) as t1
    join (
        SELECT
            manufacture
        FROM
            dim_v8sp.device_info
        where
            --add_time >= '2018-11-01T00:00:00'--AND
            plat = '1'
            and pcid = 'cm1'
    ) as t2 on t1.udid = t2.udid
group by
    t1.cnn,
    t2.manufacture,
    t2.pcid -- -黑名单渠道
    --黑名单渠道
    -- bbk手机的渠道信息
select
    http_x_forwarded_for,
    udid,
    pcid_current,
    pcid_origin,
    model,
    imei
from
    dwb_v8sp.client_log
where
    manufacture = 'BBK'
    and cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
    and cnn != ''
    AND cnn != '_oppo'
    and plat = '1'
    and dt >= '2018-12-01'
    and dt <= '2018-12-20'
group by
    http_x_forwarded_for,
    udid,
    pcid_current,
    pcid_origin,
    model,
    imei
select
    http_x_forwarded_for,
    udid,
    pcid_current,
    pcid_origin,
    model,
    imei
from
    dwb_v8sp.client_log
where
    cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
    and cnn != ''
    AND cnn != '_oppo'
    and plat = '1'
    and dt >= '2018-12-01'
    and dt <= '2018-12-20'
group by
    http_x_forwarded_for,
    udid,
    pcid_current,
    pcid_origin,
    model -- 播放详细信息
    -- 播放详细信息
SELECT
    sum(t2.video_over_duration),
    sum(t2.video_over_num),
    sum(t2.post_comment_num),
    t2.dt,
    count(distinct udid)
from
    (
        select
            udid
        from
            dwb_v8sp.client_log
        where
            cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
            and cnn != ''
            AND cnn != '_oppo'
            and plat = '1'
            and dt >= '2018-12-01'
            and dt <= '2018-12-20'
    ) t1
    join (
        select
            video_over_duration,
            udid,
            video_over_num,
            post_comment_num,
            dt
        from
            dwb_v8sp.udid_stat_info
        where
            dt >= '2018-12-01'
            and dt <= '2018-12-25'
    ) t2 on t1.udid = t2.udid
GROUP by
    t2.dt - -- - imei为空的pcid(device_info)信息
SELECT
    t1.cnn,
    t1.manufacture,
    t1.model,
    count(distinct t2.udid) as count1,
    t2.pcid
from
    (
        seLECT
            cnn,
            manufacture,
            udid,
            model
        FROM
            dwb_v8sp.client_log
        WHERE
            imei = ''
            and cnn != '6385e22aa2a95b3779af2701c98a4478_yyb'
            and cnn != ''
            and cnn != '_oppo'
            and plat = '1'
            and dt >= '2018-12-01'
            and dt <= '2018-12-20'
    ) as t1
    join (
        SELECT
            pcid,
            udid
        FROM
            dim_v8sp.device_info
        where
            add_time >= '2018-11-01T00:00:00'
    ) as t2 on t1.udid = t2.udid
group by
    t1.cnn,
    t1.manufacture,
    t1.model,
    t2.pcid -- 某段时间的活跃留存
select
    t.dt as dt,
    t.cnn as cnn,(
        case when t.diffday = 0 then '0_day' when t.diffday = 1 then '1_day' when t.diffday = 2 then '2_day' when t.diffday = 3 then '3_day' when t.diffday = 4 then '4_day' when t.diffday = 5 then '5_day' when t.diffday = 6 then '6_day' when t.diffday = 7 then '7_day' else null end
    ) as RETENTION_DAYS,
    count(distinct t.udid) as RETENTION_USERS
from
    (
        select
            t2.dt as dt,
            t2.cnn as cnn,
            t1.udid as udid,
            datediff(t1.dt, t2.dt) as diffday
        from
            (
                select
                    dt,
                    udid as udid
                from
                    dwb_v8sp.event_column_info_new
                WHERE
                    dt >= '2018-12-01'
                    and dt <= '2018-12-25'
                    and source = 'android'
                    and pname = 'com.melon.lazymelon'
                    and lower(event) not like '%push%'
                    and event != 'corner_mark_show'
                group by
                    dt,
                    udid
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
    t.cnn,(
        case when t.diffday = 0 then '0_day' when t.diffday = 1 then '1_day' when t.diffday = 2 then '2_day' when t.diffday = 3 then '3_day' when t.diffday = 4 then '4_day' when t.diffday = 5 then '5_day' when t.diffday = 6 then '6_day' when t.diffday = 7 then '7_day' else null end
    )
order by
    cnn,
    dt;-- 黑名单评论渗透率
    -- 黑名单评论渗透率
SELECT
    t2.dt,
    t1.imei
from
    (
        seLECT
            distinct udid,
            imei
        FROM
            dwb_v8sp.client_log
        WHERE
            cnn != '6385e22aa2a95b3779af2701c98a4478_yyb' -- and imei = ''
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
        group by
            dt,
            udid
    ) as t2 on t1.udid = t2.udid
where
    t2.pv != 0
group by
    t2.dt,
    t1.imei
select
    t.dt as dt,
    t.cnn as cnn,(
        case when t.diffday = 0 then '0_day' when t.diffday = 1 then '1_day' when t.diffday = 2 then '2_day' when t.diffday = 3 then '3_day' when t.diffday = 4 then '4_day' when t.diffday = 5 then '5_day' when t.diffday = 6 then '6_day' when t.diffday = 7 then '7_day' else null end
    ) as RETENTION_DAYS,
    count(distinct t.udid) as RETENTION_USERS
from
    (
        select
            t2.dt as dt,
            t1.cnn as cnn,
            t1.udid as udid,
            datediff(day, t1.dt, t2.dt) as diffday
        from
            (
                seLECT
                    cnn,
                    udid
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
    t.cnn,
    (
        case when t.diffday = 0 then '0_day' when t.diffday = 1 then '1_day' when t.diffday = 2 then '2_day' when t.diffday = 3 then '3_day' when t.diffday = 4 then '4_day' when t.diffday = 5 then '5_day' when t.diffday = 6 then '6_day' when t.diffday = 7 then '7_day' else null end
    )
order by
    cnn,
    dt;

    
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



#注册时间在一月之前，一月内有连续三天登陆
SELECT
    udid
from
    dim_v8sp.device_info
where
    udid in ()
    and substring(add_time,1,10) <='2108-12-01' 



SELECT t1.pv ,t1.vid,t2.talent
from
(
SELECT vid,count(*)as pv
from dwb_v8sp.event_column_info_new_hour
WHERE dt ='2019-01-09'
and event='effective_play'
GROUP BY vid
)t1
join 
(SELECT  talent,talent_video
FROM dwb_v8sp.talent_video_basic_metrcs_clo
where  dt = '2019-01-09'
and profit_type = 'underway'
and talent not in (
select original_id as talent from dim_v8sp.original_user
)
and current_vv_ep > 0
and talent in ('2500369731261829313','2499389068056465537')
GROUP BY talent,talent_video 
)t2
on  t1.vid = t2.talent_video
GROUP BY t1.vid,t2.talent


select DATA_DATE,VERSION,
    count(distinct case when dau>0 then udid else null end) as DAU,
    sum(case when AUDIO_PLAY_PV>0 then AUDIO_PLAY_PV else 0 end) as AUDIO_PLAY_PV,
    count(distinct case when AUDIO_PLAY_PV>0 then udid else null end) as AUDIO_PLAY_UV,
    sum(case when AUDIO_DURATION<86400 then AUDIO_DURATION else 0 end) as AUDIO_DURATION,
    sum(case when ENTER_AUDIO_PV>0 then ENTER_AUDIO_PV else 0 end) as ENTER_AUDIO_PV,
    count(distinct case when ENTER_AUDIO_PV>0 then udid else null end) as ENTER_AUDIO_UV,
    sum(case when POST_AUDIO_PV>0 then POST_AUDIO_PV else 0 end) as POST_AUDIO_PV,
    count(distinct case when POST_AUDIO_PV>0 then udid else null end) as POST_AUDIO_UV,
    sum(case when AUDIO_CANCEL_PV>0 then AUDIO_CANCEL_PV else 0 end) as AUDIO_CANCEL_PV,
    count(distinct case when AUDIO_CANCEL_PV>0 then udid else null end) as AUDIO_CANCEL_UV,
    sum(case when AUDIO_SHORT_PV>0 then AUDIO_SHORT_PV else 0 end) as AUDIO_SHORT_PV,
    count(distinct case when AUDIO_SHORT_PV>0 then udid else null end) as AUDIO_SHORT_UV,
    sum(case when AUDIO_SUCCESS_PV>0 then AUDIO_SUCCESS_PV else 0 end) as AUDIO_SUCCESS_PV,
    count(distinct case when AUDIO_SUCCESS_PV>0 then udid else null end) as AUDIO_SUCCESS_UV,
    sum(case when AUDIO_FAIL_PV>0 then AUDIO_FAIL_PV else 0 end) as AUDIO_FAIL_PV,
    count(distinct case when AUDIO_FAIL_PV>0 then udid else null end) as AUDIO_FAIL_UV,
    sum(case when LOGIN_PAGE_PV>0 then LOGIN_PAGE_PV else 0 end) as LOGIN_PAGE_PV,
    count(distinct case when LOGIN_PAGE_PV>0 then udid else null end) as LOGIN_PAGE_UV,
    count(distinct case when LOGIN_SUCCESS_PV>0 then udid else null end) as LOGIN_SUCCESS_UV,
    sum(case when LIKE_AUDIO>0 then LIKE_AUDIO else 0 end) as LIKE_AUDIO,
    sum(case when LIKE_COMMENT>0 then LIKE_COMMENT else 0 end) as LIKE_COMMENT
    from 
    (
        SELECT dt as DATA_DATE, vapp as VERSION,udid,
        count(case when lower(event) not like '%push%' and event!='corner_mark_show' then 1 else null end) as dau,
        count(case when event = 'audio_play' then 1 else null end) as AUDIO_PLAY_PV,
        sum(case when event = 'audio_over' then duration else 0 end) as AUDIO_DURATION,
        count(case when event = 'enter_audio' then 1 else null end) as ENTER_AUDIO_PV,  
        count(case when event = 'post_audio' then 1 else null end) as POST_AUDIO_PV, 
        count(case when event = 'audio_cancel' then 1 else null end) as AUDIO_CANCEL_PV,
        count(case when event = 'audio_short' then 1 else null end) as AUDIO_SHORT_PV,
        count(case when event = 'audio_success' then 1 else null end) as AUDIO_SUCCESS_PV,  
        count(case when event = 'audio_fail' then 1 else null end) as AUDIO_FAIL_PV,
        count(case when event = 'login_page' and lower(body_source) = 'audio' then 1 else null end) as LOGIN_PAGE_PV,
        count(case when event = 'login_success' and lower(body_from) = 'audio' then 1 else null end) as LOGIN_SUCCESS_PV,
        count(case when event = 'like_audio' then 1 else null end) as LIKE_AUDIO,
        count(case when event = 'like_comment' then 1 else null end) as LIKE_COMMENT
        FROM dwb_v8sp.event_column_info_new_hour 
        WHERE dt='2019-01-17' and source='android' and pname='com.melon.lazymelon' ,pcid_origin = 'cm1',vapp in ('3010','2740')
        group by dt,vapp,udid
    ) t1
    group by DATA_DATE,VERSION



    SELECT created.dt as DATA_DATE, 
     origin.cnt_video_play as CNT_VIDEO_PLAYED, 
     created.video_played_with_audio_comment as VIDEO_PLAYED_WITH_AUDIO_COMMENT, 
     created.cnt_audio_comment as CNT_AUDIO_COMMENT
FROM (select t.dt as dt,
    sum(t.video_played_with_audio_comment) as video_played_with_audio_comment,
    sum(t.cnt_audio_comment) as cnt_audio_comment
    from
      (SELECT t1.dt as dt, t1.video_played_with_audio_comment as video_played_with_audio_comment, t2.cnt_audio_comment as cnt_audio_comment
        FROM
          (select dt, vid,
              count(1) as video_played_with_audio_comment
             from dwb_v8sp.event_column_info_new_hour
             where dt = '${date}'
               and event = 'video_play'
               and source='android'
               and nvl(cast(vapp as int),0) >= 2500
            group by dt, vid
           ) t1
        JOIN 
          (select cid,
            count(distinct comment_id) as cnt_audio_comment
             from dim_v8sp.comment_info
           where substring(add_time,1,10) <= '${date}'
             and status = 2
             and comment_type = 4
           group by cid
          ) t2
        ON t1.vid = t2.cid
       ) t
    group by t.dt
   ) created
JOIN (select dt, count(1) as cnt_video_play
    from dwb_v8sp.event_column_info_new_hour
    where dt = '${date}' 
     and event = 'video_play'
     and source = 'android'
     and nvl(cast(vapp as int),0) >= 2500
     group by dt
   ) origin
ON created.dt = origin.dt
    ]]>







#test 
select count(distinct t.udid) as uv ,
(
    case when 
        t.client_show >50 
        -- and t.duration >1500 
        -- and t.event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        -- ,'share','side_enter','author_page','ugc_start','barpage')
        -- and t.style not in ('auto_clk','auto_next')
    then '`成熟期`'
    when 
        t.client_show>20 
        -- and t.duration>600 
        -- and t.event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        -- ,'share','side_enter','author_page','ugc_start','barpage')
        -- and t.style not in ('auto_clk','auto_next')
    then '`探索期`'
    when 
        t.client_show >10 
        -- and t.event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        -- ,'share','side_enter','author_page','ugc_start','barpage')
        -- and t.style not in ('auto_clk','auto_next')
    then '`体验期`'
    when 
        t.client_show >10
    then '`接触期`'
    else 
    '`初体验`'
    end 
)
from 
(
    select udid,event,style,
    count( case when event ='client_show' then 1 else null end )as client_show,
    sum( case when event ='video_over' then duration else null end )as duration 
    from dwb_v8sp.event_column_user_type
    where
    dt = '${date}'
    and user_type ='new'
    and udid not in 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where user_type ='old'
            and event = 'app_start'
            and dt = date_add('${date}',1)
        GROUP BY udid
    )
    group by udid,event,style
) t
group by 
(
    case when 
        t.client_show >50 
        -- and t.duration >1500 
        -- and t.event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        -- ,'share','side_enter','author_page','ugc_start','barpage')
        -- and t.style not in ('auto_clk','auto_next')
    then '`成熟期`'
    when 
        t.client_show>20 
        -- and t.duration>600 
        -- and t.event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        -- ,'share','side_enter','author_page','ugc_start','barpage')
        -- and t.style not in ('auto_clk','auto_next')
    then '`探索期`'
    when 
        t.client_show >10 
        -- and t.event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        -- ,'share','side_enter','author_page','ugc_start','barpage')
        -- and t.style not in ('auto_clk','auto_next')
    then '`体验期`'
    when 
        t.client_show >10
    then '`接触期`'
    else 
    '`初体验`'
    end 
)



select count(distinct a.udid)
from
(
select 
-- count( t.udid) as uv ,
udid,
(
    case when 
        t.client_show >50 
    then '50'
    when 
        t.client_show>20 
    then '20'
    when 
        t.client_show >10 
    then '10'
    else 
    'low10'
    end 
) as client,
(
    case when 
        t.duration >1500
        then '25min'
        when t.duration > 600
        then '10min'
        else 
        'low10min'
        end
) as duration,
(
    case when t.event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        ,'share','side_enter','author_page','ugc_start','barpage')
        then 'interact'
        else 'no_interact'
        end
) as jiaohu,
(
    case when t.style not in ('auto_clk','auto_next') 
        then 'no_auto'
    else 'auto'
    end 
) as auto
from 
(
    select udid,event,style,
    count( case when event ='client_show' then 1 else null end )as client_show,
    sum( case when event ='video_over' then duration else null end )as duration 
    from dwb_v8sp.event_column_user_type
    where
    dt = '${date}'
    and user_type ='new'
    and udid not in 
    (
        SELECT udid 
        from dwb_v8sp.event_column_user_type
        where user_type ='old'
            and event = 'app_start'
            and dt = date_add('${date}',1)
        GROUP BY udid
    )
    group by udid,event,style
) t
group by 
udid,
(
    case when 
        t.client_show >50 
    then '50'
    when 
        t.client_show>20 
    then '20'
    when 
        t.client_show >10 
    then '10'
    else 
    'low10'
    end 
) ,
(
    case when 
        t.duration >1500
        then '25min'
        when t.duration > 600
        then '10min'
        else 
        'low10min'
        end
) ,
(
    case when t.event in ('audio_play','enter_audio','like_audio','audio_reply','com_layer','like','bar_switch','light_feed_clk'
        ,'share','side_enter','author_page','ugc_start','barpage')
        then 'interact'
        else 'no_interact'
        end
) ,
(
    case when t.style not in ('auto_clk','auto_next') 
        then 'no_auto'
    else 'auto'
    end 
) 
)a
where a.client = 'low10'



