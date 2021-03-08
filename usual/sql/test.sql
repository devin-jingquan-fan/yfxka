select t1.body_ab,count(t1.udid)as liucun
(
select udid ,body_ab
from dwb_v8sp.event_cloum_user_type
where 
and dt ='${date}'
and user_type = 'new'
and body_ab in('151_default_0','151_default_1','151_audio_rec_0','151_audio_rec_1') 
AND cast(vapp AS BIGINT) >= 3000) t1
join 
(
select udid 
from dwb_v8sp.event_cloum_user_type
where 
and dt =date_add('${date}',1)
and user_type = 'old'
and event ='app_start'
and body_ab in('151_default_0','151_default_1','151_audio_rec_0','151_audio_rec_1') 
AND cast(vapp AS BIGINT) >= 3000) t2
on t1.udid = t2.udid
group by t1.body_ab