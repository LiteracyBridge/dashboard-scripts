SELECT distinct contentstats.project,  
	d.deployment,
	contentstats.contentpackage, 
	packagename, 
	d.startdate,
	round(timeplayed/3600,1) as hoursplayed, 
	msgs, 
	duration_min, 
	round(timeplayed/3600/TBs,0) as hourspertb,
--	EffectiveCompletions as effmsgcompletions,
	completed,
	round(completed/tbs/msgs,1) as completionspertbpermsg,
	round(EffectiveCompletions/TBs,1) as effcompletionspertb,
--	CASE WHEN completed > 0 THEN round((EffectiveCompletions-completed)/completed*100,0)
--	 	 ELSE 0 END as Partial_Percentage,
	TBs AS tbcount,

round((select count(*) from
(select project,contentpackage,count(distinct contentid) as msgsPlayed
from allsources_s a
WHERE contentid in (select contentid from contentinpackage where project=a.project and contentpackage=a.contentpackage)
group by project,contentpackage,talkingbook
having project=contentstats.project and contentpackage=contentstats.contentpackage
) foo
where msgsplayed = (select count(distinct contentid) from allsources_s aa where aa.contentpackage = contentstats.contentpackage and aa.project=contentstats.project and contentid in (select contentid from contentinpackage where project=aa.project and contentpackage=aa.contentpackage))
group by project, contentpackage
)::decimal / tbs,2) AS tbsplayingallmsgspct,	

round((select count(*) from
(select project,contentpackage,count(distinct contentid) as msgsPlayed
from allsources_s a
WHERE contentid in (select contentid from contentinpackage where project=a.project and contentpackage=a.contentpackage)
group by project,contentpackage,talkingbook
having project=contentstats.project and contentpackage=contentstats.contentpackage
) foo
where msgsplayed >= 0.5 * (select count(distinct contentid) from allsources_s aa where aa.contentpackage = contentstats.contentpackage and aa.project=contentstats.project and contentid in (select contentid from contentinpackage where project=aa.project and contentpackage=aa.contentpackage))
and msgsplayed != (select count(distinct contentid) from allsources_s aa where aa.contentpackage = contentstats.contentpackage and aa.project=contentstats.project and contentid in (select contentid from contentinpackage where project=aa.project and contentpackage=aa.contentpackage))
group by project, contentpackage
)::decimal / tbs,2) as tbsplayinghalfnotallmsgspct,


round((select count(*) from
(select project,contentpackage,count(distinct contentid) as msgsPlayed
from allsources_s a
WHERE contentid in (select contentid from contentinpackage where project=a.project and contentpackage=a.contentpackage)
group by project,contentpackage,talkingbook
having project=contentstats.project and contentpackage=contentstats.contentpackage
) foo
where msgsplayed < 0.5 * (select count(distinct contentid) from allsources_s aa where aa.contentpackage = contentstats.contentpackage and aa.project=contentstats.project and contentid in (select contentid from contentinpackage where project=aa.project and contentpackage=aa.contentpackage))
group by project, contentpackage
)::decimal / tbs,2) as tbsplayinglessthanhalfmsgspct

FROM
(SELECT a.project,a.contentpackage,
	count(distinct contentid) as Msgs,
	(select round(sum(duration_sec)/60,0) from contentmetadata2 cm join contentinpackage cp on cm.contentid=cp.contentid where cp.contentpackage=a.contentpackage) as duration_min,
	sum(played_seconds_max) AS timeplayed,
	sum(EffectiveCompletions_Max) as EffectiveCompletions,
	sum(completed_max) as completed,
	count(distinct talkingbook) as TBs
  FROM allsources_s a
  WHERE contentid IN (select distinct contentid from contentinpackage WHERE contentpackage=a.contentpackage)
  GROUP BY a.project,a.contentpackage
) as contentstats
JOIN packagesindeployment d
ON d.contentpackage = contentstats.contentpackage
and d.project = contentstats.project
WHERE contentstats.project= :'prj'
ORDER BY contentstats.project, d.startdate, packagename
