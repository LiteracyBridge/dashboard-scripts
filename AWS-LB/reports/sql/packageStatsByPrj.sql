
SELECT distinct contentstats.project,  d.deployment, packagename as "Package", d."startDate",
	Msgs, duration_min, 
	round(completed/tbs/msgs,1) as repititions,
	round(timeplayed/60/TBs,0) as Minutes_Per_TB,
	round(EffectiveCompletions/TBs,1) as Eff_Completions_Per_TB,
	round(timeplayed/60,0) as Total_Minutes_Played, 
	completed as Total_Completions,EffectiveCompletions as Total_Effective_Completions,
	CASE WHEN completed > 0 THEN round((EffectiveCompletions-completed)/completed*100,0)
	 	 ELSE 0 END as Partial_Percentage,
	TBs,
(select count(*) from
(select project,contentpackage,count(distinct contentid) as msgsPlayed
from allsources_s a
WHERE contentid in (select contentid from contentinpackage where project=a.project and contentpackage=a.contentpackage)
group by project,contentpackage,talkingbook
having project=contentstats.project and contentpackage=contentstats.contentpackage
) foo
where msgsplayed = (select count(distinct contentid) from allsources_s aa where aa.contentpackage = contentstats.contentpackage and aa.project=contentstats.project and contentid in (select contentid from contentinpackage where project=aa.project and contentpackage=aa.contentpackage))
group by project, contentpackage
) as GoodTBs,	
(select count(*) from
(select project,contentpackage,count(distinct contentid) as msgsPlayed
from allsources_s a
WHERE contentid in (select contentid from contentinpackage where project=a.project and contentpackage=a.contentpackage)
group by project,contentpackage,talkingbook
having project=contentstats.project and contentpackage=contentstats.contentpackage
) foo
where msgsplayed < 0.5 * (select count(distinct contentid) from allsources_s aa where aa.contentpackage = contentstats.contentpackage and aa.project=contentstats.project and contentid in (select contentid from contentinpackage where project=aa.project and contentpackage=aa.contentpackage))
group by project, contentpackage
) as BadTBs	
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
WHERE contentstats.project=:'prj'
--  AND deployment='2015-2'
ORDER BY contentstats.project, d."startDate", packagename
