SELECT distinct title,packagename as "Package",
    CASE WHEN cat.categoryname LIKE 'General%' THEN substring(cat.categoryname,9) ELSE cat.categoryname END AS Category,
    "order",
	format,
	round(duration_sec/60,1) AS Length_Minutes,
	round(timeplayed/60/pkgTbs,0) as Minutes_Per_TB,
	round(EffectiveCompletions/pkgTbs,1) as Eff_Completions_Per_TB,
	round(timeplayed/60,0) as Total_Minutes_Played, 
	completed as Total_Completions,EffectiveCompletions as Total_Effective_Completions,
	CASE WHEN completed > 0 THEN round((EffectiveCompletions-completed)/completed*100,0)
	 	 ELSE 0 END as Partial_Percentage,
	TBs, pkgTbs, TBs*100/pkgTbs as "TB%"
FROM
(SELECT contentid,project,contentpackage,sum(played_seconds_max) AS timeplayed,
	sum(EffectiveCompletions_Max) as EffectiveCompletions,
	sum(completed_max) as completed,
	count(distinct talkingbook) as TBs,
	(select count(distinct talkingbook) from allsources_s aa where aa.contentpackage=a.contentpackage and a.project = aa.project) as pkgTbs
  FROM allsources_s a
  WHERE project = :'prj'
  GROUP BY contentid,project,contentpackage) as contentstats

  JOIN contentmetadata2 cm
  on contentstats.contentid = cm.contentid
  and contentstats.project = cm.project
  JOIN contentinpackage cp
  ON contentstats.contentpackage = cp.contentpackage
  AND contentstats.contentid = cp.contentid
  JOIN categories cat
  ON cat.categoryid = cp.categoryid
  AND cat.projectcode=cp.project
  JOIN packagesindeployment d
  ON d.contentpackage = cp.contentpackage
  WHERE d.contentpackage = :'pkg'
  ORDER BY effectivecompletions desc
