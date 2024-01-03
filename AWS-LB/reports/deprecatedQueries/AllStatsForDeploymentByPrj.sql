COPY (SELECT a.project,deployment,sum(played_seconds_max)/3600 AS hoursplayed,
	sum(EffectiveCompletions_Max) as EffectiveCompletions,
	sum(started_max) as started, 
	sum(quarter_max) as quarter,
	sum(half_max) as half,
	sum(threequarters_max) as threequarters,
	sum(completed_max) as completed,
	sum(completed_logevents) as completed_logevents,
	sum(completed_logs) as completed_logs,
	sum(completed_stats) as completed_stats,
	sum(completed_flash) as completed_flash, 
	sum(completed_min) as completed_min, 
	sum(completed_max) as completed_max,
	sum(completed_variance) as completed_variance
  FROM allsources_s a
  JOIN packagesindeployment d
  ON a.contentpackage=d.contentpackage
  WHERE a.project = :'prj'
  GROUP BY a.project,deployment,d.startdate
  order by d.startdate) to STDOUT (FORMAT csv, HEADER true);
