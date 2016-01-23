select pd.project,pd.deployment, 
 count(distinct s.talkingbook) as "TBs",
 CASE WHEN datasource=2 THEN sum(countcompleted) ELSE 0 END as stats, 
 CASE WHEN datasource=3 THEN sum(countcompleted) ELSE 0 END as flash,
 sum(totaltimeplayed)/3600 AS "Played_Hours_Flash"
from syncaggregation s
join contentinpackage cp 
  on s.contentid = cp.contentid
  and s.contentpackage = cp.contentpackage
join packagesindeployment pd
  on cp.contentpackage = pd.contentpackage
  and cp.project = pd.project
where datasource IN (2,3)
group by pd.project,pd.deployment,datasource
order by pd.project,min(pd."startDate"),pd.deployment,datasource
