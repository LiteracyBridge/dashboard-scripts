select cp.project,s.village,cp.contentpackage, count(distinct talkingbook) AS "TBs",sum(countcompleted) as stats,sum(countcompleted)/count(distinct talkingbook) as "ComplPerTB"
from syncaggregation s
join contentinpackage cp 
  on s.contentid = cp.contentid
  and s.contentpackage = cp.contentpackage
join communities c
  on c.communityname = s.village
  and c.project = cp.project
join packagesindeployment pd
  on pd.contentpackage = cp.contentpackage
  and pd.project = cp.project
where datasource =2 
and cp.project=:'prj'
group by cp.project,s.village,pd."startDate",cp.contentpackage
order by cp.project,s.village,pd."startDate",cp.contentpackage

