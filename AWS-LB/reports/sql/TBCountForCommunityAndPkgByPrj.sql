select c.project, village, a.contentpackage, count(distinct talkingbook) as "TBs"
from communities c
left join allsources_s a
on a.village=c.communityname
where c.project=:'prj'
group by c.project,village, a.contentpackage

