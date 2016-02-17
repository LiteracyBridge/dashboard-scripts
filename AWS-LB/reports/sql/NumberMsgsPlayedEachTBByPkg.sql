COPY (select project,contentpackage,talkingbook,village,
count(distinct contentid) as msgsPlayed,
(select count(distinct contentid) from allsources_s aa 
where aa.contentpackage = a.contentpackage and aa.project=a.project
and contentid in (select contentid from contentinpackage where project=a.project and contentpackage=a.contentpackage)
) as msgsTotal
from allsources_s a
WHERE contentid in (select contentid from contentinpackage where project=a.project and contentpackage=a.contentpackage)
group by project,contentpackage,village,talkingbook
having project=:'prj' and contentpackage= :'pkg'
order by count(distinct contentid) desc,village,talkingbook) TO STDOUT (FORMAT csv, HEADER true);
