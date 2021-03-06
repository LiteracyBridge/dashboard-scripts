COPY (select distinct title,
    packagename, 
    CASE WHEN cat.categoryname LIKE 'General%' THEN substring(cat.categoryname,9) ELSE cat.categoryname END AS Category,
    "order",
	format,
	round(duration_sec/60,1) AS Length_Minutes,
    cm.languagecode,
    source,
    keywords,
    categories as ACM_categories,
    transcriptionurl,
    notes     
from contentinpackage cp
join contentmetadata2 cm
on cp.contentid = cm.contentid
and cp.project= cm.project
join categories cat
on cat.categoryid=cp.categoryid
and cat.projectcode=cp.project
JOIN packagesindeployment d
ON d.contentpackage = cp.contentpackage
where cp.contentpackage=:'pkg'
and cp.project=:'prj'
order by CASE WHEN cat.categoryname LIKE 'General%' THEN substring(cat.categoryname,9) ELSE cat.categoryname END,"order") TO STDOUT (FORMAT csv, HEADER true);
