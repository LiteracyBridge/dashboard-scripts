insert into communities (communityname, year, tbs, households, languagecode, district, project)

select others.communityname, 2017 as year, 
	r.numtbs as tbs, r.numhouseholds as households, 
	r.language as languagecode, r.district,
	others.project

from (select rm.project, rm.directory as communityname, rm.recipientid 
      from recipients_map rm
      where (not rm.directory in (select communityname from communities))
      ) others
join recipients r
  on others.recipientid = r.recipientid
