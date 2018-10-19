    --This takes 30-45 minutes 

-- update case in these smaller tables
update categoriesinpackage
set contentpackage = upper(contentpackage),
project = upper(project);

update contentinpackage
set contentpackage = upper(contentpackage),
project = upper(project);


update packagesindeployment
set contentpackage = upper(contentpackage),
deployment = upper(deployment),
project = upper(project);

update surveyevents
set packageid = upper(packageid);

update recordevents
set packageid = upper(packageid);

update tbdataoperations
set outimage=upper(outimage),
inimage=upper(inimage);

-- Just output all packages not matching packagesindeployment in case there is a case issue
-- The dashboard Java code should already be forcing all new entries to be upper case only.
select distinct contentpackage from syncaggregation
where contentpackage not in (select contentpackage from packagesindeployment)
order by contentpackage;

select distinct packageid from playedevents
where packageid not in (select contentpackage from packagesindeployment)
order by packageid;


delete from tbcollections;
--insert into tbcollections
--(select distinct contentpackage,village,talkingbook from syncaggregation
--union
--select distinct packageid as contentpackage,village,talkingbookid as talkingbook from playedevents);


delete from allsources_s;
--insert into allsources_s select * from allsources;
