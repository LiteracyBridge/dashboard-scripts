-- TBs Deployed table.

CREATE TABLE public.tbsdeployed
(
  talkingbookid character varying(255) NOT NULL,
  deployedtimestamp timestamp NOT NULL,
  project character varying(255) NOT NULL,
  deployment character varying(255) NOT NULL,
  contentpackage character varying(255) NOT NULL,
  community character varying(255) NOT NULL,
  firmware character varying(255) NOT NULL,
  location character varying(255) NOT NULL,
  coordinates point,
  username character varying(255) NOT NULL,
  tbcdid character varying(255) NOT NULL,
  action character varying(255),
  newsn boolean NOT NULL,
  testing boolean NOT NULL,
  CONSTRAINT tbdeployments_pkey PRIMARY KEY (talkingbookid, deployedtimestamp)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.tbsdeployed
  OWNER TO lb_data_uploader;


-- Query to get correct project, given community and deployment
select talkingbookid, deployedtimestamp, project, deployment, community, dep_project

from (select dep.*, cd.project as dep_project
	from tbsdeployed dep
	join communitydeployments cd on (dep.deployment ilike cd.deployment and dep.community ilike cd.community)
     ) d
order by deployedtimestamp


-- Query to find deployments missing from deployments table.
select distinct project, deployment, contentpackage, MIN(deployedtimestamp) as deployedtimestamp, count(distinct talkingbookid) from tbsdeployed
where deployment not in (select deployment from deployments)
group by project, deployment, contentpackage
order by project, deployedtimestamp


-- Query to find communities in multiple projects (in communities table)
select community 
from (select distinct communityname as community, count(distinct project) from communities group by communityname) c
where count > 1 and not (community = 'UNKNOWN' or community = 'NON-SPECIFIC' or community ilike 'DEMO%')
