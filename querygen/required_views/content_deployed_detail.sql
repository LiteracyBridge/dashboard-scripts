-- This view is required for the querygen created SQL.

DROP VIEW IF EXISTS public.content_deployed_detail;
CREATE OR REPLACE VIEW public.content_deployed_detail AS
 WITH contentinpackage_uniquecontent AS (
         SELECT DISTINCT contentinpackage.project,
            contentinpackage.contentpackage,
            contentinpackage.contentid,
            contentinpackage."order" AS sequence
           FROM contentinpackage
        ), content_unique_categories AS (
         SELECT cm_1.project,
            cm_1.contentid,
            c.categoryid,
            c.categoryname
           FROM contentmetadata2 cm_1
             JOIN categories c ON cm_1.categories::text ~~* (c.categoryname::text || '%'::text) AND cm_1.project::text = c.projectcode::text AND NOT (c.categoryid::text IN ( SELECT parent.categoryid
                   FROM categories parent
                     JOIN categories child ON child.categoryid::text ~~ (parent.categoryid::text || '-%'::text)))
        )
 SELECT d.project,
    d.deployment,
    d.deploymentnumber,
    pd.languagecode,
    pd.contentpackage,
    cc.categoryid AS acm_categoryid,
    cc.categoryname AS acm_categoryname,
    cp.contentid,
    cp.sequence,
    cm.duration_sec,
    cm.format,
    cm.title
   -- to get (project, deployment, deploymentnumber)
   FROM deployments d
     -- to get (contentpackage,languagecode) for (project,deployment)
     JOIN packagesindeployment pd ON d.deployment::text = pd.deployment::text AND d.project::text = pd.project::text
     -- to get (contentid) for (project,contentpackage)
     JOIN contentinpackage_uniquecontent cp ON cp.project::text = pd.project::text AND cp.contentpackage::text = pd.contentpackage::text
     -- to get (duration,format) for (project,contentid)
     JOIN contentmetadata2 cm ON cm.project::text = cp.project::text AND cm.contentid::text = cp.contentid::text
     -- to get (categoryid,categoryname) for (project,contentid)
     JOIN content_unique_categories cc ON cp.project::text = cc.project::text AND cp.contentid::text = cc.contentid::text;

ALTER TABLE public.content_deployed_detail
  OWNER TO lb_data_uploader;
COMMENT ON VIEW public.content_deployed_detail
  IS '-- annotated definition

WITH contentinpackage_uniquecontent AS (
 SELECT DISTINCT project,
    contentpackage,
    contentid,
    "order" as sequence
   FROM contentinpackage
),
content_unique_categories AS (
SELECT cm.project,
    cm.contentid,
    c.categoryid,
    c.categoryname
   FROM contentmetadata2 cm
     JOIN categories c
       -- the cm.categories is zero or more category names, separated by commas. The most
       -- salient category names are (tend to be) first, so this matches on that. 
       ON cm.categories ILIKE (c.categoryname || ''%'') 
       AND cm.project = c.projectcode
       AND NOT (
           -- exclude categoryids from the set of non-leaf categoryids
           c.categoryid IN ( 
               -- category ids ''id'' such that there is another categoryid that is ''id-%''
               SELECT parent.categoryid
                   FROM categories parent
                     JOIN categories child 
                       ON child.categoryid LIKE (parent.categoryid || ''-%'')
           )
       ) 
)

 SELECT d.project
    ,d.deployment
    ,d.deploymentnumber
    ,pd.languagecode
    ,pd.contentpackage
    ,cc.categoryid AS acm_categoryid
    ,cc.categoryname AS acm_categoryname
    ,cp.contentid
    ,cp.sequence
    ,cm.duration_sec
    ,cm.format
    ,cm.title

   -- to get (project, deployment, deploymentnumber)
   FROM deployments d
     -- to get (contentpackage,languagecode) for (project,deployment)
     JOIN packagesindeployment pd ON d.deployment = pd.deployment AND d.project = pd.project
     -- to get (contentid) for (project,contentpackage)
     JOIN contentinpackage_uniquecontent cp ON cp.project = pd.project AND cp.contentpackage = pd.contentpackage
     -- to get (duration,format) for (project,contentid)
     JOIN contentmetadata2 cm ON cm.project = cp.project AND cm.contentid = cp.contentid
     -- to get (categoryid,categoryname) for (project,contentid)
     JOIN content_unique_categories cc ON cp.project = cc.project AND cp.contentid = cc.contentid
';
