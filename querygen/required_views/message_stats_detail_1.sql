-- This view is required for the querygen created SQL.
DROP VIEW IF EXISTS message_stats_detail_1;
CREATE OR REPLACE VIEW public.message_stats_detail_1 AS 
 SELECT d.project
    ,d.deployment
    ,d.deploymentnumber
    ,pd.languagecode
    ,cp.contentpackage
    ,a.village
    ,cp.contentid
    ,cc.categoryid AS acm_categoryid      -- or cacm.categoryid
    ,cc.categoryname AS acm_categoryname  -- or cacm.categoryname
    ,a.talkingbook
    ,a.played_seconds_max
    ,a.completed_max

   -- the most recent revision of every deployment
   FROM deployments d
     -- to get languagecode for (project,deployment) (also gets contentpackage, but we already have that)
     JOIN packagesindeployment pd ON d.deployment = pd.deployment AND d.project = pd.project
     -- to get all of the unique contentid for (project,contentpackage). 
     JOIN contentinpackage_uniquecontent cp ON cp.project = pd.project AND cp.contentpackage = pd.contentpackage
     -- to get one categoryid for (project,contentid) (The "leftmost" category id in the list of categories from the ACM)
     JOIN content_unique_categories cc ON cp.project = cc.project AND cp.contentid = cc.contentid
     -- we don't actually retrieve anything from categories, but this speeds up the query
     JOIN categories cacm ON cacm.projectcode = cp.project AND cacm.categoryid = cc.categoryid
     -- we don't actually retrieve anything from contentmetadata2, but this speeds up the query
     JOIN contentmetadata2 cm ON cp.project = cm.project AND cp.contentid = cm.contentid
     -- we don't actually retrieve anything from languages, but this speeds up the query substantially
     JOIN languages l ON l.projectcode = d.project AND l.languagecode = pd.languagecode
     -- to get village, talkingbook, played_seconds_max, completed_max for (project, contentpackage, contentid)
     JOIN allsources_s a ON a.project = d.project AND a.contentpackage = cp.contentpackage AND a.contentid = cp.contentid
