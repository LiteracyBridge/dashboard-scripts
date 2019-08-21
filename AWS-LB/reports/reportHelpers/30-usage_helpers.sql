-- Helpers for usage queries.

-- Like contentinpackage, but with support for categories defined as a title from the ACM.
-- Such categories have the contentid as the categoryid.
-- Get the category name from categories or from contentmetadata2, as appropriate.
CREATE OR REPLACE TEMP VIEW content_cats_in_package AS (
    SELECT DISTINCT
        cip.project, 
        cip.contentpackage, 
        cip.contentid, 
        cip.categoryid, 
        cip.order AS position, 
        CASE WHEN cat.categoryname:: TEXT~~'General%':: TEXT
             THEN "substring"(cat.categoryname:: TEXT, 9):: CHARACTER VARYING
             ELSE CASE when categoryname IS NULL THEN title ELSE categoryname END
        END AS category,
        cm2.sdg_goals,
        cm2.sdg_targets
    FROM contentinpackage cip 
    LEFT OUTER JOIN categories cat ON cip.categoryid=cat.categoryid AND cip.project=cat.projectcode
    LEFT OUTER JOIN contentmetadata2 cm2 ON cip.categoryid=cm2.contentid AND cip.project=cm2.project 
);


-- Adds the languagecode, title, format, and duration_seconds from contentmetadata2, 
-- order (within playlist) from contentinpackage, and the category (name) from categories.
-- Commented out lines below swap in 'content_cats_in_package' to replace 'contentinpackage'
CREATE OR REPLACE TEMP VIEW usage_info_base_view AS (
    SELECT DISTINCT
      ps.timestamp,
      ps.project,
      ps.contentpackage,
      cm.languagecode,
      ps.recipientid,
      -- CASE WHEN cat.categoryname:: TEXT~~'General%':: TEXT
      --   THEN "substring"(cat.categoryname:: TEXT, 9):: CHARACTER VARYING
      -- ELSE cat.categoryname
      -- END             AS category,
      cp.category,
      cp.category as playlist,
      cp.sdg_goals,
      cp.sdg_targets,
      ps.contentid,
      cm.title,
      cm.format,
      cm.duration_sec AS duration_seconds,
      cp.position,
      --Use like: STRING_AGG(DISTINCT CAST(position AS TEXT), ';') AS position_list,
      ps.talkingbookid,
      ps.played_seconds,
      ps.completed as completions
    FROM playstatistics ps
      JOIN contentmetadata2 cm
        ON ps.contentid = cm.contentid AND ps.project=cm.project
      JOIN content_cats_in_package cp 
        ON ps.contentpackage = cp.contentpackage AND ps.contentid=cp.contentid
      -- JOIN contentinpackage cp
      --   ON ps.contentpackage = cp.contentpackage AND ps.contentid = cp.contentid
      -- JOIN categories cat
      --   ON cat.categoryid = cp.categoryid AND cat.projectcode=cp.project
) ;

-- Adds the language name from language, deploymentnumber and startdate from deployments, 
-- deployment (name) from packagesindeployment, and partner, affiliate, country, 
-- region, district, communityname, groupname, and agent from recipients.
SELECT * INTO TEMPORARY TABLE usage_info_temp FROM (
  SELECT DISTINCT
      uib.timestamp,
      uib.project,

      d.deploymentnumber,
      d.deployment,
      d.startdate,
      uib.contentpackage,
      uib.languagecode,
      l.language,
      uib.recipientid,
      recip.partner,
      recip.affiliate,
      recip.country,
      recip.region,
      recip.district,
      recip.communityname,
      recip.groupname,
      recip.agent,
      uib.talkingbookid,

      uib.category,
      uib.playlist,
      uib.sdg_goals,
      uib.sdg_targets,
      uib.contentid,
      uib.title,
      uib.format,
      uib.duration_seconds,
      uib.position,

      uib.played_seconds,
      uib.completions

    FROM
      usage_info_base_view uib
      JOIN packagesindeployment pid
        ON pid.project=uib.project AND pid.contentpackage = uib.contentpackage
      JOIN deployments d
        ON d.project=uib.project AND d.deployment = pid.deployment
      JOIN languages l ON l.projectcode = uib.project AND l.languagecode = uib.languagecode
      LEFT OUTER JOIN recipients recip
        ON uib.recipientid = recip.recipientid

 ) ui_temp ;

-- Update the usage_info table. We've already done the lengthy query (~15 seconds in early 2019), but
-- the transaction should be quick.
-- If the schema changes, just run one time with drop table usage_info; select * into usage_info from (select * from usage_info_temp);
BEGIN;

-- DROP TABLE usage_info;
-- SELECT * INTO usage_info FROM (SELECT * FROM usage_info_temp)uit;

DELETE FROM usage_info;
INSERT INTO usage_info SELECT * FROM usage_info_temp;

COMMIT;

-- This is a helper to count the number of talking books that reported using content from some
-- given content package.
SELECT * INTO TEMPORARY TABLE package_tbs_used FROM (
    SELECT DISTINCT
      project,
      contentpackage,
      count(DISTINCT talkingbookid) AS package_tbs_used
    FROM playstatistics
    GROUP BY project, contentpackage
) pkg_tbs_used;


-- Usage by project / deployment / package / language / category / message
SELECT * INTO TEMPORARY TABLE usage_by_message FROM (
    SELECT DISTINCT
      ps.project,
      STRING_AGG(DISTINCT ps.deployment, ';') AS deployment,
      ps.deploymentnumber,
      ps.contentpackage,
      languagecode,
      language,
      COUNT(DISTINCT category)           AS num_categories,
      STRING_AGG(DISTINCT category, ';') AS category_list,
      contentid,
      title,
      format,
      STRING_AGG(DISTINCT CAST(position AS TEXT), ';') AS position_list,
      round(duration_seconds/60.0, 1)    AS duration_minutes,
      round(sum(played_seconds)/60.0, 1) AS played_minutes,
      round(sum(played_seconds)/60.0/greatest(MAX(package_tbs_used), 1), 1)
                                         AS played_minutes_per_tb,
      sum(completions)                   AS completions,
      round(sum(completions)/greatest(MAX(package_tbs_used), 1), 1)
                                         AS completions_per_tb,
      count(DISTINCT talkingbookid)      AS num_tbs,
      MAX(ptb.package_tbs_used)          AS num_package_tbs,
      ROUND(100.0*count(DISTINCT talkingbookid)/greatest(MAX(package_tbs_used), 1), 0)
                                         AS percent_tbs_playing
    FROM
      usage_info ps
      JOIN package_tbs_used ptb
        ON ptb.project=ps.project AND ptb.contentpackage = ps.contentpackage
    GROUP BY
      ps.project,
      ps.deploymentnumber,
      ps.contentpackage,
      languagecode,
      language,
      contentid,
      title,
      format,
      duration_seconds
    ORDER BY project, contentpackage, title
) us_by_msg;


-- Usage summarized by deployment and category (aggregated across packages)
SELECT * INTO TEMPORARY TABLE usage_by_package_category FROM (
    SELECT DISTINCT
      cats.project,
      cats.deploymentnumber,
      STRING_AGG(DISTINCT cats.deployment, ';') AS deployment,
      cats.contentpackage,
      cats.languagecode,
      cats.language,
      cats.category,
      COUNT(DISTINCT cats.contentid)            AS num_messages,
      ROUND(SUM(duration_seconds)/60.0, 0)      AS duration_minutes,
      round(MAX(tbinfo.played_seconds)/60.0, 0) AS played_minutes,
      MAX(tbinfo.completions)                   AS completions,
      MAX(tbinfo.cat_tbs)                       AS cat_tbs,
      MAX(ptb.package_tbs_used)                 AS pkg_tbs
    -- The basics of a usage query
    FROM
      (
        SELECT DISTINCT
          project,
          deploymentnumber,
          deployment,
          startdate,
          contentpackage,
          languagecode,
          language,
          category,
          contentid,
          duration_seconds
        FROM usage_info
        GROUP BY
          project,
          deploymentnumber,
          deployment,
          startdate,
          contentpackage,
          languagecode,
          language,
          category,
          contentid,
          duration_seconds
      ) cats
      -- Data that is per-talkingbook (talkingbook, played duration, completions)
      JOIN
      (SELECT DISTINCT
         project,
         deploymentnumber,
         contentpackage,
         category,
         count(talkingbookid)          AS all_tbs,
         count(DISTINCT talkingbookid) AS cat_tbs,
         sum(played_seconds)           AS played_seconds,
         sum(completions)              AS completions
       FROM usage_info
       GROUP BY project, deploymentnumber, contentpackage, category
      ) tbinfo
        ON tbinfo.project=cats.project AND tbinfo.deploymentnumber=cats.deploymentnumber
           AND tbinfo.contentpackage ilike cats.contentpackage AND tbinfo.category ilike cats.category
      JOIN
      package_tbs_used ptb
        ON ptb.project=cats.project AND ptb.contentpackage ilike cats.contentpackage

    GROUP BY cats.project,
      cats.deploymentnumber,
      cats.contentpackage,
      cats.languagecode,
      cats.language,
      cats.category
    ORDER BY project
        ,category
) us_by_pkg_cat;


-- Usage at a by project and deployment.
SELECT * INTO TEMPORARY TABLE usage_by_deployment FROM (
    SELECT DISTINCT
      project,
      STRING_AGG(DISTINCT deployment, ';') AS deployment,
      deploymentnumber,
      startdate,
      COUNT(DISTINCT contentpackage)     AS num_packages,
      COUNT(DISTINCT languagecode)       AS num_languages,
      COUNT(DISTINCT communityname)      AS num_communities,
      COUNT(DISTINCT recipientid)        AS num_recipients,
      COUNT(DISTINCT category)           AS num_categories,
      COUNT(DISTINCT contentid)          AS num_messages,
      COUNT(DISTINCT talkingbookid)      AS num_tbs,
      ROUND(SUM(played_seconds)/60.0, 2) AS played_minutes,
      SUM(completions)                   AS num_completions

    FROM usage_info ui
    GROUP BY project,
        deploymentnumber,
        startdate
    ORDER BY project, startdate
) us_by_depl;


-- Report the last 4 usage counts for every project
CREATE OR REPLACE TEMP VIEW usage_dashboard AS (
    SELECT
      project,
      deployment,
      deploymentnumber,
      startdate,
      num_packages,
      num_communities,
      num_tbs
    FROM (
           SELECT
             ROW_NUMBER()
             OVER (PARTITION BY project
               ORDER BY startdate DESC) AS row,
             ubd.*
           FROM
             usage_by_deployment ubd) extract
    WHERE
      extract.row<=4
);


