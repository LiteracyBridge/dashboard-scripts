--
-- TODO: Think of some way to test this that doesn't take 5 minutes per test!
--

-- Helpers for usage queries.

SELECT * INTO TEMPORARY TABLE usage_info_base FROM (
    SELECT
      project,
      contentpackage           AS package,
      contentid,
      village                  AS community,
      talkingbook,
      played_seconds_max       AS played_seconds,
      effectivecompletions_max AS effective_completions,
      completed_max            AS completions
    FROM allsources_s
    GROUP BY
      project,
      package,
      contentid,
      talkingbook,
      village,
      played_seconds,
      effective_completions,
      completions
) us_info_b;

  -- This adds the language, title, and duration_seconds from contentmetadata2, and the category from categories.
SELECT * INTO TEMPORARY TABLE usage_info_base2 FROM (
    SELECT
      cs.project,
      cs.package,
      cm.languagecode,
      cs.community,
      CASE WHEN cat.categoryname:: TEXT~~'General%':: TEXT
        THEN "substring"(cat.categoryname:: TEXT, 9):: CHARACTER VARYING
      ELSE cat.categoryname
      END             AS category,
      cs.contentid,
      cm.title,
      cm.duration_sec AS duration_seconds,
      cs.talkingbook,
      cs.played_seconds,
      cs.effective_completions,
      cs.completions
    FROM
      usage_info_base cs
      JOIN contentmetadata2 cm
        ON cs.contentid=cm.contentid AND cs.project=cm.project
      JOIN contentinpackage cp
        ON cs.package=cp.contentpackage AND cs.contentid=cp.contentid
      JOIN categories cat
        ON cat.categoryid=cp.categoryid AND cat.projectcode=cp.project
) us_info_b2;

  -- This adds the deploymentnumber from deployments, and language from language.
  SELECT * INTO TEMPORARY TABLE usage_info FROM (
  --CREATE TEMPORARY TABLE usage_info AS
  SELECT
      pi.project,
      pi.package,
      pi.languagecode,
      l.language,
      pi.community,
      pi.category,
      pi.contentid,
      pi.title,
      pi.duration_seconds,
      pi.talkingbook,
      pi.played_seconds,
      pi.effective_completions,
      pi.completions,
      pid.deployment,
      d.deploymentnumber,
      pid.startdate

    FROM
      usage_info_base2 pi
      JOIN packagesindeployment pid
        ON pid.project=pi.project AND pid.contentpackage=pi.package
      JOIN deployments d
        ON d.project=pi.project AND d.deployment=pid.deployment
      JOIN languages l ON l.projectcode = pi.project AND l.languagecode = pi.languagecode

 ) us_info;

  -- This is a helper to count the number of talking books that reported using content from some
  -- given content package.
SELECT * INTO TEMPORARY TABLE package_tbs_used FROM (
    SELECT DISTINCT
      project,
      contentpackage  AS package,
      count(DISTINCT talkingbook) AS package_tbs_used
    FROM allsources_s
    GROUP BY project, package
) pkg_tbs_used;

  -- Usage by project / deployment / package / language / category / message
SELECT * INTO TEMPORARY TABLE usage_by_message FROM (
    SELECT DISTINCT
      ci.project,
      ci.deployment,
      ci.deploymentnumber,
      ci.startdate,
      ci.package,
      languagecode,
      language,
      COUNT(DISTINCT category)           AS num_categories,
      contentid,
      title,
      round(duration_seconds/60.0, 1)    AS duration_minutes,
      round(sum(played_seconds)/60.0, 1) AS played_minutes,
      round(sum(played_seconds)/60.0/greatest(MAX(package_tbs_used), 1), 1)
                                         AS played_minutes_per_tb,
      sum(effective_completions)         AS effective_completions,
      round(sum(effective_completions)/greatest(MAX(package_tbs_used), 1), 1)
                                         AS effective_completions_per_tb,
      sum(completions)                   AS completions,
      round(sum(completions)/greatest(MAX(package_tbs_used), 1), 1)
                                         AS completions_per_tb,
      count(DISTINCT talkingbook)        AS num_tbs,
      MAX(ptb.package_tbs_used)          AS num_package_tbs,
      ROUND(100.0*count(DISTINCT talkingbook)/greatest(MAX(package_tbs_used), 1), 0)
                                         AS percent_tbs_playing
    FROM
      usage_info ci
      JOIN package_tbs_used ptb
        ON ptb.project=ci.project AND ptb.package=ci.package
    GROUP BY
      ci.project,
      ci.deployment,
      ci.deploymentnumber,
      ci.startdate,
      ci.package,
      languagecode,
      language,
      contentid,
      title,
      duration_seconds
    ORDER BY project, startdate, package, title
) us_by_msg;

  -- Usage by project / deployment / package / language / category
SELECT * INTO TEMPORARY TABLE usage_by_category FROM (
    SELECT DISTINCT
      ci.project,
      ci.deployment,
      ci.deploymentnumber,
      ci.startdate,
      ci.package,
      ci.languagecode,
      ci.language,
      ci.category,
      count(DISTINCT ci.contentid)              AS num_titles
      --,count(ci.contentid) AS all_titles --debugging, should =num_titles
      ,
      round(sum(ci.duration_seconds)/60.0, 1)   AS duration_minutes,
      round(MAX(tbinfo.played_seconds)/60.0, 1) AS played_minutes,
      MAX(effective_completions)                AS effective_completions,
      MAX(completions)                          AS completions,
      MAX(tbinfo.num_tbs)                       AS num_tbs,
      MAX(package_tbs_used)                     AS num_package_tbs
    -- Data that is per-contentid (contentid, duration)
    FROM
      (SELECT DISTINCT
         project,
         deployment,
         deploymentnumber,
         startdate,
         package,
         languagecode,
         language,
         category,
         contentid,
         duration_seconds
       FROM usage_info
       GROUP BY
         project,
         deployment,
         deploymentnumber,
         startdate,
         package,
         languagecode,
         language,
         category,
         contentid,
         duration_seconds
      ) ci
      -- Data that is per-talkingbook (talkingbook, played duration, completions)
      JOIN (SELECT DISTINCT
              project,
              deploymentnumber,
              package,
              category,
              count(talkingbook)          AS all_tbs,
              count(DISTINCT talkingbook) AS num_tbs,
              sum(played_seconds)         AS played_seconds,
              sum(effective_completions)  AS effective_completions,
              sum(completions)            AS completions
            FROM usage_info
            GROUP BY project, deploymentnumber, package, category
           ) tbinfo
        ON tbinfo.project=ci.project AND tbinfo.deploymentnumber=ci.deploymentnumber
           AND tbinfo.package=ci.package AND
           tbinfo.category=ci.category
      JOIN
      package_tbs_used ptb
        ON ptb.project=ci.project AND ptb.package=ci.package
    GROUP BY ci.project, ci.deployment, ci.deploymentnumber, ci.startdate, ci.package, ci.languagecode, ci.language, ci.category
    ORDER BY project, startdate, package, category
) us_by_cat;

  -- Usage summarized by deployment and category (aggregated across packages)
SELECT * INTO TEMPORARY TABLE usage_by_deployment_category FROM (
    SELECT DISTINCT
      cats.project,
      cats.deployment,
      cats.deploymentnumber,
      cats.startdate,
      COUNT(DISTINCT cats.package)              AS num_packages,
      COUNT(DISTINCT cats.languagecode)         AS num_languages,
      cats.category,
      COUNT(DISTINCT cats.contentid)            AS num_messages,
      ROUND(SUM(duration_seconds)/60.0, 0)      AS duration_minutes,
      round(MAX(tbinfo.played_seconds)/60.0, 0) AS played_minutes,
      MAX(tbinfo.effective_completions)         AS effective_completions,
      MAX(tbinfo.completions)                   AS completions,
      MAX(tbinfo.num_tbs)                       AS num_tbs
    -- The basics of a usage query
    FROM
      (
        SELECT DISTINCT
          project,
          deployment,
          deploymentnumber,
          startdate,
          category,
          package,
          languagecode,
          contentid,
          duration_seconds
        FROM usage_info
        GROUP BY
          project,
          deployment,
          deploymentnumber,
          startdate,
          category,
          package,
          languagecode,
          contentid,
          duration_seconds
      ) cats
      -- Data that is per-talkingbook (talkingbook, played duration, completions)
      JOIN
      (SELECT DISTINCT
         project,
         deploymentnumber,
         category,
         count(talkingbook)          AS all_tbs,
         count(DISTINCT talkingbook) AS num_tbs,
         sum(played_seconds)         AS played_seconds,
         sum(effective_completions)  AS effective_completions,
         sum(completions)            AS completions
       FROM usage_info
       GROUP BY project, deploymentnumber, category
      ) tbinfo
        ON tbinfo.project=cats.project AND tbinfo.deploymentnumber=cats.deploymentnumber
           AND tbinfo.category=cats.category

    GROUP BY cats.project, cats.deployment, cats.deploymentnumber, cats.startdate, cats.category
    ORDER BY project, startdate, category
) us_by_depl_cat;

 -- Usage summarized by deployment and category (aggregated across packages)
SELECT * INTO TEMPORARY TABLE usage_by_package_category FROM (
    SELECT DISTINCT
      cats.project,
      cats.deploymentnumber,
      cats.deployment,
      cats.startdate,
      cats.package,
      cats.languagecode,
      cats.language,
      cats.category,
      COUNT(DISTINCT cats.contentid)            AS num_messages,
      ROUND(SUM(duration_seconds)/60.0, 0)      AS duration_minutes,
      round(MAX(tbinfo.played_seconds)/60.0, 0) AS played_minutes,
      MAX(tbinfo.effective_completions)         AS effective_completions,
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
          package,
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
          package,
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
         package,
         category,
         count(talkingbook)          AS all_tbs,
         count(DISTINCT talkingbook) AS cat_tbs,
         sum(played_seconds)         AS played_seconds,
         sum(effective_completions)  AS effective_completions,
         sum(completions)            AS completions
       FROM usage_info
       GROUP BY project, deploymentnumber, package, category
      ) tbinfo
        ON tbinfo.project=cats.project AND tbinfo.deploymentnumber=cats.deploymentnumber
           AND tbinfo.package=cats.package AND tbinfo.category=cats.category
      JOIN
      package_tbs_used ptb
        ON ptb.project=cats.project AND ptb.package=cats.package

    GROUP BY cats.project, cats.deployment, cats.deploymentnumber, cats.startdate, cats.package, cats.languagecode, cats.language, cats.category
    ORDER BY project, startdate, category
) us_by_pkg_cat;

 -- Usage by package and community
SELECT * INTO TEMPORARY TABLE usage_by_package_community FROM  (
    SELECT DISTINCT
      cats.project,
      cats.deploymentnumber,
      cats.deployment,
      cats.startdate,
      cats.package,
      cats.languagecode,
      cats.language,
      cats.community,
      COUNT(DISTINCT cats.contentid)            AS num_messages,
      ROUND(SUM(duration_seconds)/60.0, 1)      AS duration_minutes,
      round(MAX(tbinfo.played_seconds)/60.0, 1) AS played_minutes,
      MAX(tbinfo.effective_completions)         AS effective_completions,
      MAX(tbinfo.completions)                   AS completions,
      MAX(tbinfo.community_tbs)                 AS community_tbs,
      MAX(ptb.package_tbs_used)                 AS pkg_tbs
    -- The basics of a usage query
    FROM
      (
        SELECT DISTINCT
          project,
          deploymentnumber,
          deployment,
          startdate,
          package,
          languagecode,
          language,
          community,
          contentid,
          duration_seconds
        FROM usage_info
        GROUP BY
          project,
          deploymentnumber,
          deployment,
          startdate,
          package,
          languagecode,
          language,
          community,
          contentid,
          duration_seconds
      ) cats
      -- Data that is per-talkingbook (talkingbook, played duration, completions)
      JOIN
      (SELECT DISTINCT
         project,
         deploymentnumber,
         package,
         community,
         count(talkingbook)          AS all_tbs,
         count(DISTINCT talkingbook) AS community_tbs,
         sum(played_seconds)         AS played_seconds,
         sum(effective_completions)  AS effective_completions,
         sum(completions)            AS completions
       FROM usage_info
       GROUP BY project, deploymentnumber, package, community
      ) tbinfo
        ON tbinfo.project=cats.project AND tbinfo.deploymentnumber=cats.deploymentnumber
           AND tbinfo.package=cats.package AND tbinfo.community=cats.community
      JOIN
      package_tbs_used ptb
        ON ptb.project=cats.project AND ptb.package=cats.package

    GROUP BY cats.project, cats.deployment, cats.deploymentnumber, cats.startdate, 
        cats.package, cats.languagecode, cats.language, cats.community
    ORDER BY project, startdate, community
) us_by_pkg_comm;

  -- Usage at a by project and deployment.
SELECT * INTO TEMPORARY TABLE usage_by_deployment FROM (
    SELECT DISTINCT
      project,
      deployment,
      deploymentnumber,
      startdate,
      COUNT(DISTINCT package)            AS num_packages,
      COUNT(DISTINCT languagecode)       AS num_languages,
      COUNT(DISTINCT community)          AS num_communities,
      COUNT(DISTINCT category)           AS num_categories,
      COUNT(DISTINCT contentid)          AS num_messages,
      COUNT(DISTINCT talkingbook)        AS num_tbs,
      ROUND(SUM(played_seconds)/60.0, 2) AS played_minutes,
      SUM(effective_completions)         AS num_effective_completions,
      SUM(completions)                   AS num_completions

    FROM usage_info ui
    GROUP BY project, deployment, deploymentnumber, startdate
    ORDER BY project, startdate
) us_by_depl;

 -- Usage at a by project and deployment.
SELECT * INTO TEMPORARY TABLE usage_by_package FROM (
    SELECT DISTINCT
      project,
      deployment,
      deploymentnumber,
      startdate,
      package,
      languagecode,
      language,
      COUNT(DISTINCT community)          AS num_communities,
      COUNT(DISTINCT category)           AS num_categories,
      COUNT(DISTINCT contentid)          AS num_messages,
      COUNT(DISTINCT talkingbook)        AS num_tbs,
      ROUND(SUM(played_seconds)/60.0, 2) AS played_minutes,
      SUM(effective_completions)         AS num_effective_completions,
      SUM(completions)                   AS num_completions

    FROM usage_info ui
    GROUP BY project, deployment, deploymentnumber, startdate, package, languagecode, language
    ORDER BY project, startdate, package, languagecode
) us_by_pkg;

 -- Report usage at the talking book level. 
SELECT * INTO TEMPORARY TABLE usage_by_talkingbook FROM (
    SELECT DISTINCT
      ci.project,
      ci.community,
      ci.package,
      ci.talkingbook,
      COUNT(DISTINCT(contentid))         AS num_messages,
      ROUND(SUM(duration_seconds)/60.0, 1)    AS duration_minutes,
      ROUND(SUM(played_seconds)/60.0, 1) AS played_minutes,
      SUM(effective_completions)         AS effective_completions,
      SUM(completions)                   AS completions,
      MAX(package_tbs_used)              AS num_package_tbs
    FROM
      usage_info ci
      JOIN package_tbs_used ptb
        ON ptb.project=ci.project AND ptb.package=ci.package
    GROUP BY
      ci.project,
      ci.community,
      ci.package,
      ci.talkingbook
    ORDER BY project, 
      community, 
      package,
      played_minutes DESC 
 ) us_by_tb;

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

 -- Like usage_by_community, but with deployed TB count added.
SELECT * INTO TEMPORARY TABLE usage_by_community_with_depl FROM ( 
    SELECT
        uc.project, uc.deploymentnumber, uc.deployment, uc.startdate, uc.package
        ,uc.languagecode, uc.language, uc.community
        ,uc.num_messages, uc.duration_minutes, uc.played_minutes, uc.effective_completions, uc.completions
        ,uc.community_tbs as reporting_tbs, uc.pkg_tbs
        ,dc.deployed_tbs
        
    FROM usage_by_package_community uc
    JOIN deployments_by_community dc
      ON dc.deploymentnumber=uc.deploymentnumber AND dc.package=uc.package AND dc.community ilike uc.community
    ORDER BY
      uc.project
      ,uc.deploymentnumber
      ,uc.community
) us_by_cm_depl;

