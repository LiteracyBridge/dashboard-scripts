--
-- TODO: Think of some way to test this that doesn't take 5 minutes per test!
--

-- Helpers for usage queries.

SELECT * INTO TEMPORARY TABLE usage_info_base FROM (
    SELECT
      s.project,
      contentpackage           AS package,
      contentid,
      village                  AS community,
      recipientid,
      talkingbook,
      played_seconds_max       AS played_seconds,
      completed_max            AS completions
    FROM allsources_s s
    LEFT OUTER JOIN recipients_map rm ON s.project = rm.project AND s.village = rm.directory   
    GROUP BY
      s.project,
      package,
      contentid,
      talkingbook,
      community,
      recipientid,
      played_seconds,
      completions
) us_info_b;

-- This adds the language, title, and duration_seconds from contentmetadata2, and the category from categories.
SELECT * INTO TEMPORARY TABLE usage_info_base2 FROM (
    SELECT
      cs.project,
      cs.package,
      cm.languagecode,
      cs.community,
      cs.recipientid,
      CASE WHEN cat.categoryname:: TEXT~~'General%':: TEXT
        THEN "substring"(cat.categoryname:: TEXT, 9):: CHARACTER VARYING
      ELSE cat.categoryname
      END             AS category,
      cs.contentid,
      cm.title,
      cm.format,
      cm.duration_sec AS duration_seconds,
      cp.order as position,
      --Use like: STRING_AGG(DISTINCT CAST(position AS TEXT), ';') AS position_list,
      cs.talkingbook,
      cs.played_seconds,
      cs.completions
    FROM
      usage_info_base cs
      JOIN contentmetadata2 cm
        ON cs.contentid = cm.contentid AND cs.project=cm.project
      JOIN contentinpackage cp
        ON cs.package = cp.contentpackage AND cs.contentid = cp.contentid
      JOIN categories cat
        ON cat.categoryid = cp.categoryid AND cat.projectcode=cp.project
) us_info_b2;

-- This adds the deploymentnumber from deployments, language from language, and
-- partner, affiliate, country, region, and district from recipients.
SELECT * INTO TEMPORARY TABLE usage_info FROM (
  SELECT
      pi.project,
      pi.package,
      pi.languagecode,
      l.language,
      pi.community,
      pi.recipientid,
      r.partner,
      r.affiliate,
      r.country,
      r.region,
      r.district,
      pi.category,
      pi.contentid,
      pi.title,
      pi.format,
      pi.duration_seconds,
      pi.position,
      pi.talkingbook,
      pi.played_seconds,
      pi.completions,
      pid.deployment,
      d.deploymentnumber,
      pid.startdate

    FROM
      usage_info_base2 pi
      JOIN packagesindeployment pid
        ON pid.project=pi.project AND pid.contentpackage = pi.package
      JOIN deployments d
        ON d.project=pi.project AND d.deployment = pid.deployment
      JOIN languages l ON l.projectcode = pi.project AND l.languagecode = pi.languagecode
      LEFT OUTER JOIN recipients r
        ON pi.recipientid = r.recipientid

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
      --ci.deployment,
      STRING_AGG(DISTINCT ci.deployment, ';') AS deployment,
      ci.deploymentnumber,
      ci.startdate,
      ci.package,
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
      count(DISTINCT talkingbook)        AS num_tbs,
      MAX(ptb.package_tbs_used)          AS num_package_tbs,
      ROUND(100.0*count(DISTINCT talkingbook)/greatest(MAX(package_tbs_used), 1), 0)
                                         AS percent_tbs_playing
    FROM
      usage_info ci
      JOIN package_tbs_used ptb
        ON ptb.project=ci.project AND ptb.package = ci.package
    GROUP BY
      ci.project,
      --ci.deployment,
      ci.deploymentnumber,
      ci.startdate,
      ci.package,
      languagecode,
      language,
      contentid,
      title,
      format,
      duration_seconds
    ORDER BY project, startdate, package, title
) us_by_msg;


-- Usage summarized by deployment and category (aggregated across packages)
SELECT * INTO TEMPORARY TABLE usage_by_package_category FROM (
    SELECT DISTINCT
      cats.project,
      cats.deploymentnumber,
      --cats.deployment,
      STRING_AGG(DISTINCT cats.deployment, ';') AS deployment,
      cats.startdate,
      cats.package,
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
         sum(completions)            AS completions
       FROM usage_info
       GROUP BY project, deploymentnumber, package, category
      ) tbinfo
        ON tbinfo.project=cats.project AND tbinfo.deploymentnumber=cats.deploymentnumber
           AND tbinfo.package ilike cats.package AND tbinfo.category ilike cats.category
      JOIN
      package_tbs_used ptb
        ON ptb.project=cats.project AND ptb.package ilike cats.package

    GROUP BY cats.project,
      --cats.deployment,
      cats.deploymentnumber,
      cats.startdate,
      cats.package,
      cats.languagecode,
      cats.language,
      cats.category
    ORDER BY project, startdate, category
) us_by_pkg_cat;



-- Usage at a by project and deployment.
SELECT * INTO TEMPORARY TABLE usage_by_deployment FROM (
    SELECT DISTINCT
      project,
      --deployment,
      STRING_AGG(DISTINCT deployment, ';') AS deployment,
      deploymentnumber,
      startdate,
      COUNT(DISTINCT package)            AS num_packages,
      COUNT(DISTINCT languagecode)       AS num_languages,
      COUNT(DISTINCT community)          AS num_communities,
      COUNT(DISTINCT recipientid)        AS num_recipients,
      COUNT(DISTINCT category)           AS num_categories,
      COUNT(DISTINCT contentid)          AS num_messages,
      COUNT(DISTINCT talkingbook)        AS num_tbs,
      ROUND(SUM(played_seconds)/60.0, 2) AS played_minutes,
      SUM(completions)                   AS num_completions

    FROM usage_info ui
    GROUP BY project,
        --deployment,
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

