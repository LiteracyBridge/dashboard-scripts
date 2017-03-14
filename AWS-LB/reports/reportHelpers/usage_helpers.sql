--
-- USAGE QUERY: reports on how messages are listened to, by message, and 
--

-- This selects the columns that we want from allsources.
, usage_info_base AS (
    SELECT
      project,
      contentpackage,
      contentid,
      village                  AS community,
      talkingbook,
      played_seconds_max       AS played_seconds,
      effectivecompletions_max AS effective_completions,
      completed_max            AS completions
    FROM allsources_s
    GROUP BY
      project,
      contentpackage,
      contentid,
      talkingbook,
      village,
      played_seconds,
      effective_completions,
      completions
)

  -- This adds the language, title, and duration_seconds from contentmetadata2, and the category from categories.
  , usage_info_base2 AS (
    SELECT
      cs.project,
      cs.contentpackage,
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
        ON cs.contentpackage=cp.contentpackage AND cs.contentid=cp.contentid
      JOIN categories cat
        ON cat.categoryid=cp.categoryid AND cat.projectcode=cp.project
)

  -- This adds the deploymentnumber from deployments.
  , usage_info AS (
    SELECT
      pi.project,
      pi.contentpackage,
      pi.languagecode,
      pi.community,
      pi.category,
      pi.contentid,
      pi.title,
      pi.duration_seconds,
      pi.talkingbook,
      pi.played_seconds,
      pi.effective_completions,
      pi.completions,
      d.deploymentnumber

    FROM
      usage_info_base2 pi
      JOIN packagesindeployment pd
        ON pd.project=pi.project AND pd.contentpackage=pi.contentpackage
      JOIN deployments d
        ON d.project=pi.project AND d.deployment=pd.deployment
)

  -- This is a helper to count the number of talking books that reported using content from some
  -- given content package.
  , package_tbs_used AS (
    SELECT DISTINCT
      project,
      contentpackage,
      count(DISTINCT talkingbook) AS package_tbs_used
    FROM allsources_s
    GROUP BY project, contentpackage
)

  -- Usage by project / deployment / package / language / category / message
  , usage_by_message AS (
    SELECT DISTINCT
      ci.project,
      deploymentnumber,
      ci.contentpackage,
      languagecode,
      category,
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
      count(DISTINCT talkingbook)        AS num_tbs,
      MAX(package_tbs_used)              AS num_package_tbs,
      ROUND(100.0*count(DISTINCT talkingbook)/greatest(MAX(package_tbs_used), 1), 0)
                                         AS percent_tbs_playing
    FROM
      usage_info ci
      JOIN package_tbs_used ptb
        ON ptb.project=ci.project AND ptb.contentpackage=ci.contentpackage
    GROUP BY
      ci.project,
      deploymentnumber,
      ci.contentpackage,
      languagecode,
      category,
      contentid,
      title,
      duration_seconds
    ORDER BY project, deploymentnumber, contentpackage, category, title
)

  -- Usage by project / deployment / package / language / category
  , usage_by_category AS (
    SELECT DISTINCT
      ci.project,
      ci.deploymentnumber,
      ci.contentpackage,
      ci.languagecode,
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
         deploymentnumber,
         contentpackage,
         languagecode,
         category,
         contentid,
         duration_seconds
       FROM usage_info
       GROUP BY
         project,
         deploymentnumber,
         contentpackage,
         languagecode,
         category,
         contentid,
         duration_seconds
      ) ci
      -- Data that is per-talkingbook (talkingbook, played duration, completions)
      JOIN (SELECT DISTINCT
              project,
              deploymentnumber,
              contentpackage,
              category,
              count(talkingbook)          AS all_tbs,
              count(DISTINCT talkingbook) AS num_tbs,
              sum(played_seconds)         AS played_seconds,
              sum(effective_completions)  AS effective_completions,
              sum(completions)            AS completions
            FROM usage_info
            GROUP BY project, deploymentnumber, contentpackage, category
           ) tbinfo
        ON tbinfo.project=ci.project AND tbinfo.deploymentnumber=ci.deploymentnumber
           AND tbinfo.contentpackage=ci.contentpackage AND
           tbinfo.category=ci.category
      JOIN
      package_tbs_used ptb
        ON ptb.project=ci.project AND ptb.contentpackage=ci.contentpackage
    GROUP BY ci.project, ci.deploymentnumber, ci.contentpackage, ci.languagecode, ci.category
    ORDER BY project, deploymentnumber, contentpackage, category
)

  -- Usage summarized by deployment and category (aggregated across packages)
  , usage_by_deployment_category AS (
    SELECT DISTINCT
      cats.project,
      cats.deploymentnumber,
      COUNT(DISTINCT cats.contentpackage)       AS cat_packages,
      COUNT(DISTINCT cats.languagecode)         AS cat_languages,
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
          deploymentnumber,
          category,
          contentpackage,
          languagecode,
          contentid,
          duration_seconds
        FROM usage_info
        GROUP BY
          project,
          deploymentnumber,
          category,
          contentpackage,
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

    GROUP BY cats.project, cats.deploymentnumber, cats.category
    ORDER BY project, deploymentnumber, category
)

  -- Usage at a by project and deployment.
  , usage_by_deployment AS (
    SELECT DISTINCT
      s.project,
      d.deploymentnumber,
      count(DISTINCT s.contentpackage) AS num_packages,
      sum(s.num_communities)           AS num_communities,
      sum(s.num_tbs)                   AS num_tbs

    FROM (SELECT DISTINCT
            s.project,
            s.contentpackage,
            count(DISTINCT s.village)     AS num_communities,
            count(DISTINCT s.talkingbook) AS num_tbs
          FROM allsources_s s
          GROUP BY s.project, s.contentpackage
         ) s
      JOIN packagesindeployment p
        ON p.project=s.project AND p.contentpackage=s.contentpackage
      JOIN deployments d
        ON d.project=s.project AND d.deployment=p.deployment
    GROUP BY s.project, d.deploymentnumber
    ORDER BY project, deploymentnumber
)

 -- Report usage at the talking book level. 
 , usage_by_talkingbook AS (
    SELECT DISTINCT
      ci.project,
      ci.community,
      ci.contentpackage,
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
        ON ptb.project=ci.project AND ptb.contentpackage=ci.contentpackage
    GROUP BY
      ci.project,
      ci.community,
      ci.contentpackage,
      ci.talkingbook
    ORDER BY project, 
      community, 
      contentpackage,
      played_minutes DESC 
 )

  -- Report the last 4 usage counts for every project
  , usage_recent_by_project AS (
    SELECT
      project,
      deploymentnumber AS "#",
      num_packages     AS "# Packages",
      num_communities  AS "# Communities Reporting",
      num_tbs          AS "# Talking Books Reporting"
    FROM (
           SELECT
             ROW_NUMBER()
             OVER (PARTITION BY project
               ORDER BY deploymentnumber DESC) AS row,
             ubd.*
           FROM
             usage_by_deployment ubd) extract
    WHERE
      extract.row<=4
)


