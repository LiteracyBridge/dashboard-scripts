--
-- PRODUCTION QUERY
--

-- Content deployed, with all the columns we care about
, production_info AS (
    SELECT
      d.project,
      d.deployment,
      d.deploymentnumber,
      pid.contentpackage,
      pid.languagecode,
      l.language,
      cip.categoryid,
      c.categoryname,
      cip.contentid,
      cm.format,
      cm.duration_sec,
      cm.title
    -- Get projects and deployments
    FROM
      deployments d
      -- Add contentpackage and the package's language (code and name)
      JOIN packagesindeployment pid ON pid.project = d.project AND pid.deployment = d.deployment
      JOIN languages l ON l.projectcode = d.project AND l.languagecode = pid.languagecode
      -- Add the content and its category (id and name)
      JOIN
      contentinpackage cip
        ON cip.project = d.project AND cip.contentpackage = pid.contentpackage
      JOIN categories c ON c.projectcode = d.project AND c.categoryid = cip.categoryid
      -- Add the content's title, duration, and format
      JOIN contentmetadata2 cm ON cm.project = d.project AND cm.contentid = cip.contentid
)

  -- In the queries below, there is not a strict hierarchy of deployment/package/category/message; messages
  -- appear in multiple deployments, multiple packages, and sometimes in multiple categories in a package.
  -- So, #messages in a package IS NOT the same as the sum of #messages in categories in a package. And
  -- therefore, we do the aggregation independently for things that can appear, or not, independently
  -- within some container grouping (like deployment, or package).

  -- Production #packages, #messages, duration, per deployment
  , production_by_deployment AS (
    SELECT DISTINCT
      msg.project,
      msg.deployment,
      msg.deploymentnumber,
      MAX(pkg.num_packages)                  AS num_packages,
      MAX(cat.num_categories)                AS num_categories,
      COUNT(DISTINCT msg.contentid)          AS num_messages
      ---,COUNT(msg.contentid) as all_messages --debugging, should be =num_messages
      ,
      ROUND(SUM(msg.duration_sec) / 60.0, 1) AS duration_minutes
    -- Distinct content and duration
    FROM (SELECT DISTINCT
            project,
            deployment,
            deploymentnumber,
            contentid,
            duration_sec
          FROM production_info
          GROUP BY project, deployment, deploymentnumber, contentid, duration_sec
         ) msg
      -- Distinct packages
      JOIN (SELECT DISTINCT
              project,
              deployment,
              deploymentnumber,
              COUNT(DISTINCT contentpackage) AS num_packages
            FROM production_info
            GROUP BY project, deployment, deploymentnumber
           ) pkg ON pkg.project = msg.project AND pkg.deploymentnumber = msg.deploymentnumber
      -- Distinct categories
      JOIN (SELECT DISTINCT
              project,
              deployment,
              deploymentnumber,
              COUNT(DISTINCT categoryname) AS num_categories
            FROM production_info
            GROUP BY project, deployment, deploymentnumber
           ) cat ON cat.project = msg.project AND cat.deploymentnumber = msg.deploymentnumber
    GROUP BY msg.project, msg.deployment, msg.deploymentnumber
    ORDER BY project, deploymentnumber
)

  -- Production #categories, #messages, duration, per deployment per package
  , production_by_package AS (
    SELECT DISTINCT
      msg.project,
      msg.deployment,
      msg.deploymentnumber,
      msg.contentpackage,
      msg.languagecode,
      msg.language,
      MAX(cat.num_categories)                AS num_categories,
      COUNT(DISTINCT msg.contentid)          AS num_messages,
      COUNT(msg.contentid)                   AS all_messages --debugging, should be =num_messages
      ,
      ROUND(SUM(msg.duration_sec) / 60.0, 1) AS duration_minutes
    -- Distinct content and duration
    FROM
      (SELECT DISTINCT
         project,
         deployment,
         deploymentnumber,
         contentpackage,
         languagecode,
         language,
         contentid,
         duration_sec
       FROM production_info
       GROUP BY
         project,
         deployment,
         deploymentnumber,
         contentpackage,
         languagecode,
         language,
         contentid,
         duration_sec
      ) msg
      -- Distinct categories
      JOIN
      (SELECT DISTINCT
         project,
         deployment,
         deploymentnumber,
         contentpackage,
         COUNT(DISTINCT categoryid) AS num_categories
       FROM production_info
       GROUP BY project, deployment, deploymentnumber, contentpackage
      ) cat
        ON cat.project = msg.project AND cat.deployment = msg.deployment AND
           cat.contentpackage = msg.contentpackage
    GROUP BY
      msg.project,
      msg.deployment,
      msg.deploymentnumber,
      msg.contentpackage,
      msg.languagecode,
      msg.language
    ORDER BY project, deploymentnumber, contentpackage
)

  -- Production #messages, duration per deployment per package(language) per category
  , production_by_category AS (
    SELECT DISTINCT
      project,
      deployment,
      deploymentnumber,
      contentpackage,
      languagecode,
      language,
      categoryid,
      categoryname,
      COUNT(DISTINCT contentid)          AS num_messages
      --,COUNT(contentid) as all_messages --debugging, should be =num_messages
      ,
      ROUND(SUM(duration_sec) / 60.0, 1) AS duration_minutes
    FROM production_info
    GROUP BY
      project,
      deployment,
      deploymentnumber,
      contentpackage,
      languagecode,
      language,
      categoryid,
      categoryname
    ORDER BY project, deploymentnumber, contentpackage, categoryid
)

