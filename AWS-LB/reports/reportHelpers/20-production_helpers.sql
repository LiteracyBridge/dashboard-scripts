--
-- PRODUCTION QUERY
--

-- Content deployed, with all the columns we care about
SELECT * INTO TEMPORARY TABLE production_info FROM (
    SELECT
      d.project,
      d.deployment,
      d.deploymentnumber,
      pid.startdate,
      pid.contentpackage AS package,
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
      JOIN packagesindeployment pid ON pid.project = d.project AND pid.deployment ilike d.deployment
      JOIN languages l ON l.projectcode = d.project AND l.languagecode ilike pid.languagecode
      -- Add the content and its category (id and name)
      JOIN
      contentinpackage cip
        ON cip.project = d.project AND cip.contentpackage ilike pid.contentpackage
      JOIN categories c ON c.projectcode = d.project AND c.categoryid ilike cip.categoryid
      -- Add the content's title, duration, and format
      JOIN contentmetadata2 cm ON cm.project = d.project AND cm.contentid ilike cip.contentid
) prod_info;

  -- In the queries below, there is not a strict hierarchy of deployment/package/
  -- category/message; messages appear in multiple deployments, multiple packages,
  -- and sometimes in multiple categories in a package.  So, #messages in a package 
  -- IS NOT the same as the sum of #messages in categories in a package. And
  -- therefore, we do the aggregation independently for things that can appear, 
  -- or not, independently within some container grouping (like deployment, or package).

  -- Production #packages, #messages, duration, per deployment
SELECT * INTO TEMPORARY TABLE production_by_deployment FROM (
    SELECT DISTINCT
      msg.project,
      --msg.deployment,
      STRING_AGG(DISTINCT msg.deployment, ';') AS deployment,
      msg.deploymentnumber,
      msg.startdate,
      MAX(pkg.num_packages)                  AS num_packages,
      MAX(pkg.num_languages)                 AS num_languages,    
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
            startdate,
            contentid,
            duration_sec
          FROM production_info
          GROUP BY project, deployment, deploymentnumber, startdate, contentid, duration_sec
         ) msg
      -- Distinct packages
      JOIN (SELECT DISTINCT
              project,
              deployment,
              deploymentnumber,
              COUNT(DISTINCT languagecode) AS num_languages,            
              COUNT(DISTINCT package) AS num_packages
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
    GROUP BY msg.project, 
        --msg.deployment, 
        msg.deploymentnumber, 
        msg.startdate
    ORDER BY project, startdate 
) prod_by_depl;

  -- Production #categories, #messages, duration, per deployment per package
SELECT * INTO TEMPORARY TABLE production_by_package FROM (
    SELECT DISTINCT
      msg.project,
      msg.deployment,
      msg.deploymentnumber,
      msg.startdate,
      msg.package,
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
         startdate,
         package,
         languagecode,
         language,
         contentid,
         duration_sec
       FROM production_info
       GROUP BY
         project,
         deployment,
         deploymentnumber,
         startdate,
         package,
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
         package,
         COUNT(DISTINCT categoryid) AS num_categories
       FROM production_info
       GROUP BY project, deployment, deploymentnumber, package
      ) cat
        ON cat.project = msg.project AND cat.deployment ilike msg.deployment AND
           cat.package ilike msg.package
    GROUP BY
      msg.project,
      msg.deployment,
      msg.deploymentnumber,
      msg.startdate,
      msg.package,
      msg.languagecode,
      msg.language
    ORDER BY project, startdate, package
) prod_by_pkg;

  -- Production #messages, duration per deployment per package(language) per category
SELECT * INTO TEMPORARY TABLE production_by_category FROM (
    SELECT DISTINCT
      project,
      deployment,
      deploymentnumber,
      startdate,
      package,
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
      startdate,
      package,
      languagecode,
      language,
      categoryid,
      categoryname
    ORDER BY project, deploymentnumber, startdate, package, categoryid
) prod_by_pkg;

