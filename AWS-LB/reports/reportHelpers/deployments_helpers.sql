--
-- DEPLOYMENT QUERY: reports on how messages are deployed
--

-- Selects the columns we care about
, update_operation_info AS (
    SELECT
      project,
      outdeployment AS deployment,
      outimage      AS packagename,
      outcommunity  AS community,
      outsn         AS talkingbook
    FROM tbdataoperations
    WHERE
      action ILIKE 'update%'
    GROUP BY project, deployment, packagename, community, talkingbook
    ORDER BY outdeployment, outcommunity, packagename
)

  -- Add deploymentnumber and languagecode.
  , deployment_info AS (
    SELECT DISTINCT
      di.project,
      di.deployment,
      d.deploymentnumber,
      di.packagename,
      pid.languagecode,
      community,
      COUNT(DISTINCT talkingbook) AS deployed_tbs
    FROM update_operation_info di
      JOIN deployments d ON d.project = di.project AND d.deployment = di.deployment
      JOIN packagesindeployment pid
        ON pid.project = di.project AND pid.deployment = di.deployment AND
           pid.packagename = di.packagename

    GROUP BY di.project, di.deployment, d.deploymentnumber, di.packagename, pid.languagecode,
      di.community
)

  -- Report of language, #communities, #tbs, per package
  , deployments_by_package AS (
    SELECT DISTINCT
      di.project,
      di.deployment,
      di.deploymentnumber,
      di.packagename,
      di.languagecode,
      l.language,
      COUNT(DISTINCT community) AS num_communities,
      SUM(di.deployed_tbs)      AS deployed_tbs
    FROM deployment_info di
      JOIN languages l ON l.projectcode = di.project AND l.languagecode = di.languagecode

    GROUP BY di.project, di.deployment, di.deploymentnumber, di.packagename, di.languagecode,
      l.language
    ORDER BY project, deploymentnumber, packagename, languagecode, language
)

  -- Report of #packages, #languages, #communities, #tbs, per content update
  , deployments_by_update AS (
    SELECT DISTINCT
      dp.project,
      dp.deploymentnumber,
      count(DISTINCT dp.packagename)  AS num_packages,
      count(DISTINCT dp.languagecode) AS num_languages,
      sum(dp.num_communities)         AS num_communities,
      sum(dp.deployed_tbs)            AS deployed_tbs
    FROM deployments_by_package dp
    GROUP BY dp.project, dp.deploymentnumber
    ORDER BY dp.deploymentnumber
)
  
  -- Report of #tbs, per community per package
  , deployments_by_community AS (
    SELECT DISTINCT
      di.project,
      di.deploymentnumber,
      di.packagename,
      di.community,
      SUM(di.deployed_tbs)      AS deployed_tbs
    FROM deployment_info di
    GROUP BY di.project, di.deploymentnumber, di.packagename, di.community
    ORDER BY project, community, deploymentnumber, packagename
)

  -- Report of # packages, per talkingbook
  , deployments_by_talkingbook AS (
    SELECT DISTINCT project, 
        community, 
        talkingbook, 
        count(DISTINCT packagename) AS num_packages
    FROM update_operation_info
    GROUP BY project, community, talkingbook
    ORDER BY project, community, talkingbook
)

  -- Report the last 4 deployments per project
  , deployments_recent_by_project AS (
    SELECT
      project,
      deploymentnumber AS "#",
      num_packages     AS "# Packages",
      num_languages    AS "# Languages",
      num_communities  AS "# Communities Updated",
      deployed_tbs     AS "# Talking Books Updated"
    FROM (
           SELECT
             ROW_NUMBER()
             OVER (PARTITION BY project
               ORDER BY deploymentnumber DESC) AS row,
             dbu.*
           FROM
             deployments_by_update dbu) extract
    WHERE
      extract.row <= 4
)

