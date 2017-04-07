--
-- DEPLOYMENT QUERY: reports on how messages are deployed
--

-- Selects the columns we care about
SELECT * INTO TEMPORARY TABLE update_operation_info FROM (
    SELECT
      project,
      outdeployment AS deployment,
      outimage      AS package,
      outcommunity  AS community,
      outsn         AS talkingbook
    FROM tbdataoperations
    WHERE
      action ILIKE 'update%'
    GROUP BY project, deployment, package, community, talkingbook
    ORDER BY outdeployment, outcommunity, package
) update_op_i;

  -- Add deploymentnumber and languagecode.
SELECT * INTO TEMPORARY TABLE deployment_info FROM (
    SELECT DISTINCT
      di.project,
      di.deployment,
      d.deploymentnumber,
      di.package,
      pid.languagecode,
      pid.startdate,
      community,
      COUNT(DISTINCT talkingbook) AS deployed_tbs
    FROM update_operation_info di
      JOIN deployments d ON d.project = di.project AND d.deployment = di.deployment
      JOIN packagesindeployment pid
        ON pid.project = di.project AND pid.deployment = di.deployment AND
           pid.contentpackage = di.package

    GROUP BY di.project, di.deployment, d.deploymentnumber, di.package, pid.languagecode,
      pid.startdate, di.community
) depl_i;

  -- Report of language, #communities, #tbs, per package
SELECT * INTO TEMPORARY TABLE deployments_by_package FROM (
    SELECT DISTINCT
      di.project,
      di.deployment,
      di.deploymentnumber,
      di.startdate,
      di.package,
      di.languagecode,
      l.language,
      COUNT(DISTINCT community) AS num_communities,
      SUM(di.deployed_tbs)      AS deployed_tbs
    FROM deployment_info di
      JOIN languages l ON l.projectcode = di.project AND l.languagecode = di.languagecode

    GROUP BY di.project, di.deployment, di.deploymentnumber, di.startdate, di.package, di.languagecode,
      l.language
    ORDER BY project, di.startdate, deploymentnumber, package, languagecode, language
) depl_by_pkg;

  -- Report of #packages, #languages, #communities, #tbs, per content update
SELECT * INTO TEMPORARY TABLE deployments_by_deployment FROM (
    SELECT DISTINCT
      dp.project,
      dp.deployment,
      dp.deploymentnumber,
      dp.startdate,
      count(DISTINCT dp.package)  AS num_packages,
      count(DISTINCT dp.languagecode) AS num_languages,
      sum(dp.num_communities)         AS num_communities,
      sum(dp.deployed_tbs)            AS deployed_tbs
    FROM deployments_by_package dp
    GROUP BY dp.project, dp.deployment, dp.deploymentnumber, dp.startdate
    ORDER BY dp.project, dp.startdate, dp.deploymentnumber
) depl_by_depl;
  
  -- Report of #tbs, per community per package
SELECT * INTO TEMPORARY TABLE deployments_by_community FROM (
    SELECT DISTINCT
      di.project,
      di.deployment,
      di.deploymentnumber,
      di.startdate,
      di.package,
      di.community,
      SUM(di.deployed_tbs)      AS deployed_tbs
    FROM deployment_info di
    GROUP BY di.project, di.deployment, di.deploymentnumber, di.startdate, di.package, di.community
    ORDER BY project, community, di.startdate, deploymentnumber, package
) depl_by_comm;

  -- Report of # packages, per talkingbook
SELECT * INTO TEMPORARY TABLE deployments_by_talkingbook FROM (
    SELECT DISTINCT project, 
        community, 
        talkingbook, 
        count(DISTINCT package) AS num_packages
    FROM update_operation_info
    GROUP BY project, community, talkingbook
    ORDER BY project, community, talkingbook
) depl_by_tb;

  -- Report the last 4 deployments per project
CREATE OR REPLACE TEMP VIEW deployment_dashboard AS (
    SELECT
      project,
      deployment,
      deploymentnumber,
      startdate,
      num_packages,
      num_languages,
      num_communities,
      deployed_tbs
    FROM (
           SELECT
             ROW_NUMBER()
             OVER (PARTITION BY project
               ORDER BY startdate DESC) AS row,
             dbu.*
           FROM
             deployments_by_deployment dbu) extract
    WHERE
      extract.row <= 4
);


