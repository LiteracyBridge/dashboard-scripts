--
-- DEPLOYMENT QUERY: reports on how messages are deployed
--

-- Selects the columns we care about
--SELECT * INTO TEMPORARY TABLE update_operation_info FROM (
--    SELECT
--      project,
--      outdeployment AS deployment,
--      outimage      AS package,
--      outcommunity  AS community,
--      outsn         AS talkingbook
--    FROM tbdataoperations
--    WHERE
--      action ILIKE 'update%'
--    GROUP BY project, deployment, package, community, talkingbook
--    ORDER BY outdeployment, outcommunity, package
--) update_op_i;


-- Add basic recipient info to tbsdeployed
SELECT * INTO TEMPORARY TABLE tbs_deployed_info FROM (
    SELECT DISTINCT 
        td.project, 
        deployment, 
        contentpackage as package,
        td.recipientid, 
        r.communityname, 
        r.groupname,
        r.language as languagecode,
        talkingbookid
    FROM tbsdeployed td
    JOIN recipients r
      ON td.recipientid = r.recipientid
    GROUP BY td.project, 
    deployment, 
    package, 
    td.recipientid, 
    communityname, 
    groupname, 
    language, 
    talkingbookid
) tbs_deployed;

  -- Add deploymentnumber and languagecode.
-- Add deploymentnumber, start and end dates
SELECT * INTO TEMPORARY TABLE deployment_info FROM (
    SELECT DISTINCT
      di.project,
      di.deployment,
      d.deploymentnumber,
      di.package,
      languagecode,
      d.startdate,
      d.enddate,
      recipientid,
      communityname,
      groupname,
      talkingbookid
    FROM tbs_deployed_info di
      left outer JOIN deployments d ON d.project = di.project AND d.deployment ilike di.deployment

    GROUP BY di.project,
        di.deployment,
        d.deploymentnumber,
        di.package,
        languagecode,
        d.startdate,
        d.enddate,
        di.recipientid,
        di.communityname,
        di.groupname,
        di.talkingbookid
) depl_i;

  -- Report of language, #communities, #tbs, per package
--SELECT * INTO TEMPORARY TABLE deployments_by_package FROM (
--    SELECT DISTINCT
--      di.project,
--      di.deployment,
--      di.deploymentnumber,
--      di.startdate,
--      di.enddate,
--      di.package,
--      di.languagecode,
--      l.language,
--      COUNT(DISTINCT community)   AS num_communities,
--      COUNT(DISTINCT talkingbook) AS deployed_tbs
--    FROM deployment_info di
--      JOIN languages l ON l.projectcode = di.project AND l.languagecode ilike di.languagecode
--
--    GROUP BY di.project, di.deployment, di.deploymentnumber, di.startdate, di.enddate, di.package, di.languagecode,
--      l.language
--    ORDER BY project, di.startdate, deploymentnumber, package, languagecode, language
--) depl_by_pkg;

  -- Report of #packages, #languages, #communities, #tbs, per content update
SELECT * INTO TEMPORARY TABLE deployments_by_deployment FROM (
    SELECT DISTINCT
      project,
      --deployment,
      STRING_AGG(DISTINCT deployment, ';') AS deployment,
      deploymentnumber,
      startdate,
      enddate,
      count(DISTINCT package)       AS num_packages,
      count(DISTINCT languagecode)  AS num_languages,
      COUNT(DISTINCT communityname) AS num_communities,
      COUNT(DISTINCT recipientid)   AS num_recipients,
      COUNT(DISTINCT talkingbookid) AS deployed_tbs
    FROM deployment_info di
    GROUP BY project,
        deploymentnumber,
        startdate,
        enddate
    ORDER BY project, startdate, deploymentnumber
) depl_by_depl;

  -- Report of #tbs, per community per package
SELECT * INTO TEMPORARY TABLE deployments_by_community FROM (
    SELECT DISTINCT
      di.project,
      di.deployment,
      di.deploymentnumber,
      di.startdate,
      di.enddate,
      di.package,
      di.recipientid,
      di.communityname,
      di.communityname as community,
      COUNT(DISTINCT talkingbookid)      AS deployed_tbs
    FROM deployment_info di
    GROUP BY di.project, 
      di.deployment, 
      di.deploymentnumber, 
      di.startdate, 
      di.enddate, 
      di.package,
      di.recipientid,
      di.communityname
    ORDER BY project, 
      communityname, 
      di.startdate, 
      deploymentnumber, 
      package
) depl_by_comm;

  -- Report of # packages, per talkingbook
--SELECT * INTO TEMPORARY TABLE deployments_by_updateoperation FROM (
--    SELECT DISTINCT project,
--        community,
--        talkingbook,
--        count(DISTINCT package) AS num_packages
--    FROM update_operation_info
--    GROUP BY project, community, talkingbook
--    ORDER BY project, community, talkingbook
--) depl_by_tb;

-- Report from tbsdeployed, with details added from other tables.
--SELECT * INTO TEMPORARY TABLE deployments_by_talkingbook FROM (
--    SELECT
--        r.partner,
--        r.affiliate,
--        r.communityname,
--        r.groupname,
--        r.country,
--        r.region,
--        r.district,
--        tbd.talkingbookid,
--        tbd.recipientid,
--        tbd.deployedtimestamp,
--        tbd.project,
--        tbd.deployment,
--        depl.deploymentnumber,
--        tbd.contentpackage,
--        tbd.firmware,
--        tbd.location,
--        tbd.coordinates,
--        tbd.username,
--        tbd.tbcdid,
--        tbd.action,
--        tbd.newsn,
--        tbd.testing
--
--    FROM tbsdeployed tbd
--    JOIN recipients r
--      ON tbd.recipientid = r.recipientid
--    JOIN deployments depl
--      ON tbd.project = depl.project AND tbd.deployment = depl.deployment
--
--) depl_by_tbs_deployed;

-- Report of tbsdeployed, by recipient
--SELECT * INTO TEMPORARY TABLE deployments_by_recipient FROM (
--    SELECT DISTINCT
--      project,
--      partner,
--      affiliate,
--      deploymentnumber,
--      STRING_AGG(DISTINCT deployment, ';') AS deployment,
--      country,
--      region,
--      district,
--      communityname,
--      groupname,
--      recipientid,
--      COUNT(DISTINCT talkingbookid) as num_tbs
--    FROM deployments_by_talkingbook
--    GROUP BY
--      project,
--      deploymentnumber,
--      partner,
--      affiliate,
--      communityname,
--      groupname,
--      country,
--      region,
--      district,
--      recipientid
--    ORDER BY
--      project,
--      partner,
--      affiliate,
--      deploymentnumber,
--      country,
--      region,
--      district,
--      communityname,
--      groupname
--) depl_by_recip;

CREATE OR REPLACE TEMP VIEW deployment_date_mismatch AS (
    SELECT * FROM (
      SELECT distinct project,
        deploymentnumber,
        STRING_AGG(DISTINCT deployment, ';') as deployments,
        COUNT(DISTINCT startdate) as numstarts,
        COUNT(DISTINCT enddate) as numends
      FROM deployments
      GROUP BY project,
        deploymentnumber
      ORDER BY project, deploymentnumber
    ) d
    WHERE numstarts!=1 OR numends!=1
);

CREATE OR REPLACE TEMP VIEW deployment_spec AS (
    SELECT * FROM (
      SELECT distinct project,
        deploymentnumber as deployment_num,
        startdate,
        enddate,
        component,
        concat(project, '-', cast(extract(year from startdate)as integer)%100, '-', deploymentnumber) as name
      FROM deployments
      ORDER BY project, deployment_num
    ) d
);

  -- Report the last 4 deployments per project
CREATE OR REPLACE TEMP VIEW deployment_dashboard AS (
    SELECT
      project,
      deployment,
      deploymentnumber,
      startdate,
      enddate,
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
             (select * from deployments_by_deployment where deploymentnumber>0) dbu) extract
    WHERE
      extract.row <= 4
);


