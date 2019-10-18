
-- Adds deployment number and recipient's geo-political information
CREATE OR REPLACE TEMP VIEW tb_deployments AS (
    SELECT DISTINCT
        td.project, 
        d.deploymentnumber,
        td.deployment,
        --STRING_AGG(DISTINCT td.deployment, ';') AS deployments,
        r.partner,
        r.affiliate,
        r.country,
        r.region,
        r.district,
        r.communityname,
        r.groupname,
        td.recipientid,
        talkingbookid
    FROM tbsdeployed td
    JOIN deployments d
      ON d.project = td.project AND d.deployment = td.deployment
    JOIN recipients r
      ON r.recipientid = td.recipientid
    GROUP BY
      td.project,
      d.deploymentnumber,
      td.deployment,
      r.partner,
      r.affiliate,
      r.country,
      r.region,
      r.district,
      r.communityname,
      r.groupname,
      td.recipientid,
      talkingbookid

);

CREATE OR REPLACE TEMP VIEW tb_deployments_by_deployment AS (
    SELECT DISTINCT
        project,
        deploymentnumber,
        STRING_AGG(DISTINCT deployment, ';') AS deployments,
        COUNT(DISTINCT recipientid) AS num_recipients,
        COUNT(DISTINCT talkingbookid) AS num_tbs
    FROM tb_deployments
    GROUP BY
        project,
        deploymentnumber
    ORDER BY
        deploymentnumber
);        

BEGIN TRANSACTION;
DROP VIEW tbnewsn CASCADE;

CREATE OR REPLACE VIEW tbnewsn AS (
    SELECT DISTINCT 
            project
            ,deployedtimestamp AS timestamp
            ,extract(year FROM deployedtimestamp) AS year
            ,extract(quarter FROM deployedtimestamp) AS quarter
            ,extract(month FROM deployedtimestamp) AS month
            ,to_char(deployedtimestamp, 'Mon') AS month_name
            ,talkingbookid 
            ,username ,tbcdid
        FROM tbsdeployed
        WHERE newsn 
        GROUP BY 
            project
            ,deployedtimestamp
            ,talkingbookid
            ,username ,tbcdid
        ORDER BY
            project ,year ,month);        

CREATE OR REPLACE VIEW tbnewsn_by_months AS (
    SELECT DISTINCT
        project
        ,year
        ,quarter
        ,month
        ,month_name
        ,min(timestamp) AS earliest
        ,COUNT(DISTINCT timestamp) AS count
    FROM tbnewsn
    GROUP BY
        project, year, quarter, month, month_name
    ORDER BY
        project, year, month);

CREATE OR REPLACE VIEW tbnewsn_past_six_months AS (
    SELECT * 
    FROM tbnewsn_by_months
    WHERE earliest >= Now()::Date-Interval'6 months'
    ORDER BY project ,year ,month);

COMMIT TRANSACTION;
