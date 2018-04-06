
-- Adds deployment number and recipient's geo-political information
CREATE OR REPLACE TEMP VIEW tb_deployments AS (
    SELECT DISTINCT
        td.project, 
        d.deploymentnumber,
        td.deployment,
        --STRING_AGG(DISTINCT td.deployment, ';') as deployments,
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
        STRING_AGG(DISTINCT deployment, ';') as deployments,
        COUNT(DISTINCT recipientid) as num_recipients,
        COUNT(DISTINCT talkingbookid) as num_tbs
    FROM tb_deployments
    GROUP BY
        project,
        deploymentnumber
    ORDER BY
        deploymentnumber
);        

