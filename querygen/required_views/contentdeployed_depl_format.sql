-- View: contentdeployed_depl_format

-- DROP VIEW contentdeployed_depl_format;

CREATE OR REPLACE VIEW contentdeployed_depl_format AS 
 SELECT contentdeployed.project,
    contentdeployed.deploymentnumber,
    contentdeployed.language,
    contentdeployed.format,
    count(DISTINCT contentdeployed.contentid) AS msgs,
    sum(contentdeployed.min) AS min
   FROM contentdeployed
  GROUP BY contentdeployed.project, contentdeployed.deploymentnumber, contentdeployed.language, contentdeployed.format
  ORDER BY contentdeployed.project, contentdeployed.deploymentnumber, contentdeployed.language, contentdeployed.format;

ALTER TABLE contentdeployed_depl_format
  OWNER TO lb_data_uploader;

