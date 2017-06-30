-- View: contentdeployed_depl_category

-- DROP VIEW contentdeployed_depl_category;

CREATE OR REPLACE VIEW contentdeployed_depl_category AS 
 SELECT contentdeployed.project,
    contentdeployed.deploymentnumber,
    contentdeployed.language,
    contentdeployed.acm_category,
    count(DISTINCT contentdeployed.contentid) AS msgs,
    sum(contentdeployed.min) AS min
   FROM contentdeployed
  GROUP BY contentdeployed.project, contentdeployed.deploymentnumber, contentdeployed.language, contentdeployed.acm_category
  ORDER BY contentdeployed.project, contentdeployed.deploymentnumber, contentdeployed.language, contentdeployed.acm_category;

ALTER TABLE contentdeployed_depl_category
  OWNER TO lb_data_uploader;

