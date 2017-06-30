-- View: contentdeployed_all_category

-- DROP VIEW contentdeployed_all_category;

CREATE OR REPLACE VIEW contentdeployed_all_category AS 
 SELECT contentdeployed.project,
    contentdeployed.acm_category,
    count(DISTINCT contentdeployed.contentid) AS msgs,
    sum(contentdeployed.min) AS min
   FROM contentdeployed
  GROUP BY contentdeployed.project, contentdeployed.acm_category
  ORDER BY contentdeployed.project, contentdeployed.acm_category;

ALTER TABLE contentdeployed_all_category
  OWNER TO lb_data_uploader;

