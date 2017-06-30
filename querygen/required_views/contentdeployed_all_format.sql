-- View: contentdeployed_all_format

-- DROP VIEW contentdeployed_all_format;

CREATE OR REPLACE VIEW contentdeployed_all_format AS 
 SELECT contentdeployed.project,
    contentdeployed.format,
    count(DISTINCT contentdeployed.contentid) AS msgs,
    sum(contentdeployed.min) AS min
   FROM contentdeployed
  GROUP BY contentdeployed.project, contentdeployed.format
  ORDER BY contentdeployed.project, contentdeployed.format;

ALTER TABLE contentdeployed_all_format
  OWNER TO lb_data_uploader;

