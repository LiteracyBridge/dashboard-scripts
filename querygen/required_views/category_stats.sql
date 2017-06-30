-- View: category_stats

-- DROP VIEW category_stats;

CREATE OR REPLACE VIEW category_stats AS 
 SELECT message_stats.project,
    message_stats.deploymentnumber,
    message_stats.language,
    message_stats.acm_category,
    sum(message_stats.hrsplayed) AS hrs,
    sum(message_stats.completed) AS completed,
    max(message_stats.tbs) AS tbs
   FROM message_stats
  GROUP BY message_stats.project, message_stats.deploymentnumber, message_stats.language, message_stats.acm_category
  ORDER BY message_stats.project, message_stats.deploymentnumber, message_stats.language, message_stats.acm_category;

ALTER TABLE category_stats
  OWNER TO lb_data_uploader;

