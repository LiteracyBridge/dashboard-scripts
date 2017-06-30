-- View: depl_language_stats_from_categories

-- DROP VIEW depl_language_stats_from_categories;

CREATE OR REPLACE VIEW depl_language_stats_from_categories AS 
 SELECT message_stats.project,
    message_stats.deploymentnumber,
    message_stats.language,
    sum(message_stats.hrsplayed) AS hrs,
    sum(message_stats.completed) AS completed,
    max(message_stats.tbs) AS tbs
   FROM message_stats
  GROUP BY message_stats.project, message_stats.deploymentnumber, message_stats.language
  ORDER BY message_stats.project, message_stats.deploymentnumber, message_stats.language;

ALTER TABLE depl_language_stats_from_categories
  OWNER TO lb_data_uploader;

