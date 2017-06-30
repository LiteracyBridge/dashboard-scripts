-- View: format_stats_depl

-- DROP VIEW format_stats_depl;

CREATE OR REPLACE VIEW format_stats_depl AS 
 SELECT message_stats.project,
    message_stats.deploymentnumber,
    message_stats.language,
    message_stats.format,
    sum(message_stats.hrsplayed) AS hrs,
    sum(message_stats.completed) AS completed,
    max(message_stats.tbs) AS tbs
   FROM message_stats
  GROUP BY message_stats.project, message_stats.deploymentnumber, message_stats.language, message_stats.format
  ORDER BY message_stats.project, message_stats.deploymentnumber, message_stats.language, message_stats.format;

ALTER TABLE format_stats_depl
  OWNER TO lb_data_uploader;

