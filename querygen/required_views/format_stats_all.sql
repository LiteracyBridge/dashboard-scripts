-- View: format_stats_all

-- DROP VIEW format_stats_all;

CREATE OR REPLACE VIEW format_stats_all AS 
 SELECT message_stats.project,
    message_stats.format,
    sum(message_stats.hrsplayed) AS hrs,
    sum(message_stats.completed) AS completed,
    max(message_stats.tbs) AS tbs
   FROM message_stats
  GROUP BY message_stats.project, message_stats.format
  ORDER BY message_stats.project, message_stats.format;

ALTER TABLE format_stats_all
  OWNER TO lb_data_uploader;


