-- View: depl_language_stats_from_care_communities

-- DROP VIEW depl_language_stats_from_care_communities;

CREATE OR REPLACE VIEW depl_language_stats_from_care_communities AS 
 SELECT community_stats_care.project,
    community_stats_care.deploymentnumber,
    community_stats_care.language,
    sum(community_stats_care.hoursplayed) AS hrs,
    sum(community_stats_care.completed) AS completed,
    sum(community_stats_care.tbcount) AS tbs
   FROM community_stats_care
  GROUP BY community_stats_care.project, community_stats_care.deploymentnumber, community_stats_care.language
  ORDER BY community_stats_care.project, community_stats_care.deploymentnumber, community_stats_care.language;

ALTER TABLE depl_language_stats_from_care_communities
  OWNER TO lb_data_uploader;

