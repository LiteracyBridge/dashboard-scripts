-- View: community_stats_care

-- DROP VIEW community_stats_care;

CREATE OR REPLACE VIEW community_stats_care AS 
 SELECT DISTINCT contentstats.project,
    d.deploymentnumber,
    l.language,
    contentstats.contentpackage,
    contentstats.community,
    round(contentstats.timeplayed / 3600::numeric, 1) AS hoursplayed,
    contentstats.msgs,
    round(contentstats.timeplayed / 3600::numeric / contentstats.tbs::numeric, 0) AS hourspertb,
    contentstats.completed,
    round(contentstats.completed / contentstats.tbs::numeric / contentstats.msgs::numeric, 1) AS completionspertbpermsg,
    round(contentstats.effectivecompletions / contentstats.tbs::numeric, 1) AS effcompletionspertb,
    contentstats.tbs AS tbcount
   FROM ( SELECT a.project,
            a.contentpackage,
            count(DISTINCT a.contentid) AS msgs,
            ( SELECT round((sum(cm.duration_sec) / 60)::numeric, 0) AS round
                   FROM contentmetadata2 cm
                     JOIN contentinpackage cp ON cm.contentid::text = cp.contentid::text
                  WHERE cp.contentpackage::text = a.contentpackage::text) AS duration_min,
            sum(a.played_seconds_max) AS timeplayed,
            sum(a.effectivecompletions_max) AS effectivecompletions,
            sum(a.completed_max) AS completed,
            count(DISTINCT a.talkingbook) AS tbs,
            a.village AS community
           FROM allsources_s a
          WHERE (a.contentid::text IN ( SELECT DISTINCT contentinpackage.contentid
                   FROM contentinpackage
                  WHERE contentinpackage.contentpackage::text = a.contentpackage::text))
          GROUP BY a.project, a.contentpackage, a.village) contentstats
     JOIN packagesindeployment pd ON pd.contentpackage::text = contentstats.contentpackage::text AND pd.project::text = contentstats.project::text
     JOIN deployments d ON pd.deployment::text = d.deployment::text AND pd.project::text = d.project::text
     JOIN languages l ON l.projectcode::text = d.project::text AND l.languagecode::text = pd.languagecode::text
  WHERE contentstats.project::text = 'CARE'::text
  ORDER BY contentstats.project, d.deploymentnumber, contentstats.contentpackage, contentstats.community;

ALTER TABLE community_stats_care
  OWNER TO lb_data_uploader;

