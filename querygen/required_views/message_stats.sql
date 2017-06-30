-- View: message_stats

-- DROP VIEW message_stats;

CREATE OR REPLACE VIEW message_stats AS 
 SELECT d.project,
    d.deploymentnumber,
    l.language,
    cp.contentpackage,
    cacm.categoryname AS acm_category,
    cm.format,
    round(cm.duration_sec::numeric / 60.0, 1) AS min,
    round(sum(a.played_seconds_max) / 3600::numeric, 0) AS hrsplayed,
    sum(a.completed_max) AS completed,
    count(DISTINCT a.talkingbook) AS tbs,
    cm.title,
    cm.contentid
   FROM deployments d
     JOIN packagesindeployment pd ON d.deployment::text = pd.deployment::text AND d.project::text = pd.project::text
     JOIN contentinpackage_uniquecontent cp ON cp.project::text = pd.project::text AND cp.contentpackage::text = pd.contentpackage::text
     JOIN contentmetadata2 cm ON cp.project::text = cm.project::text AND cp.contentid::text = cm.contentid::text
     JOIN contentcategories cc ON cp.project::text = cc.projectcode::text AND cp.contentid::text = cc.contentid::text
     JOIN categories cacm ON cacm.projectcode::text = cp.project::text AND cacm.categoryid::text = cc.categoryid::text
     JOIN languages l ON l.projectcode::text = d.project::text AND l.languagecode::text = pd.languagecode::text
     JOIN allsources_s a ON a.project::text = d.project::text AND a.contentpackage::text = cp.contentpackage::text AND a.contentid::text = cm.contentid::text
  GROUP BY d.project, d.deploymentnumber, l.language, cp.contentpackage, cacm.categoryname, cm.format, cm.title, cm.contentid, round(cm.duration_sec::numeric / 60.0, 1)
  ORDER BY d.project, d.deploymentnumber, l.language, cp.contentpackage, cacm.categoryname, cm.title;

ALTER TABLE message_stats
  OWNER TO lb_data_uploader;

