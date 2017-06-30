-- View: contentperformance

-- DROP VIEW contentperformance;

CREATE OR REPLACE VIEW contentperformance AS 
 SELECT d.project,
    d.deploymentnumber,
    d.startdate,
    l.language,
    cp.contentpackage,
    ctb.categoryname AS tb_category,
    cacm.categoryname AS acm_category,
    cp."order",
    cm.format,
    round(cm.duration_sec::numeric / 60.0, 1) AS duration_min,
    cm.title,
    cm.contentid,
    round(cs.timeplayed / 60.0, 0) AS played_min,
    round((cs.started * 0.125 + cs.quarter * 0.375 + cs.half * 0.625 + cs.threequarter * 0.85 + cs.completed * 0.975) * cm.duration_sec::numeric / 60.1, 0) AS played_min_calc,
    round((cs.quarter * 0.375 + cs.half * 0.625 + cs.threequarter * 0.85 + cs.completed * 0.975) / GREATEST(1.0, cs.quarter + cs.half + cs.threequarter + cs.completed) * 100.0, 1) AS played_percentage_1,
    round((cs.started * 0.125 + cs.quarter * 0.375 + cs.half * 0.625 + cs.threequarter * 0.85 + cs.completed * 0.975) / GREATEST(1.0, cs.started + cs.quarter + cs.half + cs.threequarter + cs.completed) * 100.0, 1) AS played_percentage_2,
    cs.effectivecompletions,
    round(cs.effectivecompletions / cs.pkgtbcount::numeric, 1) AS effectivecompletions_pertb,
    round((cs.started + cs.quarter + cs.half + cs.threequarter + cs.completed) / cs.pkgtbcount::numeric, 1) AS plays_pertb,
    round(cs.effectivecompletions / (cs.started + cs.quarter + cs.half + cs.threequarter + cs.completed), 1) AS effectivecompletions_perplay,
    cs.tbcount,
    cs.pkgtbcount,
    cs.started,
    cs.quarter,
    cs.half,
    cs.threequarter,
    cs.completed
   FROM deployments d
     JOIN packagesindeployment pd ON d.deployment::text = pd.deployment::text AND d.project::text = pd.project::text
     JOIN ( SELECT DISTINCT ON (contentinpackage.project, contentinpackage.contentpackage, contentinpackage.contentid) contentinpackage.project,
            contentinpackage.contentpackage,
            contentinpackage.contentid,
            contentinpackage.categoryid,
            contentinpackage."order"
           FROM contentinpackage) cp ON cp.project::text = pd.project::text AND cp.contentpackage::text = pd.contentpackage::text
     JOIN contentmetadata2 cm ON cp.project::text = cm.project::text AND cp.contentid::text = cm.contentid::text
     JOIN categoriesinpackage catp ON catp.project::text = d.project::text AND catp.categoryid::text = cp.categoryid::text AND catp.contentpackage::text = cp.contentpackage::text
     JOIN categories ctb ON ctb.projectcode::text = cp.project::text AND ctb.categoryid::text = cp.categoryid::text
     JOIN contentcategories cc ON cp.project::text = cc.projectcode::text AND cp.contentid::text = cc.contentid::text
     JOIN categories cacm ON cacm.projectcode::text = cp.project::text AND cacm.categoryid::text = cc.categoryid::text
     JOIN languages l ON l.projectcode::text = d.project::text AND l.languagecode::text = pd.languagecode::text
     JOIN contentstatistics cs ON cs.contentid::text = cm.contentid::text AND cs.project::text = d.project::text AND cs.contentpackage::text = pd.contentpackage::text
  ORDER BY d.project, d.deploymentnumber, l.language, cp.contentpackage, round(cs.effectivecompletions / cs.pkgtbcount::numeric, 1) DESC;

ALTER TABLE contentperformance
  OWNER TO lb_data_uploader;
COMMENT ON VIEW contentperformance
  IS 'The listening performance of deployed content. Roughly parallel to contentdeployed.
';

