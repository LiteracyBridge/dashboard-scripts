-- View: contentdeployed

-- DROP VIEW contentdeployed;

CREATE OR REPLACE VIEW contentdeployed AS 
 SELECT d.project,
    d.deploymentnumber,
    l.language,
    cp.contentpackage,
    ctb.categoryname AS tb_category,
    cacm.categoryname AS acm_category,
    cp."order",
    cm.format,
    round(cm.duration_sec::numeric / 60.0, 1) AS min,
    cm.title,
    cm.contentid
   FROM deployments d
     JOIN packagesindeployment pd ON d.deployment::text = pd.deployment::text AND d.project::text = pd.project::text
     JOIN contentinpackage cp ON cp.project::text = pd.project::text AND cp.contentpackage::text = pd.contentpackage::text
     JOIN contentmetadata2 cm ON cp.project::text = cm.project::text AND cp.contentid::text = cm.contentid::text
     JOIN categoriesinpackage catp ON catp.project::text = d.project::text AND catp.categoryid::text = cp.categoryid::text AND catp.contentpackage::text = cp.contentpackage::text
     JOIN categories ctb ON ctb.projectcode::text = cp.project::text AND ctb.categoryid::text = cp.categoryid::text
     JOIN contentcategories cc ON cp.project::text = cc.projectcode::text AND cp.contentid::text = cc.contentid::text
     JOIN categories cacm ON cacm.projectcode::text = cp.project::text AND cacm.categoryid::text = cc.categoryid::text
     JOIN languages l ON l.projectcode::text = d.project::text AND l.languagecode::text = pd.languagecode::text
  ORDER BY d.project, d.deploymentnumber, l.language, cp.contentpackage, catp."order", cp."order";

ALTER TABLE contentdeployed
  OWNER TO lb_data_uploader;

