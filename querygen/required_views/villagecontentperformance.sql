-- View: villagecontentperformance

-- DROP VIEW villagecontentperformance;

CREATE OR REPLACE VIEW villagecontentperformance AS 
 SELECT d.project,
    d.deployment,
    d.deploymentnumber,
    cs.village,
    d.startdate,
    l.language,
    pd.contentpackage,
    cat.categoryname AS tb_category,
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
    round(cs.effectivecompletions / GREATEST(1.0, cs.started + cs.quarter + cs.half + cs.threequarter + cs.completed), 1) AS effectivecompletions_perplay,
    cs.tbcount,
    cs.started,
    cs.quarter,
    cs.half,
    cs.threequarter,
    cs.completed
   FROM deployments d
     JOIN packagesindeployment pd ON pd.project::text = d.project::text AND pd.deployment::text = d.deployment::text
     JOIN ( SELECT DISTINCT ON (contentinpackage.project, contentinpackage.contentpackage, contentinpackage.contentid) contentinpackage.project,
            contentinpackage.contentpackage,
            contentinpackage.contentid,
            contentinpackage.categoryid,
            contentinpackage."order"
           FROM contentinpackage) cp ON cp.project::text = d.project::text AND cp.contentpackage::text = pd.contentpackage::text
     JOIN categories cat ON cat.projectcode::text = d.project::text AND cat.categoryid::text = cp.categoryid::text
     JOIN contentmetadata2 cm ON cm.project::text = d.project::text AND cm.contentid::text = cp.contentid::text
     JOIN languages l ON l.projectcode::text = d.project::text AND l.languagecode::text = pd.languagecode::text
     JOIN villagecontentstatistics cs ON cs.project::text = d.project::text AND cs.contentpackage::text = pd.contentpackage::text AND cs.contentid::text = cp.contentid::text
  ORDER BY pd.contentpackage, cm.contentid;

ALTER TABLE villagecontentperformance
  OWNER TO lb_data_uploader;
COMMENT ON VIEW villagecontentperformance
  IS 'Like contentperformance, but broken down by village.

    SELECT d.project, 
        d.deployment, 
        deploymentnumber, 
        cs.village, 
        d.startdate, 
        l.language,
        pd.contentpackage, 
        cat.categoryname as tb_category, 
        cp."order",
        cm.format, 
        round(cm.duration_sec::numeric / 60.0, 1) AS duration_min,
        cm.title,
        cm.contentid,
        
        -- These columns are play events for a contentid aggregated over a village
        round(cs.timeplayed / 60.0, 0) AS played_min,
        -- A rough calculation of "played_min" based on completion statistics. 
        round((cs.started * 0.125 + cs.quarter * 0.375 + cs.half * 0.625 + cs.threequarter * 0.85 + cs.completed * 0.975) * cm.duration_sec::numeric / 60.1, 0) AS played_min_calc,
        -- Of the plays that got past 1/4, what was the average completion percentage of the play? (That is, people who got past 25% played, on average, 80% through the content.)
        round((cs.quarter * 0.375 + cs.half * 0.625 + cs.threequarter * 0.85 + cs.completed * 0.975) / GREATEST(1.0, cs.quarter + cs.half + cs.threequarter + cs.completed) * 100.0, 1) AS played_percentage_1,
        -- Of the plays that got past 10 seconds, what was the average completion percentage of the play? (That is, people who got started played, on average, 75% through the content.)
        round((cs.started * 0.125 + cs.quarter * 0.375 + cs.half * 0.625 + cs.threequarter * 0.85 + cs.completed * 0.975) / GREATEST(1.0, cs.started + cs.quarter + cs.half + cs.threequarter + cs.completed) * 100.0, 1) AS played_percentage_2,
        cs.effectivecompletions,
        -- What fraction of plays were effective completions?
        round(cs.effectivecompletions / GREATEST(1.0, cs.started + cs.quarter + cs.half + cs.threequarter + cs.completed), 1) AS effectivecompletions_perplay,
        cs.tbcount,		-- how many talking books played this at all; that is, generated a statistic for this contentid
        cs.started,        -- past 10 seconds, but not 25%
        cs.quarter,        -- 25% - 50%
        cs.half,           -- 50% - 75%
        cs.threequarter,   -- 75% - 95%
        cs.completed       -- > 95%
     
     FROM
       -- Has project, deployment, deploymentnumber, startdate 
       deployments d
       -- Adds rows for every contentpackage in (project, deployment).
       JOIN packagesindeployment pd ON pd.project = d.project AND pd.deployment = d.deployment
       -- Adds rows for every contentid in (project, contentpackage). Because contentinpackage has multiple rows for the same contentid, 
       -- pick one contentid at random.
       JOIN (SELECT DISTINCT ON (project, contentpackage, contentid)
                project, contentpackage, contentid, categoryid, "order"
                FROM contentinpackage
            ) cp ON cp.project = d.project AND cp.contentpackage = pd.contentpackage
       -- Get the category name ("Health") for each contentid categoryid. Remember that the contentid row was chosen at random.
       JOIN categories cat ON cat.projectcode = d.project AND cat.categoryid = cp.categoryid
       -- Get the format ("Song") of the contentid.
       JOIN contentmetadata2 cm ON cm.project = d.project AND cm.contentid = cp.contentid
       -- Get the language ("Daagare") for the pd languagecode ("dga")
       JOIN languages l on l.projectcode = d.project AND l.languagecode = pd.languagecode

       -- Everything so far is pre-deployment. Now add rows for the TB statistics for every (project, contentpackage, contentid)
       JOIN villagecontentstatistics cs ON cs.project = d.project AND cs.contentpackage = pd.contentpackage AND cs.contentid = cp.contentid

    -- Empirically, this ordering is the fastest execution (removing either shows it down; adding project slows it down)
    ORDER BY contentpackage, cm.contentid; 

';

