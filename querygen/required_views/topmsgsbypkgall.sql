-- View: topmsgsbypkgall

-- DROP VIEW topmsgsbypkgall;

CREATE OR REPLACE VIEW topmsgsbypkgall AS 
 SELECT DISTINCT cm.title,
    cs.project,
    d.packagename AS package,
    d.contentpackage,
        CASE
            WHEN cat.categoryname::text ~~ 'General%'::text THEN "substring"(cat.categoryname::text, 9)::character varying
            ELSE cat.categoryname
        END AS category,
    cp."order",
    cm.format,
    round((cm.duration_sec / 60)::numeric, 1) AS length_minutes,
    round(cs.timeplayed / 60::numeric / cs.pkgtbs::numeric, 0) AS minutes_per_tb,
    round(cs.effectivecompletions / cs.pkgtbs::numeric, 1) AS eff_completions_per_tb,
    round(cs.timeplayed / 60::numeric, 0) AS total_minutes_played,
    cs.completed AS total_completions,
    cs.effectivecompletions AS total_effective_completions,
        CASE
            WHEN cs.completed > 0::numeric THEN round((cs.effectivecompletions - cs.completed) / cs.completed * 100::numeric, 0)
            ELSE 0::numeric
        END AS partial_percentage,
    cs.tbs,
    cs.pkgtbs,
    cs.tbs * 100 / cs.pkgtbs AS pct_tb_completions
   FROM ( SELECT a.contentid,
            a.project,
            a.contentpackage,
            sum(a.played_seconds_max) AS timeplayed,
            sum(a.effectivecompletions_max) AS effectivecompletions,
            sum(a.completed_max) AS completed,
            count(DISTINCT a.talkingbook) AS tbs,
            ( SELECT count(DISTINCT aa.talkingbook) AS count
                   FROM allsources_s aa
                  WHERE aa.contentpackage::text = a.contentpackage::text AND a.project::text = aa.project::text) AS pkgtbs
           FROM allsources_s a
          GROUP BY a.contentid, a.project, a.contentpackage) cs
     JOIN contentmetadata2 cm ON cs.contentid::text = cm.contentid::text AND cs.project::text = cm.project::text
     JOIN contentinpackage cp ON cs.contentpackage::text = cp.contentpackage::text AND cs.contentid::text = cp.contentid::text
     JOIN categories cat ON cat.categoryid::text = cp.categoryid::text AND cat.projectcode::text = cp.project::text
     JOIN packagesindeployment d ON d.contentpackage::text = cp.contentpackage::text
  ORDER BY cs.effectivecompletions DESC;

ALTER TABLE topmsgsbypkgall
  OWNER TO lb_data_uploader;
COMMENT ON VIEW topmsgsbypkgall
  IS 'View to query Top Messages by Package.';

