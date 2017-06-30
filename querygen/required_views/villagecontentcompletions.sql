-- View: villagecontentcompletions

-- DROP VIEW villagecontentcompletions;

CREATE OR REPLACE VIEW villagecontentcompletions AS 
 SELECT vcp.project,
    vcp.village,
    vcp.contentpackage,
    vcp.contentid,
    vcp.completionstatus,
    vcp.completioncounts
   FROM ( SELECT villagecontentperformance.project,
            villagecontentperformance.village,
            villagecontentperformance.contentpackage,
            villagecontentperformance.contentid,
            'started'::text AS completionstatus,
            villagecontentperformance.started AS completioncounts
           FROM villagecontentperformance
        UNION
         SELECT villagecontentperformance.project,
            villagecontentperformance.village,
            villagecontentperformance.contentpackage,
            villagecontentperformance.contentid,
            'quarter'::text AS text,
            villagecontentperformance.quarter
           FROM villagecontentperformance
        UNION
         SELECT villagecontentperformance.project,
            villagecontentperformance.village,
            villagecontentperformance.contentpackage,
            villagecontentperformance.contentid,
            'half'::text AS text,
            villagecontentperformance.half
           FROM villagecontentperformance
        UNION
         SELECT villagecontentperformance.project,
            villagecontentperformance.village,
            villagecontentperformance.contentpackage,
            villagecontentperformance.contentid,
            'threequarter'::text AS text,
            villagecontentperformance.threequarter
           FROM villagecontentperformance
        UNION
         SELECT villagecontentperformance.project,
            villagecontentperformance.village,
            villagecontentperformance.contentpackage,
            villagecontentperformance.contentid,
            'completed'::text AS text,
            villagecontentperformance.completed
           FROM villagecontentperformance) vcp
  ORDER BY vcp.project, vcp.contentpackage, vcp.contentid;

ALTER TABLE villagecontentcompletions
  OWNER TO lb_data_uploader;
COMMENT ON VIEW villagecontentcompletions
  IS 'Like contentcompletions, but broken down by village.';

