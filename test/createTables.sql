-- This creates the tables that are needed by the scripts.
-- usage: $psql $dbcxn -f createTables.sql
--        where $psql and $dbcxn are set as by the scripts.


CREATE TABLE IF NOT EXISTS categories
(
  categoryid character varying(255),
  categoryname character varying(255),
  projectcode character varying(16)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE categories
  OWNER TO lb_data_uploader;



CREATE TABLE IF NOT EXISTS categoriesinpackage
(
  project character varying(255) NOT NULL,
  contentpackage character varying(255) NOT NULL,
  categoryid character varying(255) NOT NULL,
  "order" integer NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE categoriesinpackage
  OWNER TO lb_data_uploader;



CREATE SEQUENCE communities_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 683
  CACHE 1;
ALTER TABLE communities_seq
  OWNER TO lb_data_uploader;
CREATE TABLE communities
(
  communityname character varying(255) NOT NULL,
  year integer,
  tbs integer,
  households integer,
  lat double precision,
  "long" double precision,
  languagecode character varying(16),
  district character varying(255),
  subdistrict character varying(255),
  notes text,
  project character varying(255) NOT NULL,
  id integer NOT NULL DEFAULT nextval('communities_seq'::regclass),
  survey2015 boolean,
  CONSTRAINT communities_pkey PRIMARY KEY (project, communityname)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE communities
  OWNER TO lb_data_uploader;



CREATE TABLE IF NOT EXISTS contentinpackage
(
  project character varying(255) NOT NULL,
  contentpackage character varying(255) NOT NULL,
  contentid character varying(255) NOT NULL,
  categoryid character varying(255) NOT NULL,
  "order" integer NOT NULL,
  CONSTRAINT contentinpackage_pkey PRIMARY KEY (project, contentpackage, contentid, categoryid, "order")
)
WITH (
  OIDS=FALSE
);
ALTER TABLE contentinpackage
  OWNER TO lb_data_uploader;



CREATE TABLE IF NOT EXISTS contentmetadata2
(
  title character varying(255),
  dc_publisher character varying(255),
  contentid character varying(255) NOT NULL,
  source character varying(255),
  languagecode character varying(16),
  relatedid character varying(255),
  dtb_revision integer,
  duration_sec integer,
  format character varying(255),
  targetaudience character varying(255),
  daterecorded character varying(255),
  keywords character varying(255),
  timing character varying(255),
  speaker character varying(255),
  goal character varying(255),
  transcriptionurl character varying(255),
  notes character varying(5000),
  community character varying(255),
  status character varying(255),
  categories character varying(255),
  quality character varying(255),
  project character varying(255) NOT NULL,
  CONSTRAINT contentmetadata2_pkey PRIMARY KEY (project, contentid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE contentmetadata2
  OWNER TO lb_data_uploader;



CREATE TABLE IF NOT EXISTS languages
(
  languagecode character varying(16) NOT NULL,
  language character varying(255) NOT NULL,
  projectcode character varying(16) NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE languages
  OWNER TO lb_data_uploader;



CREATE TABLE IF NOT EXISTS packagesindeployment
(
  project character varying(255) NOT NULL,
  deployment character varying(255) NOT NULL,
  contentpackage character varying(255) NOT NULL,
  packagename character varying(255),
  "startDate" date,
  "endDate" date,
  languagecode character varying(255),
  groups character varying(255),
  distribution character varying(255)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE packagesindeployment
  OWNER TO lb_data_uploader;



CREATE SEQUENCE tbcollections_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 417959
  CACHE 1;
ALTER TABLE tbcollections_seq
  OWNER TO lb_data_uploader;
CREATE TABLE IF NOT EXISTS tbcollections
(
  contentpackage character varying(255) NOT NULL,
  communityname character varying(255) NOT NULL,
  talkingbook character varying(255) NOT NULL,
  id integer NOT NULL DEFAULT nextval('tbcollections_seq'::regclass),
  CONSTRAINT tbcollections_pkey PRIMARY KEY (contentpackage, communityname, talkingbook)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE tbcollections
  OWNER TO lb_data_uploader;



CREATE TABLE allsources_s
(
  project character varying(255) NOT NULL,
  contentpackage character varying(255) NOT NULL,
  village character varying(255) NOT NULL,
  talkingbook character varying(255) NOT NULL,
  contentid character varying(255) NOT NULL,
  played_seconds_logevents bigint,
  played_seconds_logs bigint,
  played_seconds_stats bigint,
  played_seconds_flash bigint,
  played_seconds_max bigint,
  effectivecompletions_max bigint,
  effecticecompletions_flash bigint,
  started_logevents bigint,
  started_logs bigint,
  starts_stats bigint,
  started_flash bigint,
  started_min bigint,
  started_max bigint,
  started_variance bigint,
  quarter_logevents bigint,
  quarter_logs bigint,
  quarter_flash bigint,
  quarter_max bigint,
  half_logevents bigint,
  half_logs bigint,
  half_flash bigint,
  half_max bigint,
  threequarters_logevents bigint,
  threequarters_logs bigint,
  threequarters_flash bigint,
  threequarters_max bigint,
  completed_logevents bigint,
  completed_logs bigint,
  completed_stats bigint,
  completed_flash bigint,
  completed_min bigint,
  completed_max bigint,
  completed_variance bigint
)
WITH (
  OIDS=FALSE
);
ALTER TABLE allsources_s
  OWNER TO lb_data_uploader;



-- Views
--

CREATE OR REPLACE VIEW logstatsfromplayedevents AS 
 SELECT foo.packageid,
    foo.village,
    foo.talkingbookid,
    foo.contentid,
    foo.totaltime,
    sum(foo.timeplayed) AS timeplayed,
    sum(foo.countstarted) AS started,
    sum(foo.countquarter) AS quarter,
    sum(foo.counthalf) AS half,
    sum(foo.countthreequarters) AS threequarters,
    sum(foo.countcompleted) AS completed
   FROM ( SELECT playedevents.talkingbookid,
            playedevents.contentid,
            playedevents.timeplayed,
            playedevents.totaltime,
            playedevents.percentdone,
            playedevents.isfinished,
            playedevents.cycle,
            playedevents.period,
            playedevents.dayinperiod,
            playedevents.timeinday,
            playedevents.packageid,
            playedevents.village,
                CASE
                    WHEN playedevents.percentdone < 0.25::double precision AND (playedevents.totaltime - playedevents.timeplayed) > 10 AND NOT playedevents.isfinished THEN 1
                    ELSE 0
                END AS countstarted,
                CASE
                    WHEN playedevents.percentdone >= 0.25::double precision AND playedevents.percentdone <= 0.49999::double precision AND (playedevents.totaltime - playedevents.timeplayed) > 10 AND NOT playedevents.isfinished THEN 1
                    ELSE 0
                END AS countquarter,
                CASE
                    WHEN playedevents.percentdone >= 0.499991::double precision AND playedevents.percentdone <= 0.74999::double precision AND (playedevents.totaltime - playedevents.timeplayed) > 10 AND NOT playedevents.isfinished THEN 1
                    ELSE 0
                END AS counthalf,
                CASE
                    WHEN playedevents.percentdone >= 0.749991::double precision AND playedevents.percentdone <= 0.94999::double precision AND (playedevents.totaltime - playedevents.timeplayed) > 10 AND NOT playedevents.isfinished THEN 1
                    ELSE 0
                END AS countthreequarters,
                CASE
                    WHEN playedevents.percentdone > 0.94999::double precision OR (playedevents.totaltime - playedevents.timeplayed) < 10 OR playedevents.isfinished THEN 1
                    ELSE 0
                END AS countcompleted
           FROM playedevents
          WHERE playedevents.timeplayed >= 10 AND playedevents.totaltime > 0 AND playedevents.contentid::text <> 'dga'::text) foo
  GROUP BY foo.packageid, foo.village, foo.talkingbookid, foo.contentid, foo.totaltime;

ALTER TABLE logstatsfromplayedevents
  OWNER TO lb_data_uploader;



CREATE OR REPLACE VIEW allsources AS 
 SELECT c.project,
    s0.contentpackage,
    s0.village,
    s0.talkingbook,
    s0.contentid,
    l.timeplayed AS played_seconds_logevents,
    s1.totaltimeplayed AS played_seconds_logs,
    s2.countcompleted::numeric * 1.23 * cm.duration_sec::numeric AS played_seconds_stats,
    s3.totaltimeplayed AS played_seconds_flash,
    GREATEST(l.timeplayed::numeric, s1.totaltimeplayed::bigint::numeric, s2.countcompleted::numeric * 1.23 * cm.duration_sec::numeric, s3.totaltimeplayed::bigint::numeric) AS played_seconds_max,
    GREATEST((COALESCE(0.3 * GREATEST(l.quarter, s1.countquarter::bigint, s3.countquarter::bigint)::numeric, 0::numeric) + COALESCE(0.6 * GREATEST(l.half, s1.counthalf::bigint, s3.counthalf::bigint)::numeric, 0::numeric) + COALESCE(0.83 * GREATEST(l.threequarters, s1.countthreequarters::bigint, s3.countthreequarters::bigint)::numeric, 0::numeric) + COALESCE(GREATEST(l.completed, s1.countcompleted::bigint, s2.countcompleted::bigint, s3.countcompleted::bigint)::numeric, 0::numeric))::bigint, (s2.countcompleted::bigint::numeric * 1.23)::bigint) AS effectivecompletions_max,
    0.3 * s3.countquarter::bigint::numeric + 0.6 * s3.counthalf::bigint::numeric + 0.83 * s3.countthreequarters::bigint::numeric + s3.countcompleted::bigint::numeric AS effecticecompletions_flash,
    l.started AS started_logevents,
    s1.countstarted AS started_logs,
    s2.countstarted AS started_stats,
    s3.countstarted AS started_flash,
    LEAST(l.started, s1.countstarted::bigint, s2.countstarted::bigint, s3.countstarted::bigint) AS started_min,
    GREATEST(l.started, s1.countstarted::bigint, s2.countstarted::bigint, s3.countstarted::bigint) AS started_max,
    GREATEST(l.started, s1.countstarted::bigint, s2.countstarted::bigint, s3.countstarted::bigint) - LEAST(l.started, s1.countstarted::bigint, s2.countstarted::bigint, s3.countstarted::bigint) AS started_variance,
    l.quarter AS quarter_logevents,
    s1.countquarter AS quarter_logs,
    s3.countquarter AS quarter_flash,
    GREATEST(l.quarter, s1.countquarter::bigint, s3.countquarter::bigint) AS quarter_max,
    l.half AS half_logevents,
    s1.counthalf AS half_logs,
    s3.counthalf AS half_flash,
    GREATEST(l.half, s1.counthalf::bigint, s3.counthalf::bigint) AS half_max,
    l.threequarters AS threequarters_logevents,
    s1.countthreequarters AS threequarters_logs,
    s3.countthreequarters AS threequarters_flash,
    GREATEST(l.threequarters, s1.countthreequarters::bigint, s3.countthreequarters::bigint) AS threequarters_max,
    l.completed AS completed_logevents,
    s1.countcompleted AS completed_logs,
    s2.countcompleted AS completed_stats,
    s3.countcompleted AS completed_flash,
    LEAST(l.completed, s1.countcompleted::bigint, s2.countcompleted::bigint, s3.countcompleted::bigint) AS completed_min,
    GREATEST(l.completed, s1.countcompleted::bigint, s2.countcompleted::bigint, s3.countcompleted::bigint) AS completed_max,
    GREATEST(l.completed, s1.countcompleted::bigint, s2.countcompleted::bigint, s3.countcompleted::bigint) - LEAST(s1.countcompleted, s2.countcompleted, s3.countcompleted) AS completed_variance
   FROM ( SELECT DISTINCT syncaggregation.contentpackage,
            syncaggregation.village,
            syncaggregation.talkingbook,
            syncaggregation.contentid
           FROM syncaggregation
        UNION
         SELECT DISTINCT playedevents.packageid AS pkg,
            playedevents.village AS vlg,
            playedevents.talkingbookid AS tb,
            playedevents.contentid AS cid
           FROM playedevents) s0
     JOIN communities c ON s0.village::text = c.communityname::text
     JOIN contentmetadata2 cm ON cm.contentid::text = s0.contentid::text AND cm.project::text = c.project::text
     LEFT JOIN logstatsfromplayedevents l ON s0.contentid::text = l.contentid::text AND s0.talkingbook::text = l.talkingbookid::text AND s0.contentpackage::text = l.packageid::text AND s0.village::text = l.village::text
     LEFT JOIN syncaggregation s1 ON s0.contentid::text = s1.contentid::text AND s0.talkingbook::text = s1.talkingbook::text AND s0.contentpackage::text = s1.contentpackage::text AND s0.village::text = s1.village::text AND s1.datasource = 1
     LEFT JOIN syncaggregation s2 ON s0.contentid::text = s2.contentid::text AND s0.talkingbook::text = s2.talkingbook::text AND s0.contentpackage::text = s2.contentpackage::text AND s0.village::text = s2.village::text AND s2.datasource = 2
     LEFT JOIN syncaggregation s3 ON s0.contentid::text = s3.contentid::text AND s0.talkingbook::text = s3.talkingbook::text AND s0.contentpackage::text = s3.contentpackage::text AND s0.village::text = s3.village::text AND s3.datasource = 3;

ALTER TABLE allsources
  OWNER TO lb_data_uploader;



-- Change ownership of tables not created here
-- TODO: it is possible that we need to actually create the tables here, in which case
-- copy the definitions from psql.

ALTER TABLE c3p0_test_table
  OWNER TO lb_data_uploader;


ALTER TABLE playedevents
  OWNER TO lb_data_uploader;


ALTER TABLE recordevents
  OWNER TO lb_data_uploader;


ALTER TABLE surveyevents
  OWNER TO lb_data_uploader;


ALTER TABLE syncaggregation
  OWNER TO lb_data_uploader;


ALTER TABLE syncoperationlog
  OWNER TO lb_data_uploader;


ALTER TABLE talkingbookcorruption
  OWNER TO lb_data_uploader;


ALTER TABLE tbdataoperations
  OWNER TO lb_data_uploader;


ALTER TABLE updaterecord
  OWNER TO lb_data_uploader;


ALTER TABLE updatevalidationerror
  OWNER TO lb_data_uploader;


ALTER TABLE village
  OWNER TO lb_data_uploader;

