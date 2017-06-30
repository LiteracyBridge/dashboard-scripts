-- View: contentcategories

-- DROP VIEW contentcategories;

CREATE OR REPLACE VIEW contentcategories AS 
 SELECT cm.project AS projectcode,
    cm.contentid,
    c.categoryid
   FROM contentmetadata2 cm
     JOIN categories c ON cm.categories::text ~~* (('%'::text || c.categoryname::text) || '%'::text) AND cm.project::text = c.projectcode::text AND NOT (c.categoryid::text IN ( SELECT parent.categoryid
           FROM categories child
             JOIN categories parent ON child.categoryid::text ~~ (parent.categoryid::text || '-%'::text)));

ALTER TABLE contentcategories
  OWNER TO lb_data_uploader;
COMMENT ON VIEW contentcategories
  IS '-- Annotated definition

SELECT cm.project,
    cm.contentid,
    c.categoryid
   FROM contentmetadata2 cm
     JOIN categories c
       -- the cm.categories is zero or more category names, separated by commas, so
       -- this matches cm.categories that contain the category name anywhere
       ON cm.categories ILIKE (''%'' || c.categoryname || ''%'') 
       AND cm.project = c.projectcode
       AND NOT (
           -- exclude categoryids from the set of non-leaf categoryids
           c.categoryid IN ( 
               -- category ids ''id'' such that there is another categoryid that is ''id-%''
               SELECT parent.categoryid
                   FROM categories parent
                     JOIN categories child 
                       ON child.categoryid LIKE (parent.categoryid || ''-%'')
           )
       )';

