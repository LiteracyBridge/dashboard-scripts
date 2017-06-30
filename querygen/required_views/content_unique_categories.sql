-- This view is required for the querygen created SQL.

-- View: public.content_unique_categories

-- DROP VIEW public.content_unique_categories;

CREATE OR REPLACE VIEW public.content_unique_categories AS 
 SELECT cm.project,
    cm.contentid,
    c.categoryid,
    c.categoryname
   FROM contentmetadata2 cm
     JOIN categories c ON cm.categories::text ~~* (c.categoryname::text || '%'::text) AND cm.project::text = c.projectcode::text AND NOT (c.categoryid::text IN ( SELECT parent.categoryid
           FROM categories parent
             JOIN categories child ON child.categoryid::text ~~ (parent.categoryid::text || '-%'::text)));

ALTER TABLE public.content_unique_categories
  OWNER TO lb_data_uploader;
COMMENT ON VIEW public.content_unique_categories
  IS '-- Annotated definition

SELECT cm.project,
    cm.contentid,
    c.categoryid,
    c.categoryname
   FROM contentmetadata2 cm
     JOIN categories c
       -- the cm.categories is zero or more category names, separated by commas. The most
       -- salient category names are (tend to be) first, so this matches on that.
       ON cm.categories ILIKE (c.categoryname || ''%'')
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
                    
