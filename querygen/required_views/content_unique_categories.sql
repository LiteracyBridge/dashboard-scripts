-- This view is required for the querygen created SQL.

CREATE OR REPLACE VIEW public.content_unique_categories AS 

SELECT cm.project,
    cm.contentid,
    c.categoryid,
    c.categoryname
   FROM contentmetadata2 cm
     JOIN categories c
       -- the cm.categories is zero or more category names, separated by commas. The most
       -- salient category names are (tend to be) first, so this matches on that. 
       ON cm.categories ILIKE (c.categoryname || '%') 
       AND cm.project = c.projectcode
       AND NOT (
           -- exclude categoryids from the set of non-leaf categoryids
           c.categoryid IN ( 
               -- category ids 'id' such that there is another categoryid that is 'id-%'
               SELECT parent.categoryid
                   FROM categories parent
                     JOIN categories child 
                       ON child.categoryid LIKE (parent.categoryid || '-%')