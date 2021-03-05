
CREATE OR REPLACE TEMP VIEW program_info AS (
    SELECT projectcode AS project
        ,project as description
    FROM projects
);
