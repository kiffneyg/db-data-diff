checkdbdiffs.py 

compares two database schemas.

It checks all tables within a schema 
for rowcounts, latest timestamps, and a hash sum of ALL timestamps.

If there is a more common field name for timestamps (like LAST_MODIFIED_DATE in LYNXX), edit the large
SQL named "dsql" and put that field name in this part:

                    ORDER BY
                       CASE
                          WHEN column_name LIKE 'ADDED_DATE' THEN 2
                          WHEN column_name LIKE 'LAST_MODIFIED_DATE' THEN 1
                          ELSE 3

giving the date field most commonly used as order 1, the second choice as 2.  Otherwise
the script will randomly choose a date field in the table.						  

