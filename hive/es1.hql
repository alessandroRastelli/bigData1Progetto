DROP TABLE rows;

CREATE TABLE rows (row STRING)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\n';


LOAD DATA LOCAL INPATH '${hiveconf:INPUT}' OVERWRITE INTO TABLE rows;

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/es1_SimpleBilling'
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
SELECT p.product, count(*) as count
FROM 
	(SELECT explode(split(substr(row, LOCATE(',', row) + 1 ), ',')) AS product FROM rows) p
GROUP BY p.product
ORDER BY count DESC;
