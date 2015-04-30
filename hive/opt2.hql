add jar hive-contrib-0.14.0.jar;
add jar bigdata-1.0.1.jar;
drop temporary function row_sequence; 
drop temporary function poweset; 
create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';
create temporary function powerset as 'org.yottabase.billing.hive.udf.PowersetUDF';


-- **** DEFINIZIONE TABELLE ****

-- crea tabella dove carica le righe dell'input (record scontrino)
DROP TABLE rows; 
CREATE TABLE rows (row STRING)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\n';

-- carica le righe
LOAD DATA LOCAL INPATH '${hiveconf:INPUT}' OVERWRITE INTO TABLE rows;

-- esegue il powerset e conta le occorrenze
INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/opt2_SubsetsFrequency'
SELECT products, count(*) AS count
	FROM(
		SELECT EXPLODE(SPLIT(POWERSET(SUBSTR(row, LOCATE(',', row) + 1 )), ',')) as products FROM rows
		) p
GROUP BY p.products;




