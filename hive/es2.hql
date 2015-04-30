add jar hive-contrib-0.14.0.jar;
drop temporary function row_sequence; 
create temporary function row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';


-- **** DEFINIZIONE TABELLE ****

-- crea tabella dove carica le righe dell'input (record scontrino)
DROP TABLE rows; 
CREATE TABLE rows (row STRING)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\n';

-- crea tabella con id univoco scontrino ed array prodotti
DROP TABLE receiptRows;        
CREATE TABLE receiptRows (receiptID STRING, products ARRAY<STRING>);

-- crea tabella con mese e prodotto
DROP TABLE monthProduct;        
CREATE TABLE monthProduct (month STRING, product STRING);

-- crea tabella con prodotto mese e count
DROP TABLE output;        
CREATE TABLE output (product STRING,month STRING,items INT);
-- **** CARICAMENTO INPUT ****

-- carica le righe
LOAD DATA LOCAL INPATH '${hiveconf:INPUT}' OVERWRITE INTO TABLE rows;

-- trasforma le righe in scontrini con data
INSERT INTO TABLE receiptRows 
SELECT  SPLIT(SUBSTR(row, LOCATE('-', row),3), '-'),SPLIT(SUBSTR(row, LOCATE(',', row) + 1 ), ',') FROM rows;

--creo tabella data,prodotto
INSERT INTO TABLE monthProduct
SELECT p1.receiptID,p1.product
FROM
		(SELECT receiptID, product
		FROM receiptRows LATERAL VIEW explode(products) exploded AS product) p1
WHERE p1.receiptID<3;

--

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/es2_QuarterAggregation'
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
SELECT product, concat_ws(' ',collect_set(p2.mounthCount))
FROM (
	SELECT product, concat(m, '/2015:' , count) AS mounthCount
	FROM (
		SELECT product,CAST(month+1 AS INT) AS m, count(*) AS count
		FROM monthProduct
		GROUP BY product,month) p1 ) p2 
GROUP BY product;