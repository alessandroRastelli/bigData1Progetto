add jars hive-contrib-0.14.0.jar;
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
CREATE TABLE receiptRows (receiptID INT, products ARRAY<STRING>);



-- **** CARICAMENTO INPUT ****

-- carica le righe
LOAD DATA LOCAL INPATH '${hiveconf:INPUT}' OVERWRITE INTO TABLE rows;

-- trasforma le righe in scontrini con id
INSERT INTO TABLE receiptRows 
SELECT row_sequence(), SPLIT(SUBSTR(row, LOCATE(',', row) + 1 ), ',') FROM rows;



-- **** PROCESSAMENTO ****

INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/es3_ProductPair'
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
SELECT p1.product, p2.product, count(*) AS count
FROM
		(SELECT receiptID, product
		FROM receiptRows LATERAL VIEW explode(products) exploded AS product) p1
	JOIN
		(SELECT receiptID, product
		FROM receiptRows LATERAL VIEW explode(products) exploded AS product) p2
	ON (p1.receiptID = p2.receiptID)
WHERE p1.product < p2.product
GROUP BY p1.product, p2.product
ORDER BY count DESC LIMIT 10;

