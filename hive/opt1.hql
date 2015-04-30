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
CREATE TABLE receiptRows (receiptID INT, products ARRAY<STRING>);

-- crea tabella con conteggio coppie di prodotti in ogni scontrino
DROP TABLE productReceiptPair;        
CREATE TABLE productReceiptPair (p1 STRING, p2 STRING, count INT);



-- **** CARICAMENTO INPUT ****

-- carica le righe
LOAD DATA LOCAL INPATH '${hiveconf:INPUT}' OVERWRITE INTO TABLE rows;

-- trasforma le righe in scontrini con id
INSERT INTO TABLE receiptRows 
SELECT row_sequence(), SPLIT(SUBSTR(row, LOCATE(',', row) + 1 ), ',') FROM rows;



-- calcola quante volte una coppia ricorre in uno specifico scontrino 
INSERT INTO TABLE productReceiptPair
SELECT p1.product, p2.product, count(*) AS count
FROM
		(SELECT receiptID, product
		FROM receiptRows LATERAL VIEW explode(products) exploded AS product) p1
	JOIN
		(SELECT receiptID, product
		FROM receiptRows LATERAL VIEW explode(products) exploded AS product) p2
	ON (p1.receiptID = p2.receiptID)
GROUP BY p1.product, p2.product;


-- esegue statistiche finali
INSERT OVERWRITE LOCAL DIRECTORY '${hiveconf:OUTPUT}/opt1_ProductPair'
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
SELECT pairCount.p1, pairCount.p2, pairCount.count / globalCount.count as totCount
FROM productReceiptPair globalCount JOIN productReceiptPair pairCount
WHERE globalCount.p1 = globalCount.p2
	AND	globalCount.p1 = pairCount.p1
	AND pairCount.p1 <> pairCount.p2
ORDER BY totCount DESC LIMIT 10;

