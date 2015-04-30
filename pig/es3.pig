
records = LOAD '$INPUT' USING PigStorage(',') ;

-- lista di scontrini senza data 
products = FOREACH records GENERATE TOBAG($1..) AS recordProds;

-- crea tutte le coppie di prodotti presenti in ogni scontrino
pairs = FOREACH products {
	recordPairs = CROSS recordProds, recordProds;
	GENERATE FLATTEN(recordPairs) as (p1,p2);
} 

-- elimina coppie permutazione ed identità
filtered = FILTER pairs BY p1 < p2; 

-- raggruppa le coppie uguali
grouped = GROUP filtered BY (p1, p2);

-- conta il numero di occorrenze per coppia
counts = FOREACH grouped GENERATE group, COUNT(filtered) as count;

-- ordina per numero di occorrenza
sorted = ORDER counts BY count DESC;  

-- estrae i primi 10
topK = LIMIT sorted 10;

STORE topK INTO '$OUTPUT/es3_ProductPair' USING PigStorage();

