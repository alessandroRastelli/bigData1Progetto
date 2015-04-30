
records = LOAD '$INPUT' USING PigStorage(',') ;

-- lista di scontrini senza data 
products = FOREACH records GENERATE TOBAG($1..) AS recordProds;

-- crea tutte le coppie di prodotti presenti in ogni scontrino
pairs = FOREACH products {
	recordPairs = CROSS recordProds, recordProds;
	GENERATE FLATTEN(recordPairs) as (p1,p2);
}

-- raggruppa le coppie con elementi diversi e conta il numero di occorrenze per coppia
assocs = FILTER pairs BY p1 < p2; -- filtra le coppie permutazione
assocsGrouped = GROUP assocs BY (p1, p2);
assocsCount = FOREACH assocsGrouped GENERATE group, (float) COUNT(assocs) as count1;

-- raggruppa le coppie con elementi uguali e conta il numero di occorrenze per coppia
prods = FILTER pairs BY p1 == p2; 
prodsGrouped = GROUP prods BY (p1, p2);
prodsCount = FOREACH prodsGrouped GENERATE group, (float) COUNT(prods) as count2;

-- effettua il join tra coppie prodotto (p1, p1) e coppie associazioni (p1, p2)
joined = JOIN assocsCount BY group.$0, prodsCount BY group.$0;

-- calcola le percentuali per ciascuna associazione
prodPercents = FOREACH joined GENERATE $0, $1/$3 AS percent;

-- ordina le associazioni per percentuale decrescente
sorted = ORDER prodPercents BY percent DESC; 

-- estrae le prime 10 associazioni
topK = LIMIT sorted 10;

STORE topK INTO '$OUTPUT/opt1_ProductPair' USING PigStorage();

