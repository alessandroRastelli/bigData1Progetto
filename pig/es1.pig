

myinput = LOAD '$INPUT' USING PigStorage(',') ;

-- lista di prodotti senza data 
products = FOREACH myinput GENERATE FLATTEN(TOBAG($1..));

-- raggruppa per prodotto
grouped = GROUP products BY $0;

-- conta numero di prodotti
counts = FOREACH grouped GENERATE group, COUNT(products);

-- ordina per conteggio
sorted = ORDER counts BY $1 DESC; 

STORE sorted INTO '$OUTPUT/es1_SimpleBilling' USING PigStorage();
