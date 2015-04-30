
myinput = LOAD '$INPUT' USING PigStorage(',') ;

-- seleziona solo scontrini del primo trimestre 2015 
filtered = FILTER myinput BY $0 matches '2015-[012]-.*';

-- crea la tripla (mese, elenco_prodotti)
bydate = FOREACH filtered GENERATE STRSPLIT($0, '-').$1, FLATTEN(TOBAG($1..));

-- raggruppa per mese
grouped = GROUP bydate BY ($1, $0);

-- genera la struttura (prodotto, (mese, vendite_per_mese))
counted = FOREACH grouped GENERATE group.$0, (group.$1+1, COUNT(bydate));

-- raggruppa per prodotto
groupedByMonth =  GROUP counted BY $0;

STORE groupedByMonth INTO '$OUTPUT/es2_QuarterAggregation' USING PigStorage();
