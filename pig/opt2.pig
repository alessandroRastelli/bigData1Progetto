
REGISTER '$EXTLIB'
DEFINE powerset org.yottabase.billing.pig.udf.PowersetUDF();

records = LOAD '$INPUT' USING PigStorage(',') ;

-- lista di scontrini senza data 
products = FOREACH records GENERATE TOBAG($1..) AS recordProds;

-- genera elementi dell'insieme delle parti dei prodotti di ogni scontrino
subsets = 
	FOREACH products {
		subset = powerset(recordProds);
		GENERATE FLATTEN(subset);
}

-- raggruppa per insieme di prodotti
grouped = GROUP subsets BY $0;

-- conta il numero di occorrenze per coppia
counts = FOREACH grouped GENERATE group, COUNT(subsets);

STORE counts INTO '$OUTPUT/opt2_SubsetsFrequency' USING PigStorage();
