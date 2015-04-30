package org.yottabase.billing.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PowersetUDF extends EvalFunc<DataBag> {

	private final static int MAX_SUBSET_SIZE = 4;
	
	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		
		DataBag prods = (DataBag) tuple.get(0);
		
		BagFactory bagFactory = BagFactory.getInstance();
		TupleFactory tupleFactory = TupleFactory.getInstance();
		
		/*
		 * Pour tuple elements into an ordered set:
		 * - removes duplicates
		 * - common sorting keeps same sets alike
		 */
		Set<String> products = new TreeSet<String>();
		for (Tuple prod : prods)
			products.add(prod.get(0).toString());
		
		/* Compute powerset of products set */
		Set<Set<String>> powerset = powersetOfMaxDimension(products, MAX_SUBSET_SIZE);
		
		/* Convert powerset into bag of tuples */
		DataBag bag = bagFactory.newDefaultBag();
		for (Set<String> subset : powerset){
			
			if ( !subset.isEmpty() ) {
				Tuple container = tupleFactory.newTuple();
				
				Tuple t = tupleFactory.newTuple();
				for (String p : subset)
					t.append(p);
				
				container.append(t);
				bag.add(container);	
			}
		}
		
		return bag;
	}
	
	/**
	 * Computes the powerset of the set given as input parameter,
	 * that is the set containing all subsets of the input set 
	 * (each of which has dimension not grater than maxDim)
	 * 
	 * @param originalSet is the input set
	 * @param maxDim is the maximum dimension allowed for a powerset element
	 * @return the powerset of the input set, with elements not larger than maxDim
	 */
	private static Set<Set<String>> powersetOfMaxDimension(Set<String> originalSet, int maxDim) {
		Set<Set<String>> powerset = new HashSet<Set<String>>();
		if ( originalSet.isEmpty() ) {
			powerset.add( new TreeSet<String>() );
			return powerset;
		}
		
		List<String> list = new ArrayList<String>(originalSet);
		String head = list.get(0);
		Set<String> rest = new TreeSet<String>( list.subList(1, list.size()) );
		for ( Set<String> set : powersetOfMaxDimension(rest, maxDim) )
			if ( set.size() < maxDim ) {
				Set<String> newSet = new TreeSet<String>();
				newSet.add(head);
				newSet.addAll(set);
				
				powerset.add(newSet);
				powerset.add(set);
			}
		return powerset;
	}

}
