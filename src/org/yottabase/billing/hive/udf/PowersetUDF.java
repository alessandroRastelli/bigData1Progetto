package org.yottabase.billing.hive.udf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class PowersetUDF extends UDF {

	private final static int MAX_SUBSET_SIZE = 4;
	
	public Text evaluate(final Text s) {
		if (s == null) {
			return null;
		}
		
		Set<String> products = new HashSet<String>();
		
		for(String product : s.toString().split(",")){
			products.add(product.trim());
		}
		
		Set<Set<String>> productsPowerset = powersetOfMaxDimension(products, MAX_SUBSET_SIZE);
		
		boolean firstGroup = true;
		boolean firstProduct = true;
		
		String resultString = "";
		
		for(Set<String> group : productsPowerset){
			
			if(group.isEmpty()) continue;
			
			if( ! firstGroup ) 	
				resultString += ",";
			
			firstGroup=false;
			firstProduct=true;
			
			for(String product : group){
				
				 if(! firstProduct)
					resultString += " ";
				
				firstProduct  = false;
				resultString += product;
			}
		}
		
		return new Text(resultString);
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