package org.yottabase.billing.optional.es2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * For each record, emits a counter for each
 * 	- single element within the record
 * 	- elements pair within the record
 * 	- elements triad within the record
 * 	- elements quadruple within the record
 * 
 * So doing, the generated item combinations are all and only those
 * having at least one occurrence in some record 
 * 
 * @author hduser
 *
 */
public class SubsetsFrequencyMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);

	private final static int MAX_SUBSET_SIZE = 4;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Tokenize input line */
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
		
		/* Skip record date */
		tokenizer.nextToken();

		/* Collect unique items sorted by name */
		Set<Text> items = new TreeSet<Text>();
		while ( tokenizer.hasMoreTokens() )
			items.add(new Text( tokenizer.nextToken() ));
		
		/* Compute items powerset */
		Set<Set<Text>> powerset = powersetOfMaxDimension(items, MAX_SUBSET_SIZE);
		
		/* Iterate over subsets and emit each as string key */
		for (Set<Text> subset : powerset)
			if ( !subset.isEmpty() ) {
				String subseq = new String();
				Iterator<Text> iterator = subset.iterator();
				
				while ( iterator.hasNext() )
					subseq += iterator.next().toString() + " ";
				
				context.write(new Text(subseq), ONE);
			}
		
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
	private static Set<Set<Text>> powersetOfMaxDimension(Set<Text> originalSet, int maxDim) {
		Set<Set<Text>> powerset = new HashSet<Set<Text>>();
		if ( originalSet.isEmpty() ) {
			powerset.add( new TreeSet<Text>() );
			return powerset;
		}
		
		List<Text> list = new ArrayList<Text>(originalSet);
		Text head = list.get(0);
		Set<Text> rest = new TreeSet<Text>( list.subList(1, list.size()) );
		for ( Set<Text> set : powersetOfMaxDimension(rest, maxDim) )
			if ( set.size() < maxDim ) {
				Set<Text> newSet = new TreeSet<Text>();
				newSet.add(head);
				newSet.addAll(set);
				
				powerset.add(newSet);
				powerset.add(set);
			}
		return powerset;
	}

}