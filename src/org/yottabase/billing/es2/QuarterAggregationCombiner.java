package org.yottabase.billing.es2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QuarterAggregationCombiner extends
		Reducer<Text, CountByMounth, Text, CountByMounth> {
	
	
	public void reduce(Text key, Iterable<CountByMounth> values, Context context) 
			throws IOException, InterruptedException {
		
		int[] counts =  new int[3];
		
		for(CountByMounth cm : values)
			counts[cm.getMounth()] += cm.getCount();
		
		for(int i = 0; i< counts.length; i++)
			context.write(key, new CountByMounth(i, counts[i]));
		
	}
			
}
