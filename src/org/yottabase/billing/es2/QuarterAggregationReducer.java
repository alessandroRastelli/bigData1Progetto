package org.yottabase.billing.es2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QuarterAggregationReducer extends
		Reducer<Text, CountByMounth, Text, Text> {
	
	
	public void reduce(Text key, Iterable<CountByMounth> values, Context context) 
			throws IOException, InterruptedException {
		
		int[] counts =  new int[3];
		
		for(CountByMounth cm : values)
			counts[cm.getMounth()] += cm.getCount();
		
		String out = "";
		for(int i = 0; i< counts.length; i++)
			out += (i + 1) + "/2015:" + counts[i] + ((i != counts.length -1 ) ? " " : "");
		
		context.write(key, new Text(out));
	}
			
}
