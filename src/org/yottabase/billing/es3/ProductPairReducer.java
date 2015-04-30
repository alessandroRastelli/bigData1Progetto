package org.yottabase.billing.es3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductPairReducer extends
		Reducer<ProductPair, IntWritable, ProductPair, IntWritable> {
	
	public void reduce(ProductPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
	
		int tot = 0;
		for(IntWritable count : values){
			tot += count.get();
		}
		
		context.write( key, new IntWritable(tot) );
	}	
	
}
