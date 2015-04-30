package org.yottabase.billing.optional.es1;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductPairReducer extends
		Reducer<Text, ProductCount, Text, FloatWritable> {
	
	public void reduce(Text p1, Iterable<ProductCount> values,
			Context context) throws IOException, InterruptedException {
		
		Text p2;
		IntWritable c2;
		
		//collassa le coppie uguali
		Map<Text, IntWritable> productsMap = new TreeMap<Text, IntWritable>();
		for(ProductCount productCount : values){
			p2 = productCount.getProduct();
			c2 = productsMap.get(p2);
			
			if(c2 == null)
				productsMap.put(p2, productCount.getCount());
			else
				c2.set(c2.get() + productCount.getCount().get());
			
		}
		
		//calcola le percentuali
		IntWritable c1 = productsMap.remove(p1);
		float percent;
		String outputKey;
		
		for(Entry<Text, IntWritable> entry : productsMap.entrySet()){
			
			p2 = entry.getKey();
			c2 = entry.getValue();
		
			percent = (float)c2.get() / (float)c1.get();
			
			outputKey = "(" + p1.toString() + "," + p2.toString() + ")";
			
			context.write(new Text(outputKey), new FloatWritable(percent));
			
		}
	}
	
}
