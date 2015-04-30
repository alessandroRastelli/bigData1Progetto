package org.yottabase.billing.optional.es1;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductPairCombiner extends
		Reducer<Text, ProductCount, Text, ProductCount> {
	
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
		
		
		for(Entry<Text, IntWritable> entry : productsMap.entrySet()){
			
			p2 = entry.getKey();
			c2 = entry.getValue();
			
			context.write(p1, new ProductCount(p2, c2));
			
		}
	}
	
}
