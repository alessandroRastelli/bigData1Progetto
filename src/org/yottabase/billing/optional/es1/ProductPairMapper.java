package org.yottabase.billing.optional.es1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductPairMapper extends
		Mapper<LongWritable, Text, Text, ProductCount> {

	private static final IntWritable ONE = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		List<Text> products = new ArrayList<Text>();

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");

		tokenizer.nextToken(); // salta data

		while (tokenizer.hasMoreTokens()) {
			products.add(new Text(tokenizer.nextToken()));
		}

		for(Text p1 : products){
			
			for(Text p2 : products){
				
				context.write(p1, new ProductCount(p2, ONE));
				
			}
		}
	}
}