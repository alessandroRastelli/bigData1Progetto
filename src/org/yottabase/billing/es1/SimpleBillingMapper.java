package org.yottabase.billing.es1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SimpleBillingMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
		tokenizer.nextToken();
		
		Text product;
		while (tokenizer.hasMoreTokens()) {
			product = new Text(tokenizer.nextToken());
			context.write(product, ONE);
		}

	}

}