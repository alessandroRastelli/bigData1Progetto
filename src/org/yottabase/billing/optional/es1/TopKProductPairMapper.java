package org.yottabase.billing.optional.es1;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKProductPairMapper extends
		Mapper<LongWritable, Text, NullWritable, ProductPairCount> {

	private final int K = 10;

	private Queue<ProductPairCount> top = new PriorityQueue<ProductPairCount>();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, "\t");

		Text pair = new Text( tokenizer.nextToken() );
		FloatWritable count = new FloatWritable( Float.parseFloat(tokenizer.nextToken()) );
		
		ProductPairCount ppc = new ProductPairCount(pair, count);

		top.add(ppc);
		if (top.size() > K)
			top.poll();
		
	}
	

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, NullWritable, ProductPairCount>.Context context)
			throws IOException, InterruptedException {

		super.cleanup(context);
		
		ProductPairCount ppc = null;
		while ( !top.isEmpty() ) {
			ppc = top.remove();
			context.write(NullWritable.get(), ppc);
		}
	}

}
