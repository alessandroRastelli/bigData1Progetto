package org.yottabase.billing.es3;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
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
		StringTokenizer tokenizer = new StringTokenizer(line, ",\t");

		ProductPair pair = new ProductPair(new Text(tokenizer.nextToken()),
				new Text(tokenizer.nextToken()));
		IntWritable count = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
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
