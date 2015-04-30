package org.yottabase.billing.optional.es1;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKProductPairReducer extends
		Reducer<NullWritable, ProductPairCount, NullWritable, ProductPairCount> {

	private final int K = 10;

	private Queue<ProductPairCount> top = new PriorityQueue<ProductPairCount>();

	@Override
	public void reduce(NullWritable key, Iterable<ProductPairCount> values,
			Context context) throws IOException, InterruptedException {
		
		for (ProductPairCount value : values) {
			
			try {
				top.add( (ProductPairCount) value.clone() );
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
			
			if (top.size() > K)
				top.poll();
		}
		
		for ( ProductPairCount ppc : invertQueueIntoList(top) )
			context.write(NullWritable.get(), ppc);
		
	}
	
	
	private List<ProductPairCount> invertQueueIntoList(Queue<ProductPairCount> queue) {
		List<ProductPairCount> list = new LinkedList<ProductPairCount>();
		
		while ( !top.isEmpty() ) {
			ProductPairCount ppc = top.remove();
			list.add(0, ppc);
		}
		return list;
	}

}
