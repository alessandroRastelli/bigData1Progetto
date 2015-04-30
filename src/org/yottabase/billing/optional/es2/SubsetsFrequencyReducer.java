package org.yottabase.billing.optional.es2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SubsetsFrequencyReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int tot = 0;
		for (IntWritable count : values)
			tot += count.get();

		context.write(key, new IntWritable(tot));

	}

}
