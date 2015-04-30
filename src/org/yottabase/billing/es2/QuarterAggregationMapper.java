package org.yottabase.billing.es2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QuarterAggregationMapper extends
		Mapper<LongWritable, Text, Text, CountByMounth> {

	private static final String DATE_SEPARATOR = "-";

	private static final Integer ONE = new Integer(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		StringTokenizer tokenizer = new StringTokenizer(line, ",");
		String[] date = tokenizer.nextToken().split(DATE_SEPARATOR);
		Integer mounth = new Integer(date[1]);

		if (mounth <= 2) {
			Text product;
			while (tokenizer.hasMoreTokens()) {
				product = new Text(tokenizer.nextToken());
				context.write(product, new CountByMounth(mounth, ONE));
			}
		}

	}

}