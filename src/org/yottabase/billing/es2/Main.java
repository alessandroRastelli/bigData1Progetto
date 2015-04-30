package org.yottabase.billing.es2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	
	public static final String JOB_NAME = "es2_QuarterAggregation";

	public static void main(String[] args) throws Exception {
		
		String inputJob = args[0];
		String outputJob = args[1];
		
		runJob(inputJob, outputJob);
	}
	
	public static void runJob(String inputPath, String outputPath) throws Exception{
		
		outputPath += "/" + JOB_NAME;
		
		String inputJob1 = inputPath;
		String outputJob1 = outputPath;
		
		runJob1(inputJob1, outputJob1);
		
	}
	
	private static void runJob1(String inputPath, String outputPath) throws Exception{
		
		long start_time = System.currentTimeMillis();
		
		Job job = new Job(new Configuration(), JOB_NAME);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setJarByClass(Main.class);
		job.setMapperClass(QuarterAggregationMapper.class);
		job.setCombinerClass(QuarterAggregationCombiner.class);
		job.setReducerClass(QuarterAggregationReducer.class);
		
		// Configuration
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CountByMounth.class);
		job.waitForCompletion(true);
		
		System.out.println("TEMPO " + JOB_NAME + ":job-> " + (System.currentTimeMillis() - start_time) );
		
	}
	
	
	
}