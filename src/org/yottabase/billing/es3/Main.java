package org.yottabase.billing.es3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static final String JOB_NAME = "es3_ProductPair";

	public static void main(String[] args) throws Exception {

		String inputJob = args[0];
		String outputJob = args[1];

		runJob(inputJob, outputJob);
	}

	public static void runJob(String inputPath, String outputPath)
			throws Exception {
		
		outputPath += "/" + JOB_NAME;
		
		String inputJob1 = inputPath;
		String outputJob1 = outputPath + "/job1";
		
		String inputJob2 = outputJob1;
		String outputJob2 = outputPath + "/job2";
		
		runJob1(inputJob1, outputJob1);
		runJob2(inputJob2, outputJob2);
	}
	
	public static void runJob1(String inputPath, String outputPath)
			throws Exception {
		
		long start_time = System.currentTimeMillis();
		
		Job job = new Job(new Configuration(), JOB_NAME + "job1");

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setJarByClass(Main.class);
		job.setMapperClass(ProductPairMapper.class);
		job.setCombinerClass(ProductPairReducer.class);
		job.setReducerClass(ProductPairReducer.class);

		job.setOutputKeyClass(ProductPair.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
		System.out.println("TEMPO " + JOB_NAME + ":job1-> " + (System.currentTimeMillis() - start_time) );
	}
	
	public static void runJob2(String inputPath, String outputPath)
			throws Exception {
		
		long start_time = System.currentTimeMillis();
		
		Job job = new Job(new Configuration(), JOB_NAME + "job2");

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setJarByClass(Main.class);
		job.setMapperClass(TopKProductPairMapper.class);
		job.setReducerClass(TopKProductPairReducer.class);

		
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(ProductPairCount.class);
		job.waitForCompletion(true);
		
		System.out.println("TEMPO " + JOB_NAME + ":job2-> " + (System.currentTimeMillis() - start_time) );
	}
}
