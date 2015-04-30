package org.yottabase.billing.optional.es2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static final String JOB_NAME = "opt2_SubsetsFrequency";

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
		
		runJob1(inputJob1, outputJob1);
	}
	
	public static void runJob1(String inputPath, String outputPath)
			throws Exception {
		
		long start_time = System.currentTimeMillis();
		
		Job job = new Job(new Configuration(), JOB_NAME + "job1");

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setJarByClass(Main.class);
		job.setMapperClass(SubsetsFrequencyMapper.class);
		job.setCombinerClass(SubsetsFrequencyReducer.class);
		job.setReducerClass(SubsetsFrequencyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
		System.out.println("TEMPO " + JOB_NAME + ":job1-> " + (System.currentTimeMillis() - start_time) );
	}
	
}
