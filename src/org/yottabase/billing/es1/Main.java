package org.yottabase.billing.es1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	
	public static final String JOB_NAME = "es1_SimpleBilling";

	public static void main(String[] args) throws Exception {
		
		String inputJob = args[0];
		String outputJob = args[1];
		
		runJob(inputJob, outputJob);
	}
	
	
	public static void runJob(String inputPath, String outputPath) throws Exception{
		
		long start_time = System.currentTimeMillis();
		
		outputPath += "/" + JOB_NAME;
		
		Job job = new Job(new Configuration(), JOB_NAME);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setJarByClass(Main.class);
		job.setMapperClass(SimpleBillingMapper.class);
		job.setCombinerClass(SimpleBillingCombiner.class);
		job.setReducerClass(SimpleBillingReducer.class);
		
		/*
		 *  soluzione necessaria per produrre un solo file di output con ordinamento globale
		 *  la soluzione scala sugli scontrini, ma non sul numero di prodotti
		 */
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
		System.out.println("TEMPO " + JOB_NAME + ":job1-> " + (System.currentTimeMillis() - start_time) );
	}
}