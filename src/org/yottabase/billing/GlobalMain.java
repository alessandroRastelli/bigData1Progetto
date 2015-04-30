package org.yottabase.billing;


public class GlobalMain {

	public static void main(String[] args) throws Exception {
		
		String inputPath, outputPath;
		
		if(args.length == 2){
			inputPath = args[0];
			outputPath = args[1];	
		}else{
			inputPath = "data/generator/sample/esempio.txt";
			outputPath = "data/output/mapred";
		}
		
		//FileSystem.deleteDirectory(new File(outputPath));
		
		org.yottabase.billing.es1.Main.runJob(inputPath, outputPath);
		
		org.yottabase.billing.es2.Main.runJob(inputPath, outputPath);
		
		org.yottabase.billing.es3.Main.runJob(inputPath, outputPath);
		
		org.yottabase.billing.optional.es1.Main.runJob(inputPath, outputPath);
		
		org.yottabase.billing.optional.es2.Main.runJob(inputPath, outputPath);
	}

}
