package com.core;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class BankWiseDataDriver extends Configured implements Tool{

	public static void main(String[] args) {
		System.out.println("BankWiseDataDriver.main(): Starts");
		
		try {
			int exitCode = ToolRunner.run(new Configuration(), new BankWiseDataDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("BankWiseDataDriver.main(): ENds");

	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println("BankWiseDataDriver.run(): Starts");
		
		if(args.length != 3){
			System.out.println("Three parameters are required: <refData> <inputFile> <outputFile>");
			return -1;
		}
		
		//Setting Job Configuration
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		job.setJobName("Bank Wise Data Analysis Job");
		job.setJarByClass(BankWiseDataDriver.class);
		
		//Adding Reference Data Path
		String refDataPath = args[0];
		DistributedCache.addCacheFile(new URI(refDataPath), conf);
		//Adding Input and output path
		FileInputFormat.setInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//Setting the Reducer output key value class
		//This is required as default is LongWritable and Text
		//And we are passing as Text and Text as key value reducer output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Setting Mapper and Reducer Class
		job.setMapperClass(BankWiseDataMapper.class);
		job.setReducerClass(BankWiseDataReducer.class);
		
		//Wait for job completion by checking its completion status
		//on command line interface
		boolean success = job.waitForCompletion(true);
		System.out.println("BankWiseDataDriver.run(): Ends");
		return success?0:1;
	}

}
