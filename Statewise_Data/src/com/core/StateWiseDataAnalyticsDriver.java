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
public class StateWiseDataAnalyticsDriver extends Configured implements Tool {

	//This is the starting point of application
	public static void main(String[] args) {
		System.out.println("StateWiseDataAnalyticsDriver.main(): STarts");
		
		//Initializing tool runner configuration
		try {
			int exitCode = ToolRunner.run(new Configuration(), new StateWiseDataAnalyticsDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("StateWiseDataAnalyticsDriver.run(): Ends");
	}

	@Override
	public int run(String[] args) throws Exception {
		
		//Sanity Check
		if(args.length !=3){
			System.out.println("Three parameters are required: <refData> <inputFile> <outputFile>");
			return -1;
		}
		
		//Job Configurations
		Job job = Job.getInstance(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("MNREGA DATA ANALYTICS - State Wise Data");
		job.setJarByClass(StateWiseDataAnalyticsDriver.class);
		
		//Loading Reference Data into Distributed Cache
		String refData = args[0];
		DistributedCache.addCacheFile(new URI(refData), conf);
		
		//Setting File Input and Output Path
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//Setting the Reducer output key value class
		//This is required as default is LongWritable and Text
		//And we are passing as Text and Text as key value reducer output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Setting Mapper and Reducer Class
		job.setMapperClass(StateWiseDataAnalyticsMapper.class);
		job.setReducerClass(StateWiseDataAnalyticsReducer.class);
		
		//Wait for job completion by checking its completion status
		//on command line interface
		boolean success = job.waitForCompletion(true);
		return success?0:1;
	}

}
