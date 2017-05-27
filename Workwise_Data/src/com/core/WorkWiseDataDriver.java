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
public class WorkWiseDataDriver extends Configured implements Tool{

	public static void main(String[] args) {
		System.out.println("WorkWiseDataDriver.main():Starts");
		
		//Initializing tool runner configuration
		try {
			int exitCode = ToolRunner.run(new Configuration(), new WorkWiseDataDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("WorkWiseDataDriver.main(): Ends");
	}

	@Override
	public int run(String[] args) throws Exception {
		//Sanity check
		if(args.length !=3){
			System.out.println("Three parameters are required: <refData> <inputFile> <outputFile>");
			return -1;
		}
		
		//Setting Job Configurations
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		job.setJobName("Work Wise Data Analysis");
		job.setJarByClass(WorkWiseDataDriver.class);
		
		//Adding Reference Data Path
		String refDataPath = args[0];
		DistributedCache.addCacheFile(new URI(refDataPath), conf);
		//Adding Input And Output Path
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		
		//Setting the Reducer output key value class
		//This is required as default is LongWritable and Text
		//And we are passing as Text and Text as key value reducer output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Setting Mapper and Reducer Class
		job.setMapperClass(WorkWiseDataMapper.class);
		job.setReducerClass(WorkWiseDataReducer.class);
				
		//Wait for job completion by checking its completion status
		//on command line interface
		boolean success = job.waitForCompletion(true);
		return success?0:1;
	}
}