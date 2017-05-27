package com.core;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BankWiseDataMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	//Counter to count the total number of records
	enum MY_COUNTER{
		RECORD_COUNT
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("BankWiseDataMapper.map(): Starts");
		String record;
		String[] recordArray;
		String bank="";
		String amount="";
		Text mapOutputKey = new Text("");
		Text mapOutputValue = new Text("");
		Text outputKey;
		Text outputValue;
		String refColumn = "reference";
		
		//Processing the Records
		//0: reference, 1:OfficeName, 2: Pincode, 3:OfficeType, 4:DeliveryStatus, 5:DivisionName, 6:RegionName
		//7:CircleName, 8:TalukaName, 9:DistrictName, 10:StateName, 11:Work, 12:Amount, 13:Date, 14:BankName
		//15:BranchCode
		
		record = value.toString();
		recordArray = record.split("\t");
		//Adding Header to Mapper Output
		if (recordArray[0].equals(refColumn)) {
			System.out.println("Header Found, Skipping processing");
			return;
		}else{
			
			bank = recordArray[14];
			amount = recordArray[12];
			//Setting output key value
			outputKey = new Text(bank);
			outputValue = new Text(amount);
			mapOutputKey.set(outputKey);
			mapOutputValue.set(outputValue);
			
			//Counting the number of records
			context.getCounter(MY_COUNTER.RECORD_COUNT).increment(1);
			//Writing Key VAlue to context
			context.write(mapOutputKey, mapOutputValue);
		}
		System.out.println("BankWiseDataMapper.map(): Ends");
	}
	
	

}
