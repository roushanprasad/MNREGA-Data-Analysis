package com.core;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WorkWiseDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		//Counter to check for the total number of records.
		enum MYCOUNTER {
			RECORD_COUNT
		}
		
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] recordArray = record.split("\t");
			String refColumn = "reference";
			Text mapOutputKey = new Text("");;
			Text mapOutputValue = new Text("");;
			String work;
			String amount;
			//Processing the Records
			//0: reference, 1:OfficeName, 2: Pincode, 3:OfficeType, 4:DeliveryStatus, 5:DivisionName, 6:RegionName
			//7:CircleName, 8:TalukaName, 9:DistrictName, 10:StateName, 11:Work, 12:Amount, 13:Date, 14:BankName
			//15:BranchCode
			
			
			//Adding Header to Mapper Output
			if (recordArray[0].equals(refColumn)) {
				System.out.println("Header Found, Skipping processing");
				return;
			}else{
				work = recordArray[11].toString();
				amount = recordArray[12].toString();
				
				System.out.println("Work is: "+work);
				System.out.println("Amount is: "+amount);
				
				mapOutputKey.set(work);
				mapOutputValue.set(amount);
				
				//Counting the number of records
				context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
				//Writing Mapper output to context
				context.write(mapOutputKey, mapOutputValue);
			}
			System.out.println("WorkWiseDataMapper.map(): Ends");
		}
}
