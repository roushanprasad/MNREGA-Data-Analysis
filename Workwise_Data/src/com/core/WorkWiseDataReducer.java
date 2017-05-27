package com.core;

import java.io.IOException;
import java.math.BigInteger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WorkWiseDataReducer extends Reducer<Text, Text, Text, Text> {
	
	protected void reduce(Text work, Iterable<Text> amount, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("WorkWiseDataReducer.reduce(): Starts");
		Text reducerOutputKey = new Text();
		Text reducerOutputValue = new Text();
		BigInteger sum = new BigInteger("0");
		Text totalSum;
		
		System.out.println("Work is: "+work.toString());
		//Iterating though all the values of amount for a particular works
		for (Text tempAmount : amount) {
			BigInteger tempAmt = new BigInteger(tempAmount.toString());
			sum = sum.add(tempAmt);
		}
		
		totalSum = new Text(sum.toString());
		System.out.println("Total Amount is: "+totalSum);
		
		//Setting the output key and value
		reducerOutputKey.set(work);
		reducerOutputValue.set(totalSum);
		
		//Writing to context
		//Final Data from above write will be <state> <TotalAmount>
		context.write(reducerOutputKey, reducerOutputValue);
		System.out.println("WorkWiseDataReducer.reducer(): Ends");
	}

}
