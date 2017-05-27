package com.core;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StateWiseDataAnalyticsReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text state, Iterable<Text> amount, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("StateWiseDataAnalyticsReducer.reduce(): Starts");
		Text txtReducerOutputKey = new Text();
		Text txtReducerOutputValue = new Text();
		BigInteger sum = new BigInteger("0");
		Text totalSum;
		
		//Iterating through the values to find the sum of all
		for (Text oneAmount : amount) {
			BigInteger tempAmt = new BigInteger(oneAmount.toString());
			sum = sum.add(tempAmt);
		}
		System.out.println("State is: "+state.toString());
		System.out.println("Total Sum is: "+sum);
		
		//Setting the reducer output key value
		totalSum = new Text(sum.toString());
		txtReducerOutputKey.set(state);
		txtReducerOutputValue.set(totalSum);
		
		//Writing to context
		context.write(txtReducerOutputKey, txtReducerOutputValue);
		//Final Data from above write will be <state> <TotalAmount>
		
		System.out.println("StateWiseDataAnalyticsReducer.reduce(): Ends");
	}
	

}
