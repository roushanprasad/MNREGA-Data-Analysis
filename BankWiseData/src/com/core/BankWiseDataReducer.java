package com.core;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BankWiseDataReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text bank, Iterable<Text> amount, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("BankWiseDataReducer.reduce(): Start");
		
		Text reducerOutputKey = new Text("");
		Text reducerOutputValue = new Text("");
		Text outputKey;
		Text outputValue;
		
		BigInteger totalAmount = new BigInteger("0");
		System.out.println("Bank is: "+bank.toString());
		
		//Iterating all the amount and adding them
		for (Text amt : amount) {
			System.out.println("Amount is: "+amt);
			BigInteger tempAmt = new BigInteger(amt.toString());
			totalAmount = totalAmount.add(tempAmt);
		}
		System.out.println("Total Amount is: "+totalAmount.toString());
		
		outputKey = new Text(bank);
		outputValue = new Text(totalAmount.toString());
		
		//Setting output key and value
		reducerOutputKey.set(outputKey);
		reducerOutputValue.set(outputValue);
		//Writing data to context
		context.write(reducerOutputKey, reducerOutputValue);
		System.out.println("BankWiseDataReducer.reduce(): ENds");
	}
	

}
