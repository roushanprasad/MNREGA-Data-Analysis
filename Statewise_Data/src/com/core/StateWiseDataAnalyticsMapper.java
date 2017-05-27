package com.core;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

@SuppressWarnings("deprecation")
public class StateWiseDataAnalyticsMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static ArrayList<String> refDataArrayList = new ArrayList<String>();
	private BufferedReader brReader;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("StateWiseDataAnalyticsMapper.setup(): Starts");
		
		//Loading the reference data
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if(cacheFilesLocal != null && cacheFilesLocal.length>0){
			for (Path path : cacheFilesLocal) {
				loadRefData(path, context);
			}
		}
		System.out.println("StateWiseDataAnalyticsMapper.setup(): Ends");
	}

	//Counter to check for the total trades.
		enum MYCOUNTER {
			RECORD_COUNT, FILE_EXISTS, FO_RECORD
		}
		
	//Loading reference Data into Map
	//Not needed right now, however may need in future
	private void loadRefData(Path filePath, Mapper<LongWritable, Text, Text, Text>.Context context) {
		System.out.println("StateWiseDataAnalyticsMapper.loadRefData(): Starts");
		String strReadLine="";
		try{
			System.out.println("Loading Ref Data into Buffer");
			brReader = new BufferedReader(new FileReader(filePath.toString()));

			//Read each line, split and load to ArrayList
			int count = 0;
			while ((strReadLine = brReader.readLine())!=null) {
				refDataArrayList.add(strReadLine);
				context.getCounter(MYCOUNTER.FO_RECORD).increment(1);
				count++;
			}
			System.out.println("Loding FO Trades Completed");
			System.out.println("Number of FO Trades loaded: "+count);
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(brReader!=null){
				try {
					brReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("StateWiseDataAnalyticsMapper.loadRefData(): Ends");
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("StateWiseDataAnalyticsMapper.map(): Starts");
		Text txtMapOutputKey = new Text("");
		Text txtMapOutputValue = new Text("");
		String record = value.toString();
		String refColumn="reference";
		String state;
		String amount;
		
		//Processing the Records
		//0: reference, 1:OfficeName, 2: Pincode, 3:OfficeType, 4:DeliveryStatus, 5:DivisionName, 6:RegionName
		//7:CircleName, 8:TalukaName, 9:DistrictName, 10:StateName, 11:Work, 12:Amount, 13:Date, 14:BankName
		//15:BranchCode
		
		String[] recordArray = record.split("\t");
		
		//Adding Header to Mapper Output
		if (recordArray[0].equals(refColumn)) {
			System.out.println("Header Found, Skipping processing");
			return;
		}else{
		//Processing the records
			
			//Getting the state
			state = recordArray[10];
			System.out.println("State is: "+state);
			//Check to get the correct state
			state = getCorrectState(state);
			System.out.println("Correct state is: "+state);
			//Getting the amount
			amount = recordArray[12];
			System.out.println("Amount is: "+amount);
			
			//Setting the state as key and amount as value
			txtMapOutputKey.set(state);
			txtMapOutputValue.set(amount);
			
			//Writing to context
			context.write(txtMapOutputKey, txtMapOutputValue);
		}
		System.out.println("StateWiseDataAnalyticsMapper.map(): Ends");
		
	}

	private String getCorrectState(String state) {
		String tempState;
		String defaultState=state;
		
		switch (state) {
		case "Aizawl":
			tempState = "MIZORAM";
			break;
		case "Ajmer":
			tempState = "RAJASTHAN";
			break;
		case "Allahabad":
			tempState = "UTTAR PRADESH";
			break;
		case "Alwar":
			tempState = "RAJASTHAN";
			break;
		case "Angul":
			tempState = "ODISHA";
			break;
		case "Aurangabad":
			tempState = "MAHARASHTRA";
			break;
		case "Azamgarh":
			tempState = "UTTAR PRADESH";
			break;
		case "Baramulla":
			tempState = "JAMMU & KASHMIR";
			break;
		case "Cuddalore":
			tempState = "TAMIL NADU";
			break;
		case "Cuttack":
			tempState = "ODISHA";
			break;
		case "East Singhbhum":
			tempState = "JHARKHAND";
			break;
		case "Hisar":
			tempState = "HARYANA";
			break;
		case "Jaipur":
			tempState = "RAJASTHAN";
			break;
		case "Jhajjar":
			tempState = "HARYANA";
			break;
		case "Koraput":
			tempState = "ODISHA";
			break;
		case "Madhepura":
			tempState = "BIHAR";
			break;
		case "Mau":
			tempState = "UTTAR PRADESH";
			break;
		case "Muzaffarpur":
			tempState = "BIHAR";
			break;
		case "Nalgonda":
			tempState = "ANDHRA PRADESH";
			break;
		case "Prakasam":
			tempState = "ANDHRA PRADESH";
			break;
		case "Pudukkottai":
			tempState = "TAMIL NADU";
			break;
		case "Raigarh(MH)":
			tempState = "MAHARASHTRA";
			break;
		case "Rayagada":
			tempState = "ODISHA";
			break;
		case "Saharanpur":
			tempState = "UTTAR PRADESH";
			break;
		case "Sangli":
			tempState = "MAHARASHTRA";
			break;
		case "Seraikela-kharsawan":
			tempState = "JHARKHAND";
			break;
		case "Sonipat":
			tempState = "HARYANA";
			break;
		case "South Delhi":
			tempState = "DELHI";
			break;
		case "Tiruchirappalli":
			tempState = "TAMIL NADU";
			break;
		case "Vellore":
			tempState = "TAMIL NADU";
			break;
		case "West Delhi":
			tempState = "DELHI";
			break;
		case "Yavatmal":
			tempState = "MAHARASHTRA";
			break;

		default:
			tempState = defaultState;
			break;
		}
		return tempState;
	}
	
	

}
