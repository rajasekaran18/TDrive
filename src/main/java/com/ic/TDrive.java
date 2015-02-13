package com.ic;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TDrive 
{
    public static class TDriveMapper extends Mapper<Text, Text, Text, TaxiWritable>
    {
	 
	 protected void map(Text key, Text value, Context context) throws IOException, InterruptedException 
	 {
		StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",") ;
		
		String taxiID =  tokenizer.nextToken();
		String date = tokenizer.nextToken();
		String lat = tokenizer.nextToken();
		String lon = tokenizer.nextToken();
		
		
		TaxiWritable taxiWritable = new TaxiWritable(Integer.parseInt(taxiID), date, 
				new Float(lat), new Float(lon));
		
		context.write(new Text(taxiID), taxiWritable);
	  }
    }
    
    
}
