package com.ic;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TDrive extends Configured implements Tool 
{
    public static class TDriveMapper extends Mapper<LongWritable, Text, Text, TaxiWritable>
    {
	 
    	private Text mapOutputKey = new Text();
    	private TaxiWritable mapOutputValue= new TaxiWritable();
    	
      @Override
	  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	  {	  
	    	
		StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",") ;
		
		String taxiID =  tokenizer.nextToken();
		String date = tokenizer.nextToken().split("\\s+")[0];
		String lat = tokenizer.nextToken();
		String lon = tokenizer.nextToken();
		

		  mapOutputKey.set(taxiID+":"+date);
		  mapOutputValue.set(new IntWritable(Integer.parseInt(taxiID)), new Text(date), new DoubleWritable(new Float(lat)), new DoubleWritable(new Float(lon)));
		 
		  context.write(mapOutputKey, mapOutputValue);	  
		 
	   } 	
    }
    
    
    public static class TDriveReducer extends Reducer<Text, TaxiWritable, Text, Text>
    {
    	private Text reduceOutputVal = new Text();
    	
    	@Override
    	protected void reduce(Text key, Iterable<TaxiWritable> val, Context context) throws IOException, InterruptedException
    	{
    		Iterator<TaxiWritable> iterator = val.iterator();
    		
    		double dist = 0.0;
    		TaxiWritable point1 = (TaxiWritable)iterator.next();
    		
    		while(iterator.hasNext())
    		{
    			
    			TaxiWritable point2 = (TaxiWritable)iterator.next();
    			double twoPointDistance = distance(point1.getLat().get(), point1.getLon().get(), point2.getLat().get(), point2.getLon().get());
    			if(twoPointDistance > 0)
    			{
    			  dist = dist + twoPointDistance;    			 
    			}
    			else
    			{
    				if(iterator.hasNext())
    				{
    				 point2 = (TaxiWritable)iterator.next();
    				}
    			} 
    			
    			point1 = point2;
    			
    		}
    		reduceOutputVal.set(Double.toString(dist));
    		context.write(key, reduceOutputVal );			
    	}
    	
    } 
  
    private static double distance(double lat1, double lon1, double lat2, double lon2)
    {
    	try
    	{
    		
    	  double theta = lon1 - lon2;
    	  double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
    	  dist = Math.acos(dist);
    	  dist = rad2deg(dist);
    	  dist = dist * 60 * 1.1515;
    	  return (dist);
    	}
    	catch(Exception ex)
    	{
    		return 0;
    	}  	 
    	 
     }
    
    private static double deg2rad(double deg)
    {
    	  return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad)
    {
    	  return (rad * 180 / Math.PI);
    }

    public static boolean deleteFile(String filepath, Configuration conf) throws IOException
	{
		
		Path output2 = new Path(filepath);
 		FileSystem fs2 = FileSystem.get(conf);
		return fs2.delete(output2, true);		
	}
 
    public int run(String[] args) throws Exception
    {
    	//delete ouput file to rerun the job more than once
    	deleteFile(args[1],  getConf());
    	
		Configuration conf =new Configuration();
		Job job = new Job(conf, "TDrive");
		job.setJarByClass(TDriveMapper.class);
		
		job.setInputFormatClass(TextInputFormat.class); 
		TextInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(TDriveMapper.class);
		job.setReducerClass(TDriveReducer.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TaxiWritable.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean res = job.waitForCompletion(true);
		
		if(res)
			return 0;
		else
			return -1; 	
	}
    
	public static void main(String[] args) throws Exception 
	{
		
		int res = ToolRunner.run(new TDrive(), args);
		System.exit(res);		
	}
    
}
