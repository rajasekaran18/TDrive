package com.ic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TaxiWritable implements WritableComparable<TaxiWritable> 
{
	private IntWritable taxiID;
	private Text date;
	private FloatWritable lat;
	private FloatWritable lon;
	
	public TaxiWritable(int taxiID, String date, float lat, float lon){
		this.taxiID = new IntWritable(taxiID);
		this.date = new Text(date);
		this.lat = new FloatWritable(lat);
		this.lon = new FloatWritable(lon);
	}
	
		
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		taxiID.readFields(in);
		date.readFields(in);
		lat.readFields(in);
		lon.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		taxiID.write(out);
		date.write(out);
		lat.write(out);
		lon.write(out);
	}
	
	public int compareTo(TaxiWritable other) {
		// TODO Auto-generated method stub
		int compareVal = this.taxiID.compareTo(other.taxiID);
		if(compareVal != 0){
			return compareVal;
		}
		return this.date.compareTo(other.date);
	}
	

}
