package com.ic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.*;

public class TaxiWritable implements Writable , WritableComparable<TaxiWritable> 
{	
	
	private IntWritable taxiID;
	private Text date;
	private DoubleWritable lat;
	private DoubleWritable lon;
	
	public TaxiWritable() {
		// TODO Auto-generated constructor stub
		this.taxiID = new IntWritable();
		this.date = new Text();
		this.lat = new DoubleWritable();
		this.lon = new DoubleWritable();
	}
	
	public TaxiWritable(int taxiID, String date, float lat, float lon){
		this.taxiID = new IntWritable(taxiID);
		this.date = new Text(date);
		this.lat = new DoubleWritable(lat);
		this.lon = new DoubleWritable(lon);
	}
	
	public void set(IntWritable taxiID, Text date, DoubleWritable lat, DoubleWritable lon)
	{
		this.taxiID = taxiID;
		this.date = date;
		this.lat = lat;
		this.lon = lon;
	}
		
	
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		
		taxiID.readFields(in);
		date.readFields(in);
		lat.readFields(in);
		lon.readFields(in);
	}
	
	 public static TaxiWritable read(DataInput in) throws IOException {
		 TaxiWritable taxiWritable = new TaxiWritable();
		 taxiWritable.readFields(in);
	     return taxiWritable;
	    }
	 
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		taxiID.write(out);
		date.write(out);
		lat.write(out);
		lon.write(out);
	}	  
	
	public IntWritable getTaxiID() {
		return taxiID;
	}


	public Text getDate() {
		return date;
	}


	public DoubleWritable getLat() {
		return lat;
	}


	public DoubleWritable getLon() {
		return lon;
	}

  
	public int compareTo(TaxiWritable other) {
		// TODO Auto-generated method stub
		int compareVal = this.taxiID.compareTo(other.taxiID);
		if(compareVal != 0){
			return compareVal;
		}
		return this.date.compareTo(other.date);
	}
	
	
	  @Override
	  public boolean equals(Object o) {
	    if (o instanceof TaxiWritable) {
	    	TaxiWritable tp = (TaxiWritable) o;
	      return taxiID.equals(tp.taxiID) && date.equals(tp.date)
	    		  &&lat.equals(tp.lat)&&lon.equals(tp.lon);
	    }
	    return false;
	  }
	  
	  @Override
	  public int hashCode() {
	    return taxiID.get() + lat.hashCode() * 163 + lat.hashCode();
	  }
	  
  
	  @Override
	public String toString() {
		// TODO Auto-generated method stub
		return taxiID+":"+date+":"+lat+":"+lon;
	}
	  
}
