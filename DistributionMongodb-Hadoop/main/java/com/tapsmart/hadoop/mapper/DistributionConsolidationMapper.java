package com.tapsmart.hadoop.mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.io.BSONWritable;
import com.tapsmart.hadoop.DistributionConstants;


public class DistributionConsolidationMapper extends Mapper<ObjectId, BSONObject, BSONWritable, BSONWritable> {

	@Override
    public void map(ObjectId key,BSONObject value, Context context)
            throws IOException, InterruptedException {

	    BSONObject mKey = new BasicBSONObject();
	    Object active = ((BSONObject)value).get(DistributionConstants.ACTIVE);
	    Object creative = ((BSONObject)value).get(DistributionConstants.CREATIVE);
	    Object t = ((BSONObject)value).get(DistributionConstants.TIME);
	    Object sched = (BSONObject)value.get(DistributionConstants.SCHEDULE);
	    Object dist = ((BSONObject)value).get(DistributionConstants.DIST);
	    Object mp = ((BSONObject)value).get(DistributionConstants.MP);
	    
	    if (t != null) {
		    Object start = ((BSONObject)t).get(DistributionConstants.TIME_START);
		    Object end = ((BSONObject)t).get(DistributionConstants.TIME_END);
		    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));  		      
		    //cal.set(Calendar.HOUR_OF_DAY, 0);  
		    //cal.set(Calendar.MINUTE, 0);  
		    //cal.set(Calendar.SECOND, 0);  
		    //cal.set(Calendar.MILLISECOND, 0);
		    Date now = cal.getTime();
		    if (active != null && (Boolean)active && // Check active distributions
		    	creative != null && !((BasicBSONList)creative).isEmpty() &&
		    	sched != null &&
		    	start != null && now.after((Date)start) &&
		    	end != null && now.before((Date)end)){ // Check actual distributions
		    	// Now check for schedule logic
		    	boolean scheduling = false;
		    	if (((BasicBSONList)sched).isEmpty()) {
		    		scheduling = true;
		    	} else {
			    	cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));  		      
				    cal.set(Calendar.MINUTE, 0);  
				    cal.set(Calendar.SECOND, 0);  
				    cal.set(Calendar.MILLISECOND, 0);   
				    int dayNow = cal.get(Calendar.DAY_OF_WEEK) - 2;
				    int hourNow = cal.get(Calendar.HOUR_OF_DAY);
				    for ( Object schedule : (BasicBSONList)value.get(DistributionConstants.SCHEDULE)){
				    	Integer scheduleDay = (Integer) ((BSONObject)schedule).get(DistributionConstants.DAY);
				    	Boolean scheduleActive = (Boolean) ((BSONObject)schedule).get(DistributionConstants.ACTIVE);
				    	if (dayNow == scheduleDay && scheduleActive){
				    		Object hours = ((BSONObject)schedule).get(DistributionConstants.HOURS);
				    		if (hours == null || ((BasicBSONList)hours).isEmpty()){
				    			scheduling = true;
				    		} else {
					    		for ( Object hour : (BasicBSONList)hours){
					    			Integer scheduleStart = (Integer) ((BSONObject)hour).get(DistributionConstants.HOURS_START);
					    			Integer scheduleStop = (Integer) ((BSONObject)hour).get(DistributionConstants.HOURS_END);
					    			if (scheduleStop == 0){
					    				scheduleStop = 24;
					    			}
					    			if ((scheduleStop-scheduleStart) > (hourNow-scheduleStart)){
					    				scheduling = true;
					    				break;
					    			}
					    		}
				    		}
				    		break;
				    	}
				    }
		    	}
			    if (scheduling){ // <----- //
			    	Double e,s = 0.0;
			    	Long disTotal = ((Number)((BSONObject)dist).get(DistributionConstants.TOTAL_DIST)).longValue();
			    	Long sumDone = 0l;
			    	String mpType = (String) ((BSONObject)mp).get(DistributionConstants.TYPE);
			    	
			    	if (!("CPM").equalsIgnoreCase(mpType)) {
			    		disTotal *= 200; // ctr 0.05
			    	}
			    	
			    	for ( Object placement : (BasicBSONList)value.get(DistributionConstants.PLACEMENT) ){
			    		//Sum all values about s
			    		BSONObject done = (BSONObject) ((BSONObject)placement).get(DistributionConstants.DONE);
			    		sumDone += ((Number) done.get(DistributionConstants.SHOWS)).longValue();
			    	}
			    	
			    	
			    	boolean sponsorship = false;
			    	if (("Sponsorship").equalsIgnoreCase(mpType) || ("Impressions Sponsorship").equalsIgnoreCase(mpType)) {
			    		sponsorship = true;
			    	}
			    	if (sponsorship || (disTotal - sumDone) > 0) {
				    	for ( Object placement : (BasicBSONList)value.get(DistributionConstants.PLACEMENT) ){
				    		BSONObject total = (BSONObject) ((BSONObject)placement).get(DistributionConstants.TOTAL);
				    		if (sponsorship){
					    		total.put(DistributionConstants.SHOWS, 1);
					    		total.put(DistributionConstants.EVENT, 1);
				    		} else {
				    			s = ((Number) total.get(DistributionConstants.SHOWS)).doubleValue();
				    			s = ((disTotal - sumDone) * (s/disTotal));
				    			if (!("CPM").equalsIgnoreCase(mpType)) {
				    				e = s / 200;
					    		} else {
						    		e = s;
					    		}
				    			total.put(DistributionConstants.SHOWS, s);
				    			total.put(DistributionConstants.EVENT, e);
				    		}
				    	}
				    	
					    for ( Object placement : (BasicBSONList)value.get(DistributionConstants.PLACEMENT) ){
					    	BSONObject total = (BSONObject) ((BSONObject)placement).get(DistributionConstants.TOTAL);
					    	if (((Number)total.get(DistributionConstants.SHOWS)).longValue() > 0){ // Check total shows
					    		// Check for smooth distribution
					    		Object smooth = ((BSONObject)value).get(DistributionConstants.SMOOTH);
					    		if (smooth != null && (Boolean)smooth){ //s
					    	        GregorianCalendar date1 = new GregorianCalendar();
					    	        date1.setTime((Date)start);
					    	        GregorianCalendar date2 = new GregorianCalendar();
					    	        date2.setTime((Date)end);
					    	        int days = 0;
					    	        if (date1.get(Calendar.YEAR) == date2.get(Calendar.YEAR)) {
					    	            days = date2.get(Calendar.DAY_OF_YEAR) - date1.get(Calendar.DAY_OF_YEAR);
					    	        } else {
					    	            int yearDays = date1.isLeapYear(date1.get(Calendar.YEAR)) ? 366 : 365;
					    	            days = yearDays + (date2.get(Calendar.DAY_OF_YEAR) - date1.get(Calendar.DAY_OF_YEAR));
					    	        }
					    	        Number totalDay = ((Number)total.get(DistributionConstants.SHOWS)).longValue() / days;
					    	        total.put(DistributionConstants.SHOWS, totalDay);// S
					    		} // smooth
					    		
					    		
						    	mKey.put(DistributionConstants.PLACEMENT,placement);
						    	BSONObject mValue = new BasicBSONObject();
						    	mValue.putAll(value);
						    	mValue.removeField(DistributionConstants.PLACEMENT);
						    	mValue.removeField(DistributionConstants.ID);
						    	// Now split the targets
						    	if (value.get(DistributionConstants.TARGET) != null){
						    		mValue.removeField(DistributionConstants.TARGET);
							    	for ( Object target : (BasicBSONList)value.get(DistributionConstants.TARGET) ){
							    		mKey.put(DistributionConstants.TARGET,target);
							    		context.write(new BSONWritable(mKey), new BSONWritable(mValue));
							    	}
						    	} else {
						    		context.write(new BSONWritable(mKey), new BSONWritable(mValue));
						    	}
					    	} 
				        }// fin del placement
			    	}
			    }
		    }
	    }
    }
}
