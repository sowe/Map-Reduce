package com.tapsmart.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.mongodb.hadoop.io.BSONUpdateable;
import com.mongodb.hadoop.io.BSONWritable;
import com.tapsmart.hadoop.DistributionConstants;


public class DistributionConsolidationReducer extends Reducer<BSONWritable, BSONWritable, BSONUpdateable, BSONWritable> {
	
	@Override
	public void reduce(final BSONWritable key, final Iterable<BSONWritable> values, final Context context)
	    throws IOException, InterruptedException {
		BSONUpdateable oKey = new BSONUpdateable();
		BSONWritable oValue = new BSONWritable();
		oKey.put(DistributionConstants.PLACEMENT, key.get(DistributionConstants.PLACEMENT));
		oKey.put(DistributionConstants.TARGET, key.get(DistributionConstants.TARGET));
		oValue.put(DistributionConstants.PLACEMENT, key.get(DistributionConstants.PLACEMENT));
		oValue.put(DistributionConstants.TARGET, key.get(DistributionConstants.TARGET));
		oValue.putAll(values.iterator().next());
	    context.write(oKey, oValue);
	}
}
