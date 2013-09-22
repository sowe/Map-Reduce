import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class SecondarySortReducer extends MapReduceBase 
		implements Reducer<Employee,IntWritable, Text,IntWritable> {

	@Override
	public void reduce(Employee emp, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		
		Text empName = emp.getEmpName();
		
		while(values.hasNext())
		{
			output.collect(empName, values.next());
		}
		
	}

	
		
	

}
