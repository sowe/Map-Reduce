import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SecondarySortMapper extends MapReduceBase implements Mapper<Text,Text,Employee,IntWritable> {

	Employee emp = new Employee();
	@Override
	public void map(Text arg0, Text arg1,
			OutputCollector<Employee, IntWritable> output, Reporter arg3)
			throws IOException {
		
		emp.setEmpName(arg0);
		IntWritable id = new IntWritable(Integer.parseInt(arg1.toString()));
		emp.setEmpID(id);
		
		output.collect(emp, id);
		
		
	}

}
