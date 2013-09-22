import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class SecondarySortPartitioner implements Partitioner<Employee,IntWritable>
{

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPartition(Employee emp, IntWritable empID, int numOfReducer) {
		
		return (emp.getEmpName().hashCode() & Integer.MAX_VALUE ) % numOfReducer;
	}

	

}
