import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class SecondarySortTesting {
	MapDriver<Text, Text, Employee, IntWritable> mapDriver;
	ReduceDriver<Employee,IntWritable, Text, IntWritable> reduceDriver;
	
	@Before
	public void setUp()
	{
		SecondarySortMapper mapper = new SecondarySortMapper();
		mapDriver = new MapDriver<Text, Text, Employee, IntWritable>();
		mapDriver.setMapper(mapper);
	}
	
	@Test
	public void testMapper()
	{
		mapDriver.withInput(new Text("som"),new Text("123"));
		Employee emp = new Employee();
		
		Text name = new Text("som");
		IntWritable id  = new IntWritable(123);
		
		emp.setEmpName(name);
		emp.setEmpID(id);
		
		mapDriver.withOutput(emp, new IntWritable(123));
		mapDriver.runTest();
	}

}
