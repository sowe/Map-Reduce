import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Employee implements WritableComparable<Employee>{
	private Text empName;
	private IntWritable empID;
	
	public Employee()
	{
		empName = new Text();
		empID = new IntWritable();
		
	}
	
	public Employee(String empName,int empID)
	{
		setEmpName(new Text(empName));
		setEmpID(new IntWritable(empID));
	}
	
	public Employee(Text empName,IntWritable empID)
	{
		setEmpName(empName);
		setEmpID(empID);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		empName.readFields(arg0);
		empID.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		empName.write(arg0);
		empID.write(arg0);
		
	}

	@Override
	public int compareTo(Employee o) {
		int cmp =  empName.compareTo(o.getEmpName());
		if(cmp != 0)
		{
			return cmp;
		}
		return empID.compareTo(o.getEmpID());
		
		
	}
	
	

	public Text getEmpName() {
		return empName;
	}

	public void setEmpName(Text empName) {
		this.empName = empName;
	}

	public IntWritable getEmpID() {
		return empID;
	}

	public void setEmpID(IntWritable empID) {
		this.empID = empID;
	}

}
