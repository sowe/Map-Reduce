import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SecondarySortDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new SecondarySortDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		JobConf config = new JobConf(getConf(),SecondarySortDriver.class);
		
		config.setInputFormat(KeyValueTextInputFormat.class);
		config.setMapperClass(SecondarySortMapper.class);
		config.setReducerClass(SecondarySortReducer.class);
		
		config.setMapOutputKeyClass(Employee.class);
		config.setMapOutputValueClass(IntWritable.class);
		
		config.setOutputKeyClass(Text.class);
		config.setOutputValueClass(IntWritable.class);
		
		
		config.setPartitionerClass(SecondarySortPartitioner.class);
		
		FileInputFormat.addInputPath(config, new Path(args[0]));
		FileOutputFormat.setOutputPath(config, new Path(args[1]));
		
		JobClient.runJob(config);
		
		
		
		return 0;
	}
	
	

}
