import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text,Text>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String inputString = value.toString();
		String[] splits = inputString.split("\\W+");
		
		/**
		 * Getting the file name
		 */
		FileSplit split = (FileSplit) context.getInputSplit();
		Path inputPath = split.getPath();
		
		for(String words : splits)
		{
			if(words.length() > 0)
			{
				context.write(new Text(words),new Text(inputPath.toString()));
			}
		}
	}

}
