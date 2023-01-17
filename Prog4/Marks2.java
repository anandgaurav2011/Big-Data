import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;

public class Marks2 {
	
	// Mapper class -> output -> string, int
	
	public static class Marks2Mapper extends Mapper <Object, Text, Text, IntWritable> 
	{
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line,",");
			String name  = st.nextToken();
			int marks = Integer.parseInt(st.nextToken());
			String cls = st.nextToken();
			if(cls.equals("TY"))
				context.write(new Text("Marks"), new IntWritable(marks));
			
		}
	}
	
	public static class Marks2Reducer extends Reducer <Text, IntWritable, Text, FloatWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		
		
		{
			FloatWritable avg = new FloatWritable();
			int sum = 0;
			int total = 0;
			for (IntWritable num : values)
			{
				sum = sum+num.get();
				total++;
			}
			avg.set((float)sum/total);
			context.write(new Text("Average is: "), avg);
		}
	
	
	}

public static void main(String args[]) throws Exception {

  // Create the object of Configuration class
  Configuration conf = new Configuration();
 
  // Create the object of the job class
  Job job = new Job(conf, "Marks2");
 
  // Set the data type of output key
  job.setMapOutputKeyClass(Text.class);
 
  // Set the data type of output valye
  job.setMapOutputValueClass(IntWritable.class);
 
  // Set the data type of output key
  job.setOutputKeyClass(Text.class);
 
  // Set the data type of output valye
  job.setOutputValueClass(FloatWritable.class);
 
  // Set the data format of output
  job.setOutputFormatClass(TextOutputFormat.class);
  
  // Set the data format of input
  job.setInputFormatClass(TextInputFormat.class);
  
  // Set the name of Mapper class
  job.setMapperClass(Marks2Mapper.class);
  
  // Set the name of Reducer class
  job.setReducerClass(Marks2Reducer.class);
  
  // Set the output files path from 0th argument
  FileInputFormat.addInputPath(job, new Path(args[0]));
  
  // Set the output files path from 1st argument
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
  // Execute the job and wait for the completion
  job.waitForCompletion(true);
  
}
}
