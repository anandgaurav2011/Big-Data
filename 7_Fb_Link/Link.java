import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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

public class Link
{
  public static class LinkMapper extends
  Mapper <Object, /*Input Key Type */
  Text, /*Input value Type*/
  Text, /*Output key Type*/
  IntWritable> /*Output value Type*/
  {
    //Map function
    boolean flag = false;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    { 
      String line = value.toString();
      if(flag) {
        StringTokenizer s = new StringTokenizer(line, ",");
        String id = s.nextToken();  // id
        String type = s.nextToken();  // type
        if(type.equals("link")) {
          String date = s.nextToken();  // retrieved date token
          int add =0;
          while(s.hasMoreTokens()) {
            int num = Integer.parseInt(s.nextToken());
            add += num;
           }    
            context.write(new Text(date), new IntWritable(add));
         }
      }
       flag=true;
     }
  } 

  //Main function
  public static void main(String args[]) throws Exception
  {
    // create the object of  Job configuration class
    Configuration conf = new Configuration();

    Job job = new Job(conf, "Link Count");

    job.setOutputKeyClass(Text.class);
    
    job.setOutputValueClass(IntWritable.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    
    job.setOutputFormatClass(TextOutputFormat.class);
  
    job.setMapperClass(LinkMapper.class);
    job.setNumReduceTasks(0);  //Recucers = 0

    // Set the input files paths
    FileInputFormat.addInputPath(job, new Path(args[0]));
    
    // Set the output file path from 1st argument
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}







