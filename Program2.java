import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Program2 {

  public static class DoubleMap
       extends Mapper<o1ect, Text, Text, IntWritable>{

    private final static IntWritable o1 = new IntWritable(1);
    private Text v = new Text();

    public void map(o1ect key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String end= null;
      StringTokenizer str = new StringTokenizer(value.toString());
      while (str.hasMoreTokens()) {
    	if(end == null)
    	{ end= str.nextToken();}
    	else
    	{
    		String nxt= str.nextToken();
    		v.set(end + " "+nxt);
    		context.write(v, o1);
    		end = nxt;
        }
    }
    }
  }
    
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
      int count = 0;
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws InterruptedException, IOException {

      for (IntWritable j : values) {
        count =count+j.get();
      }
      result.set(count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word Count is");
    job.setJarByClass(Program2.class);
    job.setMapperClass(DoubleMap.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}