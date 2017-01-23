import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
public class Program3 {

  public static class TokenMap
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text text = new Text();

    private Set<String> patternWords = new HashSet<String>();

    private Configuration config;
    private BufferedReader br;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      config = context.getConfiguration();
      if (config.getBoolean("wordcount.match.patterns", true)) {
        URI[] patternsURIs = Job.getInstance(config).getCacheFile();
        for (URI patternsURI : patternsURIs) {
          Path path1 = new Path(patternsURI.getPath());
          String patternsFileName = path1.getName().toString();
          parseFile(patternsFileName);
        }
      }
    }

    private void parseFile(String fileName) {
      try {
        br = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = br.readLine()) != null) {
            String words[] = pattern.split(" ");
            for(String word: words)
                patternWords.add(word);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
          String w = itr.nextToken();
          if(patternWords.contains(w)){
            text.set(w);
            context.write(text, one);
          }
      }}
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    Job job = Job.getInstance(config, "word count");
    job.setJarByClass(Program3.class);
    job.setMapperClass(TokenMap.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.addCacheFile(new Path(args[2]).toUri());
    job.getConfiguration().setBoolean("wordcount.match.patterns", true);
     
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}