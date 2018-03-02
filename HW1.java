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

import sun.tools.tree.LengthExpression;

public class HW1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  word.set(itr.nextToken());
    	  //String word1 = itr.nextToken();
    	  String word1 = word.toString();
    	  word1 = word1.replaceAll("[^a-zA-Z\\s]+", "").toLowerCase();
    	  if (word1.length() > 1){
    		  Text letter2 = new Text (Character.toString(word1.charAt(1)));
    		  context.write(letter2, one);       	  
    	  }
      }
    }
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

  public static class SortingCountSecondLetter
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  word.set(itr.nextToken());
    	  //String word1 = itr.nextToken();
    	  String word1 = word.toString();
    	  word1 = word1.replaceAll("[^a-zA-Z\\s]+", "").toLowerCase();
    	  if (word1.length() > 1){
    		  Text letter2 = new Text (Character.toString(word1.charAt(1)));
    		  context.write(letter2, one);       	  
    	  }
      }
    }
  public static void main(String[] args) throws Exception {
    
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "2 letter count HW1");
    job.setJarByClass(HW1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.waitForCompletion.(true);
    
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Sorting count of 2 letter");
    job2.setJarByClass(HW1.class);
    job2.setMapperClass(SortingCountSecondLetter.class);
    job2.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    job.waitForCompletion.(true);
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}