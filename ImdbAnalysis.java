import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.lang.*;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ImdbAnalysis {
    // Class to implement the mapper interface
  static class TokenzierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text genre = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String year = null;
      String genre_list = null;

      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      // split the line based on delimiter ';'
      String line = value.toString();
      String[] attributes = line.split(";");
      
      String tempGenre = null;

      if(attributes.length >= 4) {  
        if (attributes[1].equals("movie") && checkIfInteger(attributes[3]) && Integer.parseInt(attributes[3]) >= 2000) {

          if (attributes[4] != null) {
            genre_list = attributes[4];
            year  = attributes[3].toString();
            
            StringTokenizer itr = new StringTokenizer(genre_list.toString(), ",");
            while (itr.hasMoreTokens()) {
              tempGenre =itr.nextToken();
              if(!tempGenre.equals("\\N")) {
                genre.set(year +  " , " + tempGenre + " , " );
                context.write(genre, one);
              }           
            }
          }
        }
      }
    }

    // check if the string being type cast to integer is convertable to Int or not
    public boolean checkIfInteger(String input) {
      try
      {
        Integer.parseInt(input);
        return true;
      } 
      catch (Exception e) {
        return false;
      }
    } 

  }
  static class Imdbreducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable val : values) {
       sum += val.get();
     }
     result.set(sum);
     context.write(key, result);
   }
 }
    // Main method
 public static void main(String[] args) throws Exception {

  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "Genre count");
  job.setJarByClass(ImdbAnalysis.class);
  job.setMapperClass(TokenzierMapper.class);
  job.setCombinerClass(Imdbreducer.class);
  job.setReducerClass(Imdbreducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
