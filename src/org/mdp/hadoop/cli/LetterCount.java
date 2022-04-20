package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LetterCount {
  public static String SPLIT_REGEX = "[^\\p{L}']+";
  
  public static class LetterCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    
    private Text chars = new Text();
    
    public void map(Object param1Object, Text param1Text, Mapper<Object, Text, Text, IntWritable>.Context param1Context) throws IOException, InterruptedException {
      String str1 = param1Text.toString();
      String str2 = str1.split(" ")[0];
      ArrayList<Character> arrayList = new ArrayList();
      char[] arrayOfChar;
      int i;
      byte b;
      for (arrayOfChar = str2.toCharArray(), i = arrayOfChar.length, b = 0; b < i; ) {
        Character character = Character.valueOf(arrayOfChar[b]);
        if (Character.isAlphabetic(character.charValue()))
          if (!arrayList.contains(character)) {
            arrayList.add(character);
            this.chars.set(character.toString());
            param1Context.write(this.chars, this.one);
            param1Context.getCounter("Letters", "Non-unique-input").increment(1L);
          }  
        b++;
      } 
    }
  }
  
  public static class LetterCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable count;
    
    public LetterCountReducer() {
      this.count = new IntWritable(1);
    }
    
    public void reduce(Text param1Text, Iterable<IntWritable> param1Iterable, Reducer<Text, IntWritable, Text, IntWritable>.Context param1Context) throws IOException, InterruptedException {
      int i = 0;
      for (IntWritable intWritable : param1Iterable)
        i += intWritable.get(); 
      this.count.set(i);
      param1Context.write(param1Text, this.count);
      param1Context.getCounter("Letters", "Unique-output").increment(1L);
    }
  }
  
  public static void main(String[] paramArrayOfString) throws Exception {
    Configuration configuration = new Configuration();
    String[] arrayOfString = (new GenericOptionsParser(configuration, paramArrayOfString)).getRemainingArgs();
    if (arrayOfString.length != 2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    } 
    String str1 = arrayOfString[0];
    String str2 = arrayOfString[1];
    Job job = Job.getInstance(configuration, LetterCount.class.getName());
    job.setJarByClass(LetterCount.class);
    job.setMapperClass(LetterCountMapper.class);
    job.setCombinerClass(LetterCountReducer.class);
    job.setReducerClass(LetterCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(str1));
    FileOutputFormat.setOutputPath(job, new Path(str2));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
