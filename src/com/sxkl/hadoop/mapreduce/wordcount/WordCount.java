package com.sxkl.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		Text k2 = new Text() ;
		IntWritable v2 = new IntWritable() ;
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			for (String word : words) {
				k2.set(word);
				v2.set(1);
				context.write(k2, v2);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			IntWritable v3 = new IntWritable();
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			v3.set(count);
			context.write(key, v3);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "wordCount");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inputPath = new Path("hdfs://192.168.1.104:9000/test/in/test.txt");
		FileInputFormat.addInputPath(job, inputPath);
		Path outputPath = new Path("hdfs://192.168.1.104:9000/test/out");
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1) ;
	}
}
