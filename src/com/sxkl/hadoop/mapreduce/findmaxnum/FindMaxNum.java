package com.sxkl.hadoop.mapreduce.findmaxnum;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FindMaxNum extends Configured implements Tool{

	public static class MyMapper extends Mapper<Object, Text, LongWritable, NullWritable> {

		long max = Long.MIN_VALUE;
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer token = new StringTokenizer(value.toString(), " ");
			while(token.hasMoreTokens()){
				long num1 = Long.parseLong(token.nextToken());
				long num2 = Long.parseLong(token.nextToken());
				max = Long.max(max, Long.max(num1, num2));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "find max num");
		job.setJarByClass(FindMaxNum.class);
		
		job.setMapperClass(MyMapper.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		Path inputPath = new Path("hdfs://192.168.1.104:9000/mapreduce/findmaxnum/random.txt");
		FileInputFormat.addInputPath(job, inputPath);
		Path outputPath = new Path("hdfs://192.168.1.104:9000/mapreduce/findmaxnum/out1");
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new FindMaxNum(), args) ;
		System.exit(res);
	}
}
