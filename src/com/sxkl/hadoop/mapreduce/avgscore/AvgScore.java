package com.sxkl.hadoop.mapreduce.avgscore;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvgScore extends Configured implements Tool{
	
	public static class AvgScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text resultKey = new Text();
			DoubleWritable resultValue = new DoubleWritable();
			
			String[] inputs = value.toString().split(" ");
			String name = inputs[0];
			double score = Double.parseDouble(inputs[1]);
			resultKey.set(name);
			resultValue.set(score);
			context.write(resultKey, resultValue);
		}
	}
	
	public static class AvgScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> scores, Context context) throws IOException, InterruptedException {
			DoubleWritable avgScore = new DoubleWritable();
			double sum = 0;
			int num = 0;
			for (DoubleWritable score : scores) {
				sum += score.get();
				num++;
			}
			avgScore.set(sum/num);
			context.write(key, avgScore);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "avg score");
		job.setJarByClass(AvgScore.class);
		
		job.setMapperClass(AvgScoreMapper.class);
		job.setReducerClass(AvgScoreReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		Path inputPath = new Path("hdfs://192.168.1.104:9000/mapreduce/avgscore/*");
		FileInputFormat.addInputPath(job, inputPath);
		Path outputPath = new Path("hdfs://192.168.1.104:9000/mapreduce/avgscore/out");
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new AvgScore(), args) ;
		System.exit(res);
	}
}
