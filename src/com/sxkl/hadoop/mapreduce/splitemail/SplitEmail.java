package com.sxkl.hadoop.mapreduce.splitemail;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author wangyao
 * @date 2018年7月21日 上午10:10:26
 */
public class SplitEmail extends Configured implements Tool {
	
	public static class SplitEmailMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, one);
		}
	}
	
	public static class SplitEmailReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();
		private MultipleOutputs<Text, IntWritable> out;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			out = new MultipleOutputs<>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int begin = key.toString().indexOf("@");
			int end = key.toString().indexOf(".");
			if(begin >= end) {
				return;
			}
			String name = key.toString().substring(begin+1, end);
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
		    result.set(sum);
		    out.write(key, result, name);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(!Objects.isNull(out)) {
				out.close();
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration() ;
		//删除已经存在的输出目录
		Path mypath = new Path(args[1]) ;
		FileSystem hdfs = mypath.getFileSystem(conf);
		if(hdfs.isDirectory(mypath)){
			hdfs.delete(mypath, true) ;
		}
		
		Job job = Job.getInstance(conf, "emailcount") ;
		job.setJarByClass(SplitEmail.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SplitEmailMapper.class);
		job.setReducerClass(SplitEmailReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] params = {"hdfs://192.168.1.104:9000/mapreduce/splitemail",
		"hdfs://192.168.1.104:9000/mapreduce/splitemail/output"} ;
		int res = ToolRunner.run(new SplitEmail(), params) ;
		System.exit(res);
	}

}
