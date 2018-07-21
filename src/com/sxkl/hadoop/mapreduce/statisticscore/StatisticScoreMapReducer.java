package com.sxkl.hadoop.mapreduce.statisticscore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sxkl.hadoop.mapreduce.splitemail.SplitEmail;

/**
 * @author wangyao
 * @date 2018年7月21日 上午11:03:59
 */
public class StatisticScoreMapReducer extends Configured implements Tool {

	public static class StatisticScoreMapper extends Mapper<Text, ScoreWritable, Text, ScoreWritable> {

		@Override
		protected void map(Text key, ScoreWritable value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class StatisticScoreReducer extends Reducer<Text, ScoreWritable, Text, Text> {

		private Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<ScoreWritable> scores, Context context) throws IOException, InterruptedException {
			float total = 0f;
			float avg = 0f;
			for (ScoreWritable score : scores) {
				total += score.getToltal();
				avg = score.getAvg();
			}
			outValue.set(total + "\t" + avg);
			context.write(key, outValue);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//删除已经存在的输出目录
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if(hdfs.isDirectory(mypath)){
			hdfs.delete(mypath, true);
		}
		
		Job job = Job.getInstance(conf, "scorecount");
		job.setJarByClass(StatisticScoreMapReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(StatisticScoreMapper.class);
		job.setReducerClass(StatisticScoreReducer.class);
		
		//如果是自定义的类型，需要进行设置
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreWritable.class);
		
		//设置自定义的输入格式
		job.setInputFormatClass(ScoreInputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] params = {"hdfs://192.168.1.104:9000/mapreduce/statisticscore",
		"hdfs://192.168.1.104:9000/mapreduce/statisticscore/output"} ;
		int res = ToolRunner.run(new StatisticScoreMapReducer(), params) ;
		System.exit(res);
	}
}
