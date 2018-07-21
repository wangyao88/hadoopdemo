package com.sxkl.hadoop.mapreduce.statisticscore;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * @author wangyao
 * @date 2018年7月21日 上午11:11:05
 */
public class ScoreRecordReader extends RecordReader<Text, ScoreWritable> {
	
	//行读取器
	public LineReader lineReader;
	//行数据
	public Text line;
	//自定义key类型
	public Text lineKey;
	//自定义的value类型
	public ScoreWritable lineValue;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit)inputSplit;
		Configuration conf = context.getConfiguration();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(file);
		lineReader = new LineReader(fileIn, conf);
		line = new Text();
		lineKey = new Text();
		lineValue = new ScoreWritable();
	}
	
	/**
	 * 读取每一行数据的时候，都会执行该方法
	 * 我们只需要根据自己的需求，重点编写该方法即可，其他的方法比较固定，仿照就好
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		int lineSize = lineReader.readLine(line);
		if(lineSize == 0) {
			return false;
		}
		String[] pieces = line.toString().split("\\s+");
		if(pieces.length != 7) {
			throw new IOException("无效数据");
		}
		float chinese = 0, math = 0, english = 0, physics = 0, chemistry = 0;
		try {
			chinese = Float.parseFloat(pieces[2].trim());
			math = Float.parseFloat(pieces[3].trim());
			english = Float.parseFloat(pieces[4].trim());
			english = Float.parseFloat(pieces[5].trim());
			chemistry = Float.parseFloat(pieces[6].trim());
		} catch (NumberFormatException  e) {
			throw new IOException("无效数据");
		}
		lineKey.set(pieces[0] + "\t" + pieces[1]);
		lineValue.set(chinese, math, english, physics, chemistry);
		return true;
	}
	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return lineKey;
	}

	@Override
	public ScoreWritable getCurrentValue() throws IOException, InterruptedException {
		return lineValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}
	
	@Override
	public void close() throws IOException {
		if(!Objects.isNull(lineReader)) {
			lineReader.close();
		}
	}

}
