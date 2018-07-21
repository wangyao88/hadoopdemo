package com.sxkl.hadoop.mapreduce.statisticscore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author wangyao
 * @date 2018年7月21日 上午11:05:19
 */
public class ScoreWritable implements WritableComparable<Object>{
	
	private float chinese;
	private float math;
	private float english;
	private float physics;
	private float chemistry;

	public ScoreWritable() {}

	public ScoreWritable(float chinese, float math, float english, float physics, float chemistry) {
		super();
		this.chinese = chinese;
		this.math = math;
		this.english = english;
		this.physics = physics;
		this.chemistry = chemistry;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		chinese = in.readFloat();
		math = in.readFloat();
		english = in.readFloat();
		physics = in.readFloat();
		chemistry = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(chinese);
		out.writeFloat(math);
		out.writeFloat(english);
		out.writeFloat(physics);
		out.writeFloat(chemistry);
	}

	@Override
	public int compareTo(Object obj) {
		return 0;
	}
	
	public float getToltal() {
		return this.getChinese() + this.getMath() + this.getEnglish() + this.getPhysics() + this.getChemistry();
	}

	public float getAvg() {
		return this.getToltal()/5;
	}
	
	public void set(float chinese, float math, float english, float physics, float chemistry) {
		this.chinese = chinese;
		this.math = math;
		this.english = english;
		this.physics = physics;
		this.chemistry = chemistry;
	}

	public float getChinese() {
		return chinese;
	}

	public float getMath() {
		return math;
	}

	public float getEnglish() {
		return english;
	}

	public float getPhysics() {
		return physics;
	}

	public float getChemistry() {
		return chemistry;
	}
}
