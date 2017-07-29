package com.pranit.mba.rules;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// Partition based on Consequent of the Rules

public class RuleAggregatorPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int num_partition) {
		String consequent = key.toString().split("#")[0];
		return consequent.hashCode() % num_partition;
	}
}