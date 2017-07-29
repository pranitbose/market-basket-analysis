package com.pranit.mba.rules;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 *  To create custom key with consequent & lift of the rule and emit it 
 *  so that our final list of rules gets grouped and sorted by this custom key.
 */

public class RuleAggregatorMap extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text key_out = new Text();
	private Text value_out = new Text();

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyValue = value.toString().split("\t");
		String rule = keyValue[0];
        String measures = keyValue[1];
        String lift = measures.toString().split(",")[2].trim().replace(")", "");
        String consequent = rule.split(" => ")[1].trim().replaceAll("^\\[|\\]$", "");
        key_out.set(consequent+"#"+lift);
        value_out.set(rule+"\t"+measures);
        context.write(key_out, value_out);
	}
}