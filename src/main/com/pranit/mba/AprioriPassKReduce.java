package com.pranit.mba;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.pranit.mba.utils.AprioriAlgorithm;
import com.pranit.mba.utils.Utilities;

public class AprioriPassKReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable count = new IntWritable();
	private AprioriAlgorithm apriori = new AprioriAlgorithm();
	private Utilities util = new Utilities();
	private ArrayList<String> frequentList; // Used only for Pass 1
	private int minSupportCount;
	private int pass;
	private String savedStatePath;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		pass = conf.getInt("APRIORI_PASS", 1);
		savedStatePath = conf.get("SAVED_STATE_PATH");
		try {
			apriori = util.deserialize(savedStatePath); // Load the saved object state into current object
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		minSupportCount = conf.getInt("MIN_SUPPORT_COUNT", 1);
		if(pass == 1)
			frequentList = new ArrayList<String>(); 
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable value : values)
			sum += value.get();
		// Prune by threshold Support Count
		if(sum < minSupportCount) // Not frequent; Discard
			return;
		if(pass == 1) // Build the frequent item list for Pass 1
			frequentList.add(key.toString());
		apriori.mapPut(key.toString(), sum); // Store <frequent_itemset, count> pair in Map
		count.set(sum);	
		context.write(key, count); // Write to HDFS <frequent_itemset, count> as output of Pass i
	}
	
	@Override
	protected void cleanup(Context context) throws IOException {
		if(pass == 1)
			apriori.buildItemsList(frequentList); // Build the frequent item list for Pass 1
		/*
		 * Filter the Candidate list of current pass by threshold support count 
		 * to get the Frequent list for the next pass ready.
		 */
		apriori.nextFrequentItemsets();
		util.serialize(savedStatePath, apriori); // Save the current object state
	}
}