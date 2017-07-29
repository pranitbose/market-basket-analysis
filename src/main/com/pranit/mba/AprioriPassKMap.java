package com.pranit.mba;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.pranit.mba.utils.AprioriAlgorithm;
import com.pranit.mba.utils.Utilities;

/*
 * K pass Apriori MapReduce
 * Mapper for each Pass i <= 1 to K, would emit a <itemset, 1> pair 
 * for each generated candidate item-set of size i ( > 1) from frequent item-set
 * of previous pass i-1 across all transactions.
 * Only for Pass 1 we simply emit <item, 1> pair and do nothing else.
 */

public class AprioriPassKMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text item = new Text();
	private final static IntWritable one = new IntWritable(1);
	private AprioriAlgorithm apriori = new AprioriAlgorithm();
	private ArrayList<String> candidateList; // Store the candidate list generated for Pass i
	private String delimiter;
	private int pass;
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		pass = conf.getInt("APRIORI_PASS", 1);
		delimiter = conf.get("DELIMITER");
		String savedStatePath = conf.get("SAVED_STATE_PATH");
		if(pass > 1)  { // Fetch the candidate list
			Utilities util = new Utilities(); 
			try {
				apriori = util.deserialize(savedStatePath); // Load the saved object state into current object
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			candidateList = apriori.getNextCandidateItemsets();
			util.serialize(savedStatePath, apriori); // Save the current object state
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		HashSet<String> txnItemSet = new HashSet<String>();
		String txn = value.toString();
		StringTokenizer items = new StringTokenizer(txn, delimiter);
		while(items.hasMoreTokens()) {
			String itm = items.nextToken();
			itm = itm.trim().replaceAll(" +", " ");
			if(pass == 1) {
				item.set(itm);
				context.write(item, one); // Emit <item, 1> pair
			}
			else
				txnItemSet.add(itm);
		}
		if(pass == 1)
			return;
		countItemsetsPassK(context, txnItemSet);
	}
	
	// Emit <itemset, 1> pair for those candidate item-sets which occur in each input transaction record
	
	private void countItemsetsPassK(Context context, HashSet<String> txnItemSet) throws IOException, InterruptedException {
		
		for(String itemset : candidateList) {
			String[] items = itemset.split(",");
			boolean found = true;
			for(String itm : items) {			
				if(!txnItemSet.contains(itm)) {
					found = false;
					break;
				}
			}
			if(!found) // Discard this item-set if any one of it's item is not found in this transaction record
				continue;
			item.set(itemset);
			context.write(item, one); // Emit <itemset, 1> pair
		}
	}
}