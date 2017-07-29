package com.pranit.mba.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Aggregate the Rules sorted by Confidence first followed by List of the Rules.
 * Remove any Redundant Rules and write to Final Output only the valid Association Rules found.
 */

public class RuleAggregatorReduce extends Reducer<Text, Text, Text, Text> {

	private Text key_out = new Text();
	private Text value_out = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	// Formatted header of final output; written only once to output file
        key_out.set(String.format("\t%-85s", "ASSOCIATION RULE"));
        value_out.set(String.format("%-10s", "SUPPORT")+"   "+String.format("%-10s", "CONFIDENCE")+"\t      "+String.format("%-5s", "LIFT"));
        context.write(key_out, value_out);
        key_out.set("");
        value_out.set("");
        context.write(key_out, value_out);
    }
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
		String[] pair, measures;
		String rule, antecedent, data;
		double support, confidence, lift, liftPercent;
		ArrayList<String> cache = new ArrayList<String>(); // Caching to iterate twice
		HashSet<String> redundantRules = findRedundantRules(values, cache);
        for(String value : cache) {
        	pair = value.toString().split("\t");
            rule = pair[0];
            antecedent = rule.split(" => ")[0].trim().replaceAll("^\\[|\\]$", "");
            if(redundantRules.contains(antecedent)) // Skip the Rule if redundant
            	continue;
            measures = pair[1].replaceAll("^\\(| |\\)$", "").split(",");
            support = Double.parseDouble(measures[0]) * 100;
            confidence = Double.parseDouble(measures[1]) * 100;
            lift = Double.parseDouble(measures[2]);
            liftPercent = (lift - 1.0) * 100;
            
            // Formatted content of final output
            key_out.set(String.format(" %-90s", rule));
            data = String.format("%6.2f", support)+"%\t\t"+String.format("%3.0f", confidence)+"%\t  "+String.format("%.3f", lift)+" -> "+String.format("%3.0f", liftPercent)+"%";
            value_out.set(data);
            context.write(key_out, value_out);
        }
	}
	
	/*
	 * Find the redundant Rules from the antecedent and confidence of these Rules.
	 * As Rules are grouped by Consequent, in each call of reduce() 
	 * multiple Rules will have the same consequent.
	 * So fining the redundant antecedents is equivalent to finding redundant Rules.
	 */
	
	private HashSet<String> findRedundantRules(Iterable<Text> values, ArrayList<String> cache) {
		HashMap<String, Double> map = new HashMap<String, Double>();
		ArrayList<String> rules = new ArrayList<String>();
		String[] pair, measures;
		String rule, antecedent;
		for(Text value : values) {
			cache.add(value.toString());
			pair = value.toString().split("\t");
			rule = pair[0];
			antecedent = rule.split(" => ")[0].trim().replaceAll("^\\[|\\]$", "");
			measures = pair[1].replaceAll("^\\(| |\\)$", "").split(",");
			map.put(antecedent, new Double(measures[1])); // Store <antecedent, confidence> pair
		}
		// Sort the antecedents by it's Confidence in descending order
		map.entrySet().stream().sorted((e1, e2) -> {
				int c1 = (-1) * e1.getValue().compareTo(e2.getValue());
				if(c1 == 0) {
					Integer len1 = e1.getKey().length();
					Integer len2 = e2.getKey().length();
					int c2 = (-1) * len1.compareTo(len2);
					if(c2 == 0)
						return e1.getKey().compareTo(e2.getKey());
					return c2;
				}
				return c1;
				
			}).forEach(e -> rules.add(e.getKey()));

		HashSet<String> redundant = new HashSet<String>(); //  Store the redundant antecedents
		for(int i=rules.size()-1; i>=1; i--) {
			String itemset1 = rules.get(i);
			for(int j=i-1; j>=0; j--) {				
				String itemset2 = rules.get(j);
				if(!isSubset(itemset1, itemset2))
					continue;
				// A subset rule has greater confidence than it's super-set rule
				if(map.get(itemset1) >= map.get(itemset2))
					redundant.add(itemset2); // Mark the super-set (item-set) and thus it's rule as redundant
			}
		}
		return redundant;
	}
	
	// isSubset(a, b) returns true if a is a subset of b otherwise false
	
	private boolean isSubset(String itemset1, String itemset2) {
		if(itemset1.length() > itemset2.length()) // item-set1 is larger than item-set2; Not a subset
			return false;
		HashSet<String> set = new HashSet<String>();
		StringTokenizer is1 = new StringTokenizer(itemset1,",");
		StringTokenizer is2 = new StringTokenizer(itemset2,",");
		while(is2.hasMoreTokens())
			set.add(is2.nextToken());
		while(is1.hasMoreTokens()) {
			if(!set.contains(is1.nextToken()))
				return false; // Item in item-set1 is not part of item-set2; Not a subset
		}
		return true; // Is a subset
	}
}