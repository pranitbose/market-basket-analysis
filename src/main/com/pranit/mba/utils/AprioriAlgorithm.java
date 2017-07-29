package com.pranit.mba.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

/* 
 * Apriori Algorithm - Java Implementation of Apriori to serve as helper class for MapReduce jobs
 * Generate and Prune till we get all the valid frequent item-sets for the input dataset,
 * which we can use to build the Association Rules for analysis.
 * 
 * This implementation uses Object Serialization to persist the data stored in the variables
 * and data-structures in between multiple MapReduce jobs and tasks.
 * State of the current object is saved in the Local File System and when needed is retrieved 
 * from Local FS to load the current object with previously the saved values.  
 */

public class AprioriAlgorithm implements Serializable {
	
	private static final long serialVersionUID = -6881613323741098535L;	
	
	// Store <itemset, count> pair for all frequent item-sets
	private HashMap<String, Integer> map = new HashMap<String, Integer>();
	// Store the list of Candidate item-sets first and then the Frequent item-sets for current Pass 
	private ArrayList<String> list = new ArrayList<String>();
	private int minSupportCount; // Store the threshold Support Count
	private int maxPass; // Store the maximum number of Passes
	private int curPass = 1; // Store the current Pass
	
	// Build the initial list with frequent items from Pass 1
	public void buildItemsList(ArrayList<String> freqItemsPass1) {
		list = new ArrayList<String>(freqItemsPass1);
	}
	
	// Set the minimum Support Count that each frequent item-set should satisfy
	
	public void setMinSupportCount(int supportCount) {
		minSupportCount = supportCount;
	}
	
	/*
	 * Set the maximum number of pass for which the Algorithm will run and then terminate
	 * if it doesn't converge within this specified number of maximum passes.
	 */
	
	public void setMaxPass(int pass) {
		maxPass = pass;
	}
	
	// Return all Frequent list of item-sets and it's corresponding Support Counts
	
	public HashMap<String, Integer> getFrequentItemsets() {
		return map;
	}
	
	// Return the current value of Pass
	
	public int getCurrentPass() {
		return curPass;
	}
	
	// Increment the current value of Pass
	
	public void nextPass() {
		curPass++;
	}
	
	// Store in Map <itemset, count> pair
	
	public void mapPut(String key, int value) {
		map.put(key, value);
	}
	
	// Retrieve from Map the value of <count> for the corresponding <itemset> key
	
	public Integer mapGet(String key) {
		return map.get(key);
	}
	
	/*
	 * Loop until algorithm converges and no more frequent item-set can be generated
	 * Or loop until current Pass exceeds the maximum number of passes set.
	 * The algorithm terminates on whichever condition is reached first.
	 */
	
	public boolean hasConverged() {
		return curPass > maxPass || (curPass > 1 && list.size() <= 1); 
	}
	
	// Return Candidate list of item-sets for next Pass
	
	public ArrayList<String> getNextCandidateItemsets() {
		generateNextCandidateItemsets();
		return list;
	}
	
	/*
	 * Generate list of Candidate item-sets of size K from list Frequent item-sets of size K-1
	 * Frequent list of previous pass is used to generate Candidate list of current pass.
	 */
	
	private void generateNextCandidateItemsets() {
		ArrayList<String> temp = new ArrayList<String>();
		HashSet<String> set = new HashSet<String>();		
		for(String itemset1 : list) {
			for(String itemset2:list) {
				if(itemset1.equals(itemset2))
					continue;
				String newItemset = union(itemset1, itemset2, set);
				if(newItemset == null)
					continue;
				if(temp.contains(newItemset))
					continue;
				temp.add(newItemset);
			}
		}
		list = new ArrayList<String>(temp); // Update the current list of item-sets
	}
	
	/*
	 *  Build larger item-sets of size K from two small item-sets, each of size K-1
	 *  Two small item-sets must differ from each other by only element
	 */
	
	private String union(String itemset1, String itemset2, HashSet<String> set) {
		StringTokenizer is1 = new StringTokenizer(itemset1,",");
		StringTokenizer is2 = new StringTokenizer(itemset2,",");
		int cnt = is1.countTokens();
		int i = 1;
		while(is1.hasMoreTokens() && is2.hasMoreTokens()) {
			String item1 = is1.nextToken();
			String item2 = is2.nextToken();
			if(item1.equals(item2) && i<cnt) { // Matches till the last element of both item-sets 
				set.add(item1);
				i++;
				continue;
			}
			if(i < cnt) { // Mismatch before the last element of both item-sets; Discard
				set.clear();
				return null;
			}
			// Mismatch at the last element of both item-sets; Add both elements
			set.add(item1);
			set.add(item2);
		}
		Iterator<String> itr = set.iterator();
		StringBuilder result = new StringBuilder(); // Build the resultant item-set separated by comma
		while(itr.hasNext()) {
			result.append(itr.next());
			result.append(",");		
		}
		set.clear();
		return result.toString().replaceAll(",$", "");
	}
	
	// Build the Frequent list of item-sets for next Pass from Candidate list of current Pass
	
	public void nextFrequentItemsets() {
		ArrayList<String> temp = new ArrayList<String>();
		for(String itemset : list) {		
			Integer val = (Integer)map.get(itemset);
			// This item-set generated is not found in the transaction data-set so discard it
			if(val == null)
				continue;
			// Prune by Support Count threshold to extract frequent item-sets only
			if(val < minSupportCount)
				continue;
			temp.add(itemset);
		}
		list = new ArrayList<String>(temp); // Update the current list of item-sets
	}
}