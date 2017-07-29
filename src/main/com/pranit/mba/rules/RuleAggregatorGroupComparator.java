package com.pranit.mba.rules;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Group Rules by Consequent

public class RuleAggregatorGroupComparator extends WritableComparator {

	public RuleAggregatorGroupComparator() {
		super(Text.class, true); // True will enable to create instances
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		String primaryKeyA = a.toString().split("#")[0];
        String primaryKeyB = b.toString().split("#")[0];
        
        return primaryKeyA.compareTo(primaryKeyB);
	}
}