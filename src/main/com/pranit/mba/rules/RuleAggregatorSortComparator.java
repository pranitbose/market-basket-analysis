package com.pranit.mba.rules;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Sort Rules first by Consequent and then by Lift of the Rule

public class RuleAggregatorSortComparator extends WritableComparator {

	public RuleAggregatorSortComparator() {
		super(Text.class, true); // True will enable to create instances
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		String[] keyA = a.toString().split("#");
        String primaryKeyA = keyA[0]; // Consequent
        Double secondaryKeyA = new Double(keyA[1]); // Lift
        String[] keyB = b.toString().split("#");
        String primaryKeyB = keyB[0];
        Double secondaryKeyB = new Double(keyB[1]);

        int c = primaryKeyA.compareTo(primaryKeyB);
        if(c == 0)
            return (-1) * secondaryKeyA.compareTo(secondaryKeyB); // Sort in descending order by Lift
        return c;
	}
}