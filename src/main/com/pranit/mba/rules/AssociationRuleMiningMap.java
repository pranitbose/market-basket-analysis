package com.pranit.mba.rules;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * For each <itemset,support> pair say <{a,b,c},4> Mapper will emit
 * <{a,b,c},4>
 * <{b,c},{a,b,c;4}>
 * <{a,c},{a,b,c;4}>
 * <{a,b},{a,b,c;4}>
 * Ignore {} braces, it has just been added here to highlight key and value separately.
 */

public class AssociationRuleMiningMap extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text key_out = new Text();
	private Text val_out = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] pair = value.toString().split("\t");
		String itemset = pair[0].trim();
		String supportCount = pair[1].trim();
		StringTokenizer itm = new StringTokenizer(itemset, ",");
		String[] items = new String[itm.countTokens()];
		int i = 0;
		while(itm.hasMoreTokens())
			items[i++] = itm.nextToken();
		key_out.set(itemset);
		val_out.set(supportCount);
		context.write(key_out, val_out);
		
		if(items.length > 1) {
            for(String item1 : items) {
                StringBuilder subsetComboBuilder = new StringBuilder();
                for (String item2 : items) {
                    if (!item2.equals(item1)) {
                    	subsetComboBuilder.append(item2);
                    	subsetComboBuilder.append(",");
                    }
                }
                key_out.set(subsetComboBuilder.toString().replaceAll(",$", ""));
                val_out.set(itemset+";"+supportCount);
                context.write(key_out, val_out);
            }
		}
	}
}