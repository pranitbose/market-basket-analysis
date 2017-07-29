package com.pranit.mba.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.pranit.mba.utils.AprioriAlgorithm;
import com.pranit.mba.utils.Utilities;

/*
 * Mining Association Rules for each frequent item-set and validate it by threshold Confidence
 * Mining Rules only with 1 item as consequent
 * 
 * If X => Y is the Rule obtained then
 * Rule Support: Support(X+Y)
 * Antecedent Support: Support(X)
 * Consequent Support: Support(Y)
 * Confidence: Support(X+Y) / Support(X)
 * Lift: Support(X+Y) / (Support(X) * Support(Y))
 * 
 * Input for Reducer will be formatted as below, for example,
 * key -> a,b
 * values -> [ {a,b,c;4}, {a,b,d;3}, 6, {a,b,f;4} ]
 * where each value is say,  a,b,c;4
 * and support of a,b is 6 here.
 * Ignore the {} braces it's only for illustration purpose.
 */

public class AssociationRuleMiningReduce extends Reducer<Text, Text, Text, Text> {

    private Text ruleKey = new Text();
    private Text ruleValue = new Text();
    private AprioriAlgorithm apriori = new AprioriAlgorithm();
    private HashSet<String> rules = new HashSet<String>();
    private double minConfidence;
    private int txnCount;
    private boolean liftFilter;
    
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        minConfidence = conf.getDouble("MIN_CONFIDENCE", 0.1);
        txnCount = conf.getInt("TRANSACTION_COUNT", 1);
        liftFilter = conf.getBoolean("LIFT_FILTER", true);
        String savedStatePath = conf.get("SAVED_STATE_PATH");
        Utilities util = new Utilities(); 
        try {
            apriori = util.deserialize(savedStatePath); // Load the saved object state into current object
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> itemsetMap = new HashMap<String, Integer>();
        double antecedentSupport = 1.0;
        
        StringTokenizer antecedentST = new StringTokenizer(key.toString(), ",");
        ArrayList<String> antecedentItems = new ArrayList<String>();
        while(antecedentST.hasMoreTokens())
            antecedentItems.add(antecedentST.nextToken());

        // Store super-sets (item-sets) of antecedent and it's support count
        for(Text textItemset : values) { 
            StringTokenizer item = new StringTokenizer(textItemset.toString(), ";");
            String[] itemset = new String[item.countTokens()];
            int i = 0;
            while(item.hasMoreTokens())
                itemset[i++] = item.nextToken();
            
            if(itemset.length == 1) // Support of antecedent               
                antecedentSupport = 1.0*Double.parseDouble(itemset[0]) / txnCount;
            else {
                
                String itemsetKey = itemset[0]; // Super-set of antecedent             
                int supportCount = Integer.parseInt(itemset[1]); // Support count of that super-set
                itemsetMap.put(itemsetKey, supportCount);
            }
        }

        // Association Rule mining and Rule validation
        double ruleSupport, consequentSupport;
        double confidence, lift;
        String antecedent, consequent;
        // Loop to generate various consequents from antecedent and it's list of super-sets
        for(String itemset : itemsetMap.keySet()) {
            StringTokenizer items = new StringTokenizer(itemset, ",");
            HashSet<String> consequentItems = new HashSet<String>();
            while(items.hasMoreTokens())
                consequentItems.add(items.nextToken());
            
            consequentItems.removeAll(antecedentItems);
            if(consequentItems.size() == 1)
                consequentSupport = 1.0*apriori.mapGet(consequentItems.toArray()[0].toString()) / txnCount;
            else
                consequentSupport = -1.0;
            
            ruleSupport = 1.0*itemsetMap.get(itemset) / txnCount;
            confidence = ruleSupport / antecedentSupport;
            lift = confidence / consequentSupport;

            // Prune or Validate the Rule obtained
            if(confidence >= minConfidence && (!liftFilter || lift > 1.0)) {
                antecedent = "["+key.toString()+"]";
                consequent = consequentItems.toString().replaceAll("(, )+", ",").trim();
                String rule = antecedent + " => " + consequent;
                String revRule = consequent + " => " + antecedent;
                if(rules.contains(rule) || rules.contains(revRule)) // Avoid duplicate and reverse rules
                    continue;
                rules.add(rule);
                ruleKey.set(rule);
                ruleValue.set("("+String.format("%.6f", ruleSupport)+", "+String.format("%.6f", confidence)+", "+String.format("%.6f", lift)+")");
                context.write(ruleKey, ruleValue);
            }
        }
	}
}