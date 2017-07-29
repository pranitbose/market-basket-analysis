package com.pranit.mba;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.pranit.mba.rules.*;
import com.pranit.mba.utils.AprioriAlgorithm;
import com.pranit.mba.utils.Utilities;

// Driver program for Market Basket Analysis, a MapReduce implementation in Hadoop

public class MbaDriver extends Configured implements Tool {
	
	private final static String USAGE = "USAGE %s: <input dir path> <output dir path> <min. support> <min. confidence> <transaction count> <transaction delimiter> <max no. of passes> <enable/disable filter value>\n";
	private static String defFS; // Value of default HDFS
	private static String inputDir; // Path in HDFS
	private static String outputDir; // Path in HDFS
	private static int txnCount; // Total count of transactions in input data-set
	private static double minSupport;
	private static double minConfidence;
	// Delimiter to be used to extract each item across all the transactions in input data-set
	private static String delimiter;
	private static int maxPass; // Maximum no. of passes for which Apriori MapReduce job will run
	private static boolean liftFilter; // Filter Rules by positive Lift (> 1.0) or none
	private static String pathToSavedState; // Path to the saved state of AprioriAlgorithm object
	
	private AprioriAlgorithm apriori = new AprioriAlgorithm();
	private Utilities util = new Utilities();
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MbaDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 8) {
			//throw new IllegalArgumentException("Invalid arguments!\n"+USAGE);
			System.err.printf("Invalid arguments!\n"+USAGE, getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return 1;
		}
		// Configure the path to where the Object state of the AprioriAlgorithm will be saved
		String pwd = Paths.get(".").toAbsolutePath().normalize().toString(); // Present working directory
		pathToSavedState = pwd + "/tmp";
		new File(pathToSavedState).mkdir();
		pathToSavedState = pathToSavedState + "/apriori_saved_state.ser";
		new File(pathToSavedState).createNewFile();
		
		// Store the arguments received through Command Line
		inputDir = args[0];
		outputDir = args[1];
		minSupport = Double.parseDouble(args[2]);
		minConfidence = Double.parseDouble(args[3]);
		txnCount = Integer.parseInt(args[4]);
		delimiter = args[5];
		maxPass = Integer.parseInt(args[6]);
		liftFilter = (args[7].equals("1")) ? true : false;
		
		int minSupportCount = (int)Math.ceil(minSupport * txnCount); // Calculate Absolute Support from threshold Support
		
		Configuration conf = new Configuration();
		defFS = conf.get("fs.defaultFS");
		apriori.setMinSupportCount(minSupportCount);
		apriori.setMaxPass(maxPass);
		
		// Start of Jobs
		jobFrequentItemsetMining(minSupportCount);
		
		// Write to HDFS the list of all Frequent item-sets found
		String filePath = defFS + outputDir + "/all-frequent-itemsets/freq-list";
		LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
		// Sort item-sets in descending order of their Support Counts
		apriori.getFrequentItemsets().entrySet().stream().sorted((e1, e2) -> (-1) * e1.getValue().compareTo(e2.getValue())).forEachOrdered(e -> map.put(e.getKey(), e.getValue()));
		util.addFileToHDFS(conf, filePath, map);
		
		jobAssociationRuleMining();
		jobAssociationRuleAggregation();
		// End of Jobs
		return 0;
	}
	
	// Job -> Frequent item-sets Mining using K-Pass Apriori Algorithm
	
	private void jobFrequentItemsetMining(int minSupportCount) throws IOException, ClassNotFoundException, InterruptedException {
		String hdfsInputPath = defFS + inputDir;
		String hdfsOutputPath = defFS + outputDir + "/output-pass-";
		boolean success;
		while(!apriori.hasConverged()) {
			int currentPass = apriori.getCurrentPass();
			util.serialize(pathToSavedState, apriori);
			Configuration config = new Configuration();
			config.setInt("APRIORI_PASS", currentPass);
			config.set("DELIMITER", delimiter);
			config.setInt("MIN_SUPPORT_COUNT", minSupportCount);
			config.set("SAVED_STATE_PATH", pathToSavedState);
			config.setBoolean("mapreduce.map.output.compress", true); // Compress output of Mapper
			config.setBoolean("mapreduce.output.fileoutputformat.compress", false); // Reducer output left uncompressed
			Job job = Job.getInstance(config, "Apriori Pass "+currentPass);
			job.setJarByClass(MbaDriver.class);
			job.setMapperClass(AprioriPassKMap.class);
			job.setCombinerClass(AprioriPassKCombiner.class);
			job.setReducerClass(AprioriPassKReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
			FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath+currentPass));
			success = job.waitForCompletion(true);
			if(!success)
				throw new IllegalStateException("Job Apriori Pass "+currentPass+" failed!");
			apriori = util.deserialize(pathToSavedState);
			apriori.nextPass();
		}
		util.serialize(pathToSavedState, apriori);
	}
	
	// Job -> Association Rule Mining to find complete set of valid Rules from list of Frequent item-sets
	
	private void jobAssociationRuleMining() throws IOException, ClassNotFoundException, InterruptedException {
		String hdfsInputPath = defFS + outputDir + "/all-frequent-itemsets/freq-list";
		String hdfsOutputPath = defFS + outputDir + "/rule-mining-output";
		Configuration config = new Configuration();
		config.setDouble("MIN_CONFIDENCE", minConfidence);
		config.setInt("TRANSACTION_COUNT", txnCount);
		config.setBoolean("LIFT_FILTER", liftFilter);
		config.set("SAVED_STATE_PATH", pathToSavedState);
		Job job = Job.getInstance(config, "Association Rule Mining");
		job.setJarByClass(MbaDriver.class);
		job.setMapperClass(AssociationRuleMiningMap.class);
		job.setReducerClass(AssociationRuleMiningReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
		FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));
		boolean success = job.waitForCompletion(true);
		if(!success)
			throw new IllegalStateException("Job Association Rule Mining failed!");
	}
	
	// Job -> Association Rules Aggregation to remove any redundant Rules; Final Output of Market Basket Analysis
	
	private void jobAssociationRuleAggregation() throws IOException, ClassNotFoundException, InterruptedException {
		String hdfsInputPath = defFS + outputDir + "/rule-mining-output";
		String hdfsOutputPath = defFS + outputDir + "/final-output";
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Association Rule Aggregation");
		job.setJarByClass(MbaDriver.class);
		job.setMapperClass(RuleAggregatorMap.class);
		job.setReducerClass(RuleAggregatorReduce.class);
		job.setPartitionerClass(RuleAggregatorPartitioner.class);
		job.setGroupingComparatorClass(RuleAggregatorGroupComparator.class);
		job.setSortComparatorClass(RuleAggregatorSortComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
		FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));
		boolean success = job.waitForCompletion(true);
		if(!success)
			throw new IllegalStateException("Job Association Rule Aggregation failed!");
	}
	
}