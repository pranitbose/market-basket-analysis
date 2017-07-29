package com.pranit.mba.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utilities {
	
	// Write <ItemSet, Count> pair from Map to HDFS
	
	public void addFileToHDFS(Configuration conf, String filePath, Map<String, Integer> map) throws IOException {
		FileSystem hdfs = FileSystem.get(conf);
		Path file = new Path(filePath);
		OutputStream os = hdfs.create(file, true);
		
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os));
		StringBuilder content = new StringBuilder();
		for(Entry<String, Integer> pair : map.entrySet()) {
			content.append(pair.getKey());
			content.append("\t");
			content.append(pair.getValue());
			content.append("\n");
		}
		out.write(content.toString());
		out.close();
		os.close();
	}

	// Read <ItemSet, Count> pair from HDFS to Map
	
	public HashMap<String, Integer> getFileFromHDFS(Configuration conf, String filePath) throws IOException {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		FileSystem hdfs = FileSystem.get(conf);
		Path file = new Path(filePath);
		if(!hdfs.exists(file)) {
			return map;
		}
		InputStream is = hdfs.open(file);
		BufferedReader in = new BufferedReader(new InputStreamReader(is));
		String line = null;
		while ((line = in.readLine()) != null && line.trim().length() > 0) {
            String[] pair = line.split("\t");
            map.put(pair[0], Integer.parseInt(pair[1]));
        }
		return map;
	}
	
	// Serialize the current state of AprioriAlgorithm object and save it to local File System
	
	public void serialize(String path, AprioriAlgorithm apriori) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path, false)); // Overwrite existing file
	    oos.writeObject(apriori);
	    oos.close();
	}
	
	// Retrieve the saved state of AprioriAlgorithm object from local File System and deserialize it
	
	public AprioriAlgorithm deserialize(String path) throws IOException, ClassNotFoundException {
		AprioriAlgorithm apriori = null;
		if(path == null)
			return apriori;
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path));
	    apriori = (AprioriAlgorithm)ois.readObject();
	    ois.close();
	    return apriori;
	}
}
