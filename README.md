# Market Basket Analysis
Hadoop MapReduce implementation of Market Basket Analysis for Frequent Item-set and Association Rule mining using Apriori algorithm.

## Description
This Big Data project is a simple working model of Market Basket Analysis. This project is implemented using Hadoop MapReduce
framework. Basically this project runs multiple MapReduce jobs to produce the final output. This project uses K-Pass Apriori algorithm for frequent item-sets mining followed by association rule mining to generate all the valid Rules and their corresponding measures such as Support, Confidence and Lift. The frequent item-sets are obtained using a threshold Support and the Rules are validated using a threshold Confidence. Duplicate, reverse and redundant rules are removed to produce interesting and useful rules only. These list of Rules sorted by consequent (RHS of the association) first and then by Lift is the final output of this project. The entire process of building and running this project has been automated using Gradle. Check the _Usage_ section for more details.

## Prerequisites
Make sure you have the following list of dependencies for this project installed and setup on your system first:

- Linux Operating System
- Java JDK 1.8+
- Hadoop 2.7.1+
- Gradle 4.0.1+

## Usage
First download the project as zip archive and extract it to your desired location or just clone the repository using,

```
$ git clone https://github.com/pranitbose/market-basket-analysis.git
```

This project uses Hadoop 2.7.3 by default. If you have an older release of Hadoop installed then you can update the jar dependencies of the project to your current Hadoop release version. For example if you have Hadoop 2.7.1 installed then you need to edit the _build.gradle_ file changing the following lines to match your current version. Change it from 2.7.3 to 2.x.y,

```groovy
dependencies {
	compile 'org.apache.hadoop:hadoop-common:2.7.1'
	compile 'org.apache.hadoop:hadoop-mapreduce-client-core:2.7.1'
	compile 'org.apache.hadoop:hadoop-core:1.2.1'
}
```

*NOTE*: This project is not tested for older releases of Hadoop below 2.7.1 and it is recommended not to use an older Hadoop release to avoid compatibility issues.

Please refer to this documentation [Gradle Install](https://gradle.org/install/) to install and setup Gradle on your system.

Make sure that you start your node cluster first by using `start-dfs.sh` followed by `start-yarn.sh`. You can check whether the deamons are running using `jps` command.

### Configure the Project
You can change the configuration paramters to run this project with your specified settings. All the configurable parameters to customize is available in the _config_ file. Edit this file to update default values of the paramters to your desired values.
*#* denotes comments in this file which has been provided for your information. Some of this parameters will be passed as command line arguments to the jar file while running this project while others will be used to automate the running process. Multiple transaction datasets has been put under _dataset/_ directory. If you want to use your own dataset just copy that file into this directory mentioned and change the dataset name parameter in the _config_ file to the one you will be using. If you want to run the project with different minimum Support or Confidence value then you have to change the value and save the file before next run. 

_NOTE_: Please **don't change the order of the paramters** in the config file. Keep the order intact as it is else you will run into errors while running this project. You can add additional comments if you want prefixing with # 

### Run the Project
Since the entire process of running this project has been automated you can simply straightaway get the task rolling.
Move into the project folder and do the following,

__Compile and Build the jar__

```
$ gradle build
```

__Run the jar__

```
$ gradle run
```

That's all you need to do! Just sit and wait for the the gradle _run_ task to complete running this project. This _run_ task will basically do the following:
1. Copy input dataset from local File System to HDFS (Overwrites if the directory in HDFS already exists)
2. Delete the output directory in HDFS
3. Run the jar with required arguments using `hadoop jar` command
4. Copy the output directory in HDFS to local File System (Overwrites if the directory in local file system already exists. Give a different output path in local file system or rename the output directories produced by previous runs if you don't want previous Job or Project outputs overwritten)

_NOTE_: If you change some value in the config file then you can only execute `gradle run` command provided that you had already built the jar before at some point of time. You will only need to execute `gradle build` again if you have changed the source code of some file or some jar dependencies.

If you want to skip the `gradle run` step and manually want to run the jar file to have more control on what happens then you need to know the paramters you need to pass while executing the jar file.

You can run the project like this:

```
$ hadoop jar ./build/libs/mba.jar <inp_dir> <out_dir> <min_sup (0.0-1.0)> <min_conf (0.0-1.0)> <txns_count> <delimiter> <max_pass> <filterbylift (0|1)>
```

- *inp_dir*: path to the input dataset in HDFS
- *out_dir*: path to the output directory in HDFS which will store all the intermediate and final results.
- *min_sup*: minimum support value for frequent item-set mining. The value should be in the range 0.0 - 1.0
- *min_conf*: minimum confidence value for association rule mining. The value should be in the range 0.0 - 1.0
- *txns_count*: total number of transactions in the input dataset. Value should not be 0.
- *delimiter*: literal used in the dataset to separate multiple items in a single line of transaction. For a .csv file , will be the separator. If the the separator is whitespace then use qoutes to enclose it like this " "
- *max_pass*: maximum number of iterations you want the Apriori algorithm to run for. A value of 5 will find all frequent item-sets of size upto 5 if possible given the threshold support specified above.
- *filterbylift*: a value of 1 will filter all the rules by positive lift percentage and final output will only contain rules with lift > 1.0 otherwise a value of 0 will output all the rules irrespective of the lift value.

## License
This project is licensed under the terms of the MIT license.