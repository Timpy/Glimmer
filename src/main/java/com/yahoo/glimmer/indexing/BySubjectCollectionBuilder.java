package com.yahoo.glimmer.indexing;

/*
 * Copyright (c) 2012 Yahoo! Inc. All rights reserved.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is 
 *  distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *  See accompanying LICENSE file.
 */

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.IdentityDocumentFactory;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BySubjectCollectionBuilder extends Configured implements Tool {

    public static String getTaskId(Configuration conf) throws IllegalArgumentException {
	if (conf == null) {
	    throw new NullPointerException("conf is null");
	}

	String taskId = conf.get("mapred.task.id");
	if (taskId == null) {
	    throw new IllegalArgumentException("Configuration does not contain the property mapred.task.id");
	}

	String[] parts = taskId.split("_");
	if (parts.length != 6 || !parts[0].equals("attempt") || (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
	    throw new IllegalArgumentException("TaskAttemptId string : " + taskId + " is not properly formed");
	}

	return parts[4];
    }

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

	private FileSystem fs;

	private SimpleCompressedDocumentCollectionBuilder builder;

	private String outputDir;

	private static int count;

	@Override
	public void setup(Context context) {
	    Configuration job = context.getConfiguration();

	    outputDir = job.get(OUTPUT_DIR);
	    if (!outputDir.endsWith("/"))
		outputDir = outputDir + "/";

	    try {

		fs = FileSystem.get(job);
		Path path = new Path(outputDir);
		if (!fs.exists(path)) {
		    fs.mkdirs(path);
		    FsPermission allPermissions = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
		    fs.setPermission(path, allPermissions);

		}

		// basename is actually the complete path
		builder = new SimpleCompressedDocumentCollectionBuilder("collection-", new IdentityDocumentFactory(), true);
		// Use a map task name as suffix
		// String suffix = getTaskId(job);
		// Use original file name as suffix
		String input = ((FileSplit) context.getInputSplit()).getPath().getName();
		String suffix = input.substring(input.lastIndexOf('-') + 1, input.lastIndexOf('.'));
		builder.open(outputDir, suffix, fs);

	    } catch (IOException e) {

		throw new RuntimeException(e);
	    }

	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	    String nextLine = value.toString();

	    if (count++ % 100000 == 0) {
		System.out.println("Processed " + count + " lines.");

		// Get current size of heap in bytes
		long heapSize = Runtime.getRuntime().totalMemory();

		// Get maximum size of heap in bytes. The heap cannot grow
		// beyond this size.
		// Any attempt will result in an OutOfMemoryException.
		long heapMaxSize = Runtime.getRuntime().maxMemory();

		// Get amount of free memory within the heap in bytes. This size
		// will increase
		// after garbage collection and decrease as new objects are
		// created.
		long heapFreeSize = Runtime.getRuntime().freeMemory();

		System.out.println("Heap size: current/max/free: " + heapSize + "/" + heapMaxSize + "/" + heapFreeSize);

	    }
	    String url = nextLine.substring(0, nextLine.indexOf('\t'));
	    String doc = nextLine.substring(nextLine.indexOf('\t') + 1, nextLine.length());
	    builder.startDocument(url, url); // both title and uri are url
	    builder.startTextField();
	    // Parse using FastBufferedReader
	    FastBufferedReader fbr = new FastBufferedReader(new StringReader(doc));
	    MutableString word = new MutableString(""), nonWord = new MutableString("");

	    while (fbr.next(word, nonWord)) {
		builder.add(word, nonWord);
	    }
	    builder.endTextField();
	    builder.endDocument();

	}

	@Override
	public void cleanup(Context context) {
	    try {
		builder.close(outputDir, fs);
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

    }

    private void printUsage() {
	System.out.println("Usage : BySubjectCollectionBuilder <input_dir> <output_dir>");
    }

    private static final String OUTPUT_DIR = "OUTPUT_DIR";

    public int run(String[] args) throws Exception {

	if (args.length < 2) {
	    printUsage();
	    return 1;
	}

	Job job = new Job(getConf());

	job.setJarByClass(BySubjectCollectionBuilder.class);
	job.setJobName("BySubjectCollectionBuilder" + System.currentTimeMillis());

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	job.setMapperClass(MapClass.class);
	job.setNumReduceTasks(0);

	FileInputFormat.addInputPath(job, new Path(args[0]));

	FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp/"));

	job.getConfiguration().set(OUTPUT_DIR, args[1]);

	boolean success = job.waitForCompletion(true);
	return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new BySubjectCollectionBuilder(), args);
	System.exit(ret);
    }

}
