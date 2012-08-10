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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BySubjectCollectionBuilder extends Configured implements Tool {
    public static class MapClass extends Mapper<LongWritable, Text, MutableString, MutableString> {
	private final MutableString word = new MutableString();
	private final MutableString nonWord = new MutableString();
	private static int count;

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

	    int indexOfFirstTab = nextLine.indexOf('\t');
	    String url = nextLine.substring(0, indexOfFirstTab);
	    String doc = nextLine.substring(indexOfFirstTab + 1);
	    
	    word.append(url);
	    context.write(word, word);

	    FastBufferedReader fbr = new FastBufferedReader(new StringReader(doc));
	    while (fbr.next(word, nonWord)) {
		context.write(word, nonWord);
	    }

	    word.setLength(0);
	    context.write(word, word);
	}
    }

    private static class BuilderOutputWriter extends RecordWriter<MutableString, MutableString> {
	private final HdfsSimpleCompressedDocumentCollectionBuilder builder;
	private boolean newDoc = true;

	public BuilderOutputWriter(TaskAttemptContext job, Path taskWorkPath) throws IllegalArgumentException, IOException {
	    FileSystem fs = FileSystem.get(job.getConfiguration());
	    builder = new HdfsSimpleCompressedDocumentCollectionBuilder("collection-", new IdentityDocumentFactory(), true, fs, taskWorkPath);
	    // Use the id for this task.  It's the same for all attempts of this task.
	    builder.open(Integer.toString(job.getTaskAttemptID().getTaskID().getId()));
	}

	@Override
	public void write(MutableString key, MutableString value) throws IOException, InterruptedException {
	    if (key.length() == 0 && value.length() == 0) {
		// Start next doc.
		builder.endTextField();
		builder.endDocument();
		newDoc = true;
	    } else if (newDoc) {
		newDoc = false;
		builder.startDocument(key.toString(), value.toString());
		builder.startTextField();
	    } else {
		builder.add(key, value);
	    }
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	    try {
		builder.close();
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}
    }

    private static class BuilderOutputFormat extends FileOutputFormat<MutableString, MutableString> {
	@Override
	public RecordWriter<MutableString, MutableString> getRecordWriter(TaskAttemptContext task) throws IOException, InterruptedException {
	    FileOutputCommitter committer = (FileOutputCommitter)getOutputCommitter(task);
	    return new BuilderOutputWriter(task, committer.getWorkPath());
	}
    }

    private void printUsage() {
	System.out.println("Usage : BySubjectCollectionBuilder <input_dir> <output_dir>");
    }

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
	job.setOutputFormatClass(BuilderOutputFormat.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));

	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	boolean success = job.waitForCompletion(true);
	return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new BySubjectCollectionBuilder(), args);
	System.exit(ret);
    }
}
