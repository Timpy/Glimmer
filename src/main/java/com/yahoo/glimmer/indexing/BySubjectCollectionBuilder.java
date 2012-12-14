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

import it.unimi.di.mg4j.document.DocumentCollectionBuilder;
import it.unimi.di.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.mg4j.io.HadoopFileSystemIOFactory;
import it.unimi.di.mg4j.io.IOFactory;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.yahoo.glimmer.util.BySubjectRecord;

public class BySubjectCollectionBuilder extends Configured implements Tool {
    static class BuilderOutputWriter extends RecordWriter<LongWritable, Text> {
	private static final String COLLECTION_PREFIX = "collection-";
	private static final MutableString TAB_WORD = new MutableString("\t");

	private BySubjectRecord bySubjectRecord = new BySubjectRecord();
	private final MutableString word = new MutableString();
	private final MutableString nonWord = new MutableString();
	FastBufferedReader fbr = new FastBufferedReader();
	
	private int count;

	private final DocumentCollectionBuilder builder;

	public BuilderOutputWriter(TaskAttemptContext job, Path taskWorkPath) throws IllegalArgumentException, IOException {
	    Path outputPath = FileOutputFormat.getOutputPath(job);
	    String collectionBase = new Path(outputPath, COLLECTION_PREFIX).toString();

	    FileSystem fs = FileSystem.get(job.getConfiguration());
	    IOFactory ioFactory = new HadoopFileSystemIOFactory(fs);
	    builder = new StartOffsetDocumentCollectionBuilder(collectionBase, new IdentityDocumentFactory(), ioFactory);
	    // Use the id for this task. It's the same for all attempts of this
	    // task.
	    String suffix = String.format("%05d", job.getTaskAttemptID().getTaskID().getId());
	    builder.open(suffix);
	}
	
	// Test constructor
	public BuilderOutputWriter(DocumentCollectionBuilder builder) throws IllegalArgumentException, IOException {
	    this.builder = builder;
	}

	@Override
	public void write(LongWritable key, Text value) throws IOException, InterruptedException {
	    if (count % 100000 == 0) {
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
	    count++;
	    
	    if (!bySubjectRecord.parse(value.getBytes(), 0, value.getLength())) {
		throw new IllegalArgumentException("Failed to parse subject doc:\n" + value.toString());
	    }

	    /* As the doc id's are the same as the values from the ALL
	    * resources hash they are not consecutive.
	    * We need to create 'empty docs' for the ALL resource hash values
	    * that aren't subjects.
	    * 
	    * Also, as the BySubject input is split. Each split ends up as a sub collection.
	    * The doc id's per sub collection are renumbered to start at 0. So for the first
	    * record of the split we compute the docId 'offset' to the last record of the previous split.
	    * All docId's in the split are then offset so that when the sub collections are loaded into a ConcatinatedCollection the doc's are stored under the correct id
	    * 
	    */
	    for (int emptyDocId = bySubjectRecord.getPreviousId() + 1 ; emptyDocId < bySubjectRecord.getId() ; emptyDocId++) {
		// write empty doc.
		builder.startDocument("", "");
		builder.endDocument();
	    }

	    // Start non-empty doc.
	    builder.startDocument(bySubjectRecord.getSubject(), bySubjectRecord.getSubject());
	    builder.startTextField();
	    
	    addField(Integer.toString(bySubjectRecord.getId()));
	    addField(Integer.toString(bySubjectRecord.getPreviousId()));
	    addField(bySubjectRecord.getSubject());
	    
	    fbr.setReader(bySubjectRecord.getRelationsReader());
	    while (fbr.next(word, nonWord)) {
		builder.add(word, nonWord);
	    }

	    // End Doc.
	    builder.endTextField();
	    builder.endDocument();
	}
	
	private void addField(CharSequence value) throws IOException {
	    word.setLength(0);
	    word.append(value);
	    builder.add(word, TAB_WORD);
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

    private static class BuilderOutputFormat extends FileOutputFormat<LongWritable, Text> {
	@Override
	public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext task) throws IOException, InterruptedException {
	    FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(task);
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

	Job job = Job.getInstance(getConf());

	job.setJarByClass(BySubjectCollectionBuilder.class);
	job.setJobName("BySubjectCollectionBuilder" + System.currentTimeMillis());

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);
	job.setOutputFormatClass(BuilderOutputFormat.class);

	job.setNumReduceTasks(0);

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
