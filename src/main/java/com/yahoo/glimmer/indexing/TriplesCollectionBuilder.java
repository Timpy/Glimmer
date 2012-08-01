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

import it.unimi.dsi.lang.MutableString;
import it.unimi.di.mg4j.document.IdentityDocumentFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class TriplesCollectionBuilder extends Configured implements Tool {

    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    try {
		Node[] nodes = NxParser.parseNodes(line);
		Node context = nodes[3];

		// Remove tabs if any: we use it as line separator
		line = line.replaceAll("\t", " ");

		output.collect(new Text(context.toString()), new Text(line + "\t"));

	    } catch (ParseException e) {
		System.out.println(line);
		e.printStackTrace();
	    } catch (RuntimeException e) {
		System.out.println(line);
		e.printStackTrace();
	    }

	}

    }

    public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	private static final MutableString TAB = new MutableString("\t");
	private static final MutableString DOT = new MutableString(".");
	private static final MutableString NEWLINE = new MutableString("\n");
	private static final int MAX_TRIPLES_PER_DOCUMENT = 1000;

	private FileSystem fs;

	private SimpleCompressedDocumentCollectionBuilder collection;

	private String outputDir;

	@Override
	public void configure(JobConf job) {

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
		collection = new SimpleCompressedDocumentCollectionBuilder("triples-", new IdentityDocumentFactory(), true);
		// Use a UUID as suffix
		String uuid = UUID.randomUUID().toString();
		collection.open(outputDir, uuid, fs);

	    } catch (IOException e) {

		throw new RuntimeException(e);
	    }

	}

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	    collection.startDocument(key.toString(), key.toString()); // title
								      // and
								      // uri
								      // are
								      // both
								      // the
								      // URI
	    int count = 0;
	    while (values.hasNext()) {
		if (count++ > MAX_TRIPLES_PER_DOCUMENT) {
		    System.err.println("Document has more than " + MAX_TRIPLES_PER_DOCUMENT + " triples per document: " + key);
		    break;
		}
		// Each value is an N-Quad
		try {
		    Node[] nodes = NxParser.parseNodes(values.next().toString());
		    collection.startTextField();
		    for (Node node : nodes) {
			collection.add(new MutableString(node.toString()), TAB);
		    }
		    collection.add(DOT, NEWLINE);
		    collection.endTextField();
		} catch (ParseException e) {
		    // not likely, because we also parsed it in the mapper
		    e.printStackTrace();
		}
	    }
	    collection.endDocument();

	}

	@Override
	public void close() {
	    try {
		collection.close(outputDir, fs);
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}

    }

    /*
     * public static class ReduceClass extends MapReduceBase implements
     * Reducer<Text, Text, Text, Text> {
     * 
     * public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,
     * Text> output, Reporter reporter) throws IOException { String result = "";
     * while (values.hasNext()) { result += values.next().toString(); }
     * 
     * output.collect(key, new Text(result)); } }
     */

    private void printUsage() {
	System.out.println("Usage : TriplesCollectionBuilder <input_dir> <output_dir>");
    }

    private static final String OUTPUT_DIR = "OUTPUT_DIR";

    public int run(String[] args) throws Exception {

	if (args.length < 2) {
	    printUsage();
	    return 1;
	}

	JobConf job = new JobConf(getConf(), TriplesCollectionBuilder.class);
	job.setJobName("TriplesCollectionBuilder" + System.currentTimeMillis());

	job.setInputFormat(TextInputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setOutputFormat(TextOutputFormat.class);

	job.setMapperClass(MapClass.class);
	job.setReducerClass(ReduceClass.class);

	job.setMapOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));

	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	FileOutputFormat.setCompressOutput(job, true);
	FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

	job.set(OUTPUT_DIR, args[1]);

	JobClient.runJob(job);

	return 0;
    }

    /**
     * Launcher for the Dump PreProcessing job.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new TriplesCollectionBuilder(), args);
	System.exit(ret);
    }
}
