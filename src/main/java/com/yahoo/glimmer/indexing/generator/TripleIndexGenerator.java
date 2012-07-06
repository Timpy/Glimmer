package com.yahoo.glimmer.indexing.generator;

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

import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;
import com.yahoo.glimmer.indexing.HorizontalDocumentFactory;
import com.yahoo.glimmer.indexing.RDFDocumentFactory;
import com.yahoo.glimmer.indexing.RDFInputFormat;
import com.yahoo.glimmer.indexing.VerticalDocumentFactory;

/**
 * Generate an inverted index from an input of <url, docfeed> pairs using MG4J
 */
public class TripleIndexGenerator extends Configured implements Tool {
    private static final String METHOD_ARG = "method";
    private static final String METHOD_ARG_VALUE_VERTICAL = "vertical";
    private static final String METHOD_ARG_VALUE_HORIZONTAL = "horizontal";
    private static final String PREDICATES_ARG = "properties";
    // Job configuration attribute names
    static final String OUTPUT_DIR = "OUTPUT_DIR";
    static final String NUMBER_OF_DOCUMENTS = "NUMBER_OF_DOCUMENTS";
    static final int MAX_POSITIONLIST_SIZE = 1000000;
    static final int MAX_INVERTEDLIST_SIZE = 50000000;
    static final String ALIGNMENT_INDEX_NAME = "alignment";
    
    private static final String RESOURCES_HASH_ARG = "RESOURCES_HASH";
    
    static { // register comparator
	WritableComparator.define(TermOccurrencePair.class, new TermOccurrencePair.Comparator());
    }
    
    public int run(String[] arg) throws Exception {
	SimpleJSAP jsap = new SimpleJSAP(TripleIndexGenerator.class.getName(), "Generates a keyword index from RDF data.", new Parameter[] {
		new FlaggedOption(METHOD_ARG, JSAP.STRING_PARSER, "horizontal", JSAP.REQUIRED, 'm', METHOD_ARG, "horizontal or vertical."),
		new FlaggedOption(PREDICATES_ARG, JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', PREDICATES_ARG,
			"Subset of the properties to be indexed."),

		new UnflaggedOption("input", JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location for the input data."),
		new UnflaggedOption("numdocs", JSAP.INTEGER_PARSER, JSAP.REQUIRED, "Number of documents to index"),
		new UnflaggedOption("output", JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location for the output."),
		new UnflaggedOption(RESOURCES_HASH_ARG, JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location of the resources hash file."),

	});

	JSAPResult args = jsap.parse(arg);

	// check whether the command line was valid, and if it wasn't,
	// display usage information and exit.
	if (!args.success()) {
	    System.err.println();
	    System.err.println("Usage: java " + TripleIndexGenerator.class.getName());
	    System.err.println("                " + jsap.getUsage());
	    System.err.println();
	    System.exit(1);
	}

	Job job = new Job(getConf());

	job.setJarByClass(TripleIndexGenerator.class);

	job.setJobName("TripleIndexGenerator" + System.currentTimeMillis());

	job.setInputFormatClass(RDFInputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.setMapperClass(DocumentMapper.class);
	job.setReducerClass(TermOccurrencePairReduce.class);

	job.setMapOutputKeyClass(TermOccurrencePair.class);
	job.setMapOutputValueClass(Occurrence.class);

	job.setPartitionerClass(TermOccurrencePair.FirstPartitioner.class);
	job.getConfiguration().setClass("mapred.output.key.comparator.class", TermOccurrencePair.Comparator.class, WritableComparator.class);
	job.getConfiguration().set("mapreduce.user.classpath.first", "true");
	job.setGroupingComparatorClass(TermOccurrencePair.FirstGroupingComparator.class);

	//DistributedCache.addCacheFile(new URI(args.getString(RESOURCES_HASH_ARG)), job.getConfiguration());
	job.getConfiguration().set(RDFDocumentFactory.RESOURCES_FILENAME_KEY, args.getString(RESOURCES_HASH_ARG));

	job.getConfiguration().setInt(NUMBER_OF_DOCUMENTS, args.getInt("numdocs"));

	FileInputFormat.setInputPaths(job, new Path(args.getString("input")));
	
	job.getConfiguration().set(OUTPUT_DIR, args.getString("output"));
	FileOutputFormat.setOutputPath(job, new Path(args.getString("output")));

	if (args.getString(METHOD_ARG).equalsIgnoreCase(METHOD_ARG_VALUE_HORIZONTAL)) {
	    job.getConfiguration().setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, HorizontalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	} else if (args.getString(METHOD_ARG).equalsIgnoreCase(METHOD_ARG_VALUE_VERTICAL)){
	    job.getConfiguration().setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, VerticalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	    if (!args.contains(PREDICATES_ARG)) {
		throw new IllegalArgumentException("When '" + METHOD_ARG + "' is '" + METHOD_ARG_VALUE_VERTICAL + "' you have to give a predicates file too.");
	    }
	    job.getConfiguration().set(VerticalDocumentFactory.PREDICATES_FILENAME_KEY, args.getString(PREDICATES_ARG));
	} else {
	    throw new IllegalArgumentException(METHOD_ARG + " should be '" + METHOD_ARG_VALUE_HORIZONTAL + "' or '" + METHOD_ARG_VALUE_VERTICAL + "'");

	}

	job.getConfiguration().setInt("mapred.linerecordreader.maxlength", 10000);

	boolean success = job.waitForCompletion(true);

	return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new TripleIndexGenerator(), args);
	System.exit(ret);
    }
}
