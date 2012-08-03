package com.yahoo.glimmer.indexing.preprocessor;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

public class PrepTool extends Configured implements Tool {
    public static final String SUBJECT_FILTER_ARG = "subjectFilter";
    public static final String PREDICATE_FILTER_ARG = "predicateFilter";
    public static final String OBJECT_FILTER_ARG = "objectFilter";
    public static final String CONTEXT_FILTER_ARG = "contextFilter";
    public static final String AND_FILTER_CONJUNCTION_ARG = "andConjunction";
    public static final String NO_CONTEXTS_ARG = "excludeContexts";
    private static final String OUTPUT_ARG = "output";
    private static final String INPUT_ARG = "input";

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new PrepTool(), args);
	System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {

	SimpleJSAP jsap = new SimpleJSAP(PrepTool.class.getName(), "RDF tuples pre-processor for Glimmer", new Parameter[] {
	    	new FlaggedOption(SUBJECT_FILTER_ARG, JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 's', SUBJECT_FILTER_ARG, "Only process tuples with their subject matching this regex."),
	    	new FlaggedOption(PREDICATE_FILTER_ARG, JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'p', PREDICATE_FILTER_ARG, "Only process tuples with their predicate matching this regex."),
	    	new FlaggedOption(OBJECT_FILTER_ARG, JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'o', OBJECT_FILTER_ARG, "Only process tuples with their object matching this regex."),
	    	new FlaggedOption(CONTEXT_FILTER_ARG, JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c', CONTEXT_FILTER_ARG, "Only process tuples with their context matching this regex."),
	    	new Switch(AND_FILTER_CONJUNCTION_ARG, 'a', AND_FILTER_CONJUNCTION_ARG, "If more than one filter is set use AND conjunction to include tuples. Default is OR."),
		new Switch(NO_CONTEXTS_ARG, 'C', NO_CONTEXTS_ARG, "Don't process the contexts for each tuple."),
		new UnflaggedOption(INPUT_ARG, JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location for the input data."),
		new UnflaggedOption(OUTPUT_ARG, JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location for the out data."),

	});

	JSAPResult jsapResult = jsap.parse(args);
	if (!jsapResult.success()) {
	    System.err.print(jsap.getUsage());
	    System.exit(1);
	}
	

	Configuration config = getConf();
	
	if (jsapResult.contains(SUBJECT_FILTER_ARG)) {
	    config.set(TuplesToResourcesMapper.SUBJECT_REGEX_KEY, jsapResult.getString(SUBJECT_FILTER_ARG));
	    System.out.println("Subject filter set to " + jsapResult.getString(SUBJECT_FILTER_ARG));
	}
	if (jsapResult.contains(PREDICATE_FILTER_ARG)) {
	    config.set(TuplesToResourcesMapper.PREDICATE_REGEX_KEY, jsapResult.getString(PREDICATE_FILTER_ARG));
	    System.out.println("Predicate filter set to " + jsapResult.getString(PREDICATE_FILTER_ARG));
	}
	if (jsapResult.contains(OBJECT_FILTER_ARG)) {
	    config.set(TuplesToResourcesMapper.OBJECT_REGEX_KEY, jsapResult.getString(OBJECT_FILTER_ARG));
	    System.out.println("Object filter set to " + jsapResult.getString(OBJECT_FILTER_ARG));
	}
	if (jsapResult.contains(CONTEXT_FILTER_ARG)) {
	    config.set(TuplesToResourcesMapper.CONTEXT_REGEX_KEY, jsapResult.getString(CONTEXT_FILTER_ARG));
	    System.out.println("Context filter set to " + jsapResult.getString(CONTEXT_FILTER_ARG));
	}
	if (jsapResult.getBoolean(AND_FILTER_CONJUNCTION_ARG, false)) {
	    config.setBoolean(TuplesToResourcesMapper.FILTER_CONJUNCTION_KEY, true);
	    System.out.println("Filter conjunction is AND");
	}
	
	boolean withContexts = !jsapResult.getBoolean(NO_CONTEXTS_ARG, false);
	config.setBoolean(TuplesToResourcesMapper.INCLUDE_CONTEXTS_KEY, withContexts);

	Job job = new Job(config);
	job.setJarByClass(PrepTool.class);

	job.setJobName(PrepTool.class.getName() + "-part1-" + System.currentTimeMillis());
	job.setInputFormatClass(TextInputFormat.class);

	job.setMapperClass(TuplesToResourcesMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	job.setReducerClass(ResourcesReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.setOutputFormatClass(ResourceRecordWriter.OutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(jsapResult.getString(INPUT_ARG)));

	Path outputDir = new Path(jsapResult.getString(OUTPUT_ARG));
	FileOutputFormat.setOutputPath(job, outputDir);

	if (!job.waitForCompletion(true)) {
	    System.err.print("Failed to process tuples from " + jsapResult.getString(INPUT_ARG));
	    return 1;
	}

	// We now have:
	// One file per reducer containing lists of urls(recourses) for
	// subjects, predicates, objects and contexts.
	// One file per reducer that contains all resources. subjects +
	// predicates + objects + contexts.
	// One file per reducer that contains the subjects + all <predicate>
	// <object>|"Literal" <context> on that subject.
	
	return 0;
    }
}
