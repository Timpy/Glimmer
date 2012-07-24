package com.yahoo.glimmer.util;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

public class MergeSortTool extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MergeSortTool.class);
    private static final String OUTPUT_ARG = "output";
    private static final String INPUT_ARG = "input";
    private static final String COUNT_ARG = "count";

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new MergeSortTool(), args);
	System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {

	SimpleJSAP jsap = new SimpleJSAP(MergeSortTool.class.getName(), "Merges alpha numerically sorted text files on HDFS", new Parameter[] {
	    new FlaggedOption(INPUT_ARG, JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', INPUT_ARG, "input filenames glob eg. .../part-r-?????/sortedlines.text"),
	    new FlaggedOption(OUTPUT_ARG, JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', OUTPUT_ARG, "output filename"),
	    new FlaggedOption(COUNT_ARG, JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', COUNT_ARG, "optionally create a file containing a count of the number of lines merged in text"),
	});

	JSAPResult jsapResult = jsap.parse(args);
	if (!jsapResult.success()) {
	    System.err.print(jsap.getUsage());
	    System.exit(1);
	}

	// FileSystem fs = FileSystem.get(getConf());
	// CompressionCodecFactory factory = new
	// CompressionCodecFactory(getConf());
	// mergeSort(fs, sourcePaths, outputPath, factory);

	// Maybe quicker to use a MR job with one reducer.. Currently
	// decompression, merge and compression are all done in this thread..

	Path inputGlobPath = new Path(jsapResult.getString(INPUT_ARG));
	
	Configuration config = getConf();
	FileSystem fs = FileSystem.get(config);
	
	FileStatus[] sources = fs.globStatus(inputGlobPath);

	if (sources.length == 0) {
	    System.err.println("No files matching input glob:" + inputGlobPath.toString());
	    return 1;
	}
	
	List<Path> sourcePaths = new ArrayList<Path>(sources.length);
	for (FileStatus source : sources) {
	    if (source.isDir()) {
		System.err.println(source.getPath().toString() + " is a directory.");
		return 1;
	    }
	    sourcePaths.add(source.getPath());
	}
	
	Path outputPath = new Path(jsapResult.getString(OUTPUT_ARG));

	CompressionCodecFactory factory = new CompressionCodecFactory(config);

	FSDataOutputStream countsOutputStream = null;
	if (jsapResult.contains(COUNT_ARG)) {
	    Path countsPath = null;
	    countsPath = new Path(jsapResult.getString(COUNT_ARG));
	    countsOutputStream = fs.create(countsPath);
	}

	int lineCount = MergeSortTool.mergeSort(fs, sourcePaths, outputPath, factory);
	System.out.println("Merged " + lineCount + " lines into " + outputPath.toString());
	if (countsOutputStream != null) {
	    countsOutputStream.writeBytes("" + lineCount + '\n');
	}
	countsOutputStream.flush();
	countsOutputStream.close();

	return 0;
    }

    public static int mergeSort(FileSystem fs, List<Path> sourcePaths, Path outputPath, CompressionCodecFactory compressionCodecFactory) throws IOException {
	assert sourcePaths.size() > 0 : "No source paths given.";

	LOG.info("Sorted merge into " + outputPath.toString());
	OutputStream outputStream = fs.create(outputPath);

	CompressionCodec inputCompressionCodec = compressionCodecFactory.getCodec(sourcePaths.get(0));
	if (inputCompressionCodec != null) {
	    LOG.info("Input compression codec " + inputCompressionCodec.getClass().getName());
	}

	CompressionCodec outputCompressionCodec = compressionCodecFactory.getCodec(outputPath);
	if (outputCompressionCodec != null) {
	    LOG.info("Output compression codec " + outputCompressionCodec.getClass().getName());
	    outputStream = outputCompressionCodec.createOutputStream(outputStream);
	}

	List<BufferedReader> readers = new ArrayList<BufferedReader>();
	OutputStreamWriter writer = new OutputStreamWriter(outputStream);

	for (Path partPath : sourcePaths) {
	    LOG.info("\tAdding source " + partPath.toString());
	    InputStream inputStream = fs.open(partPath);
	    if (inputCompressionCodec != null) {
		inputStream = inputCompressionCodec.createInputStream(inputStream);
	    }
	    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	    readers.add(reader);
	}

	int count = ReadersWriterMergeSort.mergeSort(readers, writer);

	writer.close();
	for (BufferedReader reader : readers) {
	    reader.close();
	}
	readers.clear();
	LOG.info("Processed " + count + " lines into " + outputPath.toString());
	return count;
    }
}
