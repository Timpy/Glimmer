package com.yahoo.glimmer.util;

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
import org.apache.hadoop.conf.Configured;
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
import com.martiansoftware.jsap.UnflaggedOption;
import com.yahoo.glimmer.indexing.preprocessor.TuplesTool;

public class MergeSortTool extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MergeSortTool.class);
    private static final String SOURCES_ARG = "sources";
    private static final String OUTPUT_ARG = "output";

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new MergeSortTool(), args);
	System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {

	SimpleJSAP jsap = new SimpleJSAP(TuplesTool.class.getName(), "RDF tuples pre-processor for Glimmer", new Parameter[] {
		new FlaggedOption(OUTPUT_ARG, JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', OUTPUT_ARG, "output filename"),
		new UnflaggedOption(SOURCES_ARG, JSAP.STRING_PARSER, "", JSAP.REQUIRED, JSAP.GREEDY, "list of source files to merge.")
	});

	JSAPResult jsapResult = jsap.parse(args);
	if (!jsapResult.success()) {
	    System.err.print(jsap.getUsage());
	    System.exit(1);
	}
	
	Path outputPath = new Path(jsapResult.getString(OUTPUT_ARG));
	String[] sources = jsapResult.getStringArray(SOURCES_ARG);
	List<Path> sourcePaths = new ArrayList<Path>();
	for (String source : sources) {
	    sourcePaths.add(new Path(source));
	}
	
	FileSystem fs = FileSystem.get(getConf());
	CompressionCodecFactory factory = new CompressionCodecFactory(getConf());
	mergeSort(fs, sourcePaths, outputPath, factory);
	
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
