package com.yahoo.glimmer.indexing.preprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;
import com.yahoo.glimmer.util.MergeSortTool;

public class TuplesTool extends Configured implements Tool {
    private static final String OUTPUT_ARG = "output";
    private static final String INPUT_ARG = "input";

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new TuplesTool(), args);
	System.exit(ret);
    }
    
    @Override
    public int run(String[] args) throws Exception {

	SimpleJSAP jsap = new SimpleJSAP(TuplesTool.class.getName(), "RDF tuples pre-processor for Glimmer", new Parameter[] {
		new UnflaggedOption(INPUT_ARG, JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location for the input data."),
		new UnflaggedOption(OUTPUT_ARG, JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location for the out data."),

	});

	JSAPResult jsapResult = jsap.parse(args);
	if (!jsapResult.success()) {
	    System.err.print(jsap.getUsage());
	    System.exit(1);
	}

	Path outputDir = new Path(jsapResult.getString(OUTPUT_ARG));
	
	Job job = new Job(getConf());

	job.setJarByClass(TuplesTool.class);

	job.setJobName(TuplesTool.class.getName() + "-part1-" + System.currentTimeMillis());
	job.setInputFormatClass(TextInputFormat.class);

	job.setMapperClass(TuplesToResourcesMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setCombinerClass(ResourcesReducer.class);
	job.setReducerClass(ResourcesReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	job.setOutputFormatClass(ResourceRecordWriter.OutputFormat.class);


	FileInputFormat.setInputPaths(job, new Path(jsapResult.getString(INPUT_ARG)));

	FileOutputFormat.setOutputPath(job, outputDir);

	if (!job.waitForCompletion(true)) {
	    System.err.print("Failed to process tuples from " + jsapResult.getString(INPUT_ARG));
	    return 1;
	}
	
	// We now have:
	// 	One file per reducer containing lists of urls(recourses) for subjects, predicates, objects and contexts.
	// 	One file per reducer that contains all resources.  subjects + predicates + objects + contexts.
	// 	One file per reducer that contains the subjects + all <predicate> <object>|"Literal" <context> on that subject.
	// All the files are sorted but we need to merge each reducers output into one file that is also sorted.
	
	FileSystem fs = FileSystem.get(getConf());
	
	Map<String, List<Path>> filenamesToPartPaths = new HashMap<String, List<Path>>();
	
	FileStatus[] outputPartStatuses = fs.listStatus(outputDir, new Utils.OutputFileUtils.OutputFilesFilter());
	for (FileStatus outputPartStatus : outputPartStatuses) {
	    FileStatus[] outputFileStatuses = fs.listStatus(outputPartStatus.getPath());
	    for (FileStatus outputFileStatus : outputFileStatuses) {
		String fullFilename = outputFileStatus.getPath().toString();
		String filename = fullFilename.substring(fullFilename.lastIndexOf('/') + 1);
		
		List<Path> partPaths = filenamesToPartPaths.get(filename);
		if (partPaths == null) {
		    partPaths = new ArrayList<Path>();
		    filenamesToPartPaths.put(filename, partPaths);
		}
		partPaths.add(outputFileStatus.getPath());
	    }
	}
	
	CompressionCodecFactory factory = new CompressionCodecFactory(getConf());
	
	for (String filename : filenamesToPartPaths.keySet()) {
	    Path outputPath = new Path(outputDir, filename);
	    List<Path> sourcePaths = filenamesToPartPaths.get(filename);
	    int lineCount = MergeSortTool.mergeSort(fs, sourcePaths, outputPath, factory);
	    System.out.println("Merged " + lineCount + " lines into " + filename);
	}

	return 0;
    }
}
