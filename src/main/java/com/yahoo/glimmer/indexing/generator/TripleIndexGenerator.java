package com.yahoo.glimmer.indexing.generator;

import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
		new FlaggedOption("method", JSAP.STRING_PARSER, "horizontal", JSAP.REQUIRED, 'm', "method", "horizontal or vertical."),
		new FlaggedOption("properties", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "properties",
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

	job.setPartitionerClass(FirstPartitioner.class);
	job.getConfiguration().setClass("mapred.output.key.comparator.class", TermOccurrencePair.Comparator.class, WritableComparator.class);
	job.getConfiguration().set("mapreduce.user.classpath.first", "true");
	job.setGroupingComparatorClass(FirstGroupingComparator.class);

	DistributedCache.addCacheFile(new URI(args.getString(RESOURCES_HASH_ARG)), job.getConfiguration());

	job.getConfiguration().setInt(NUMBER_OF_DOCUMENTS, args.getInt("numdocs"));

	FileInputFormat.setInputPaths(job, new Path(args.getString("input")));
	
	job.getConfiguration().set(OUTPUT_DIR, args.getString("output"));
	FileOutputFormat.setOutputPath(job, new Path(args.getString("output")));

	if (args.getString("method").equalsIgnoreCase("horizontal")) {
	    job.getConfiguration().setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, HorizontalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	} else {
	    job.getConfiguration().setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, VerticalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	}

	if (args.getString("properties") != null) {
	    job.getConfiguration().set(RDFDocumentFactory.INDEXEDPROPERTIES_FILENAME_KEY, args.getString("properties"));
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
