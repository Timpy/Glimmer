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

import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentFactory;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

public class DocSizesGenerator extends Configured implements Tool {
    private static final String METHOD_ARG = "method";
    private static final String METHOD_ARG_VALUE_VERTICAL = "vertical";
    private static final String METHOD_ARG_VALUE_HORIZONTAL = "horizontal";
    // Job configuration attribute names
    private static final String PROPERTIES_ARGS = "properties";
    private static final String RESOURCES_HASH_ARG = "resourcesHash";
    private static final String OUTPUT_DIR_ARG = "OUTPUT_DIR";
    private static final String NUMBER_OF_DOCUMENTS_ARG = "NUMBER_OF_DOCUMENTS";
    
    private final static FsPermission ALL_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    
    private static enum Counters {
	NUMBER_OF_RECORDS, INDEXED_OCCURRENCES, FAILED_PARSING
    }

    public static class DocSize implements WritableComparable<DocSize>, Cloneable {
	private int document, size;
	private static final DocSizeComparator comparator = new DocSizeComparator();

	// Hadoop needs this
	public DocSize() {
	}

	public DocSize(int document, int size) {
	    this.document = document;
	    this.size = size;
	}

	public DocSize(DocSize p) {
	    this.document = p.document;
	    this.size = p.size;
	}

	public int getDocument() {
	    return document;
	}

	public void setDocument(int document) {
	    this.document = document;
	}

	public int getSize() {
	    return size;
	}

	public void setSize(int size) {
	    this.size = size;
	}

	public void readFields(DataInput in) throws IOException {
	    document = in.readInt();
	    size = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
	    out.writeInt(document);
	    out.writeInt(size);
	}

	@Override
	public boolean equals(Object o) {
	    if (o instanceof DocSize) {
		if (((DocSize) o).document == document && ((DocSize) o).size == size) {
		    return true;
		}
	    }
	    return false;
	}

	@Override
	public int hashCode() {
	    int hash = 7;
	    hash = 31 * hash + document;
	    hash = 31 * hash + size;
	    return hash;
	}

	public String toString() {
	    return document + ":" + size;
	}

	public int compareTo(DocSize o) {
	    return comparator.compare(this, o);

	}

	public Object clone() {
	    return new DocSize(document, size);
	}

    }

    public static class DocSizeComparator implements Comparator<DocSize> {

	public int compare(DocSize o1, DocSize o2) {
	    if (o1.document < o2.document) {
		return -1;
	    } else if (o1.document > o2.document) {
		return +1;
	    } else {
		if (o1.size < o2.size) {
		    return -1;
		} else if (o1.size > o2.size) {
		    return +1;
		}
	    }
	    return 0;
	}
    }

    /*
     * TODO: custom comparator that operates on the byte level
     * 
     * @author pmika
     */
    public static class IndexDocSizePair implements WritableComparable<IndexDocSizePair> {
	private int index;
	private DocSize ds = new DocSize();

	/*
	 * Required for Hadoop
	 */
	public IndexDocSizePair() {
	}

	public IndexDocSizePair(int index, DocSize ds) {
	    this.index = index;
	    this.ds = ds;
	}

	public int getIndex() {
	    return index;
	}

	public void setIndex(int index) {
	    this.index = index;
	}

	public void readFields(DataInput in) throws IOException {
	    ds.readFields(in);
	    index = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
	    ds.write(out);
	    out.writeInt(index);
	}

	public int compareTo(IndexDocSizePair other) {
	    if (index != other.index) {
		return ((Integer) index).compareTo(other.index);
	    } else {
		return ds.compareTo(other.ds);
	    }
	}

	@Override
	public int hashCode() {
	    return 31 * ds.hashCode() + index;
	}

	@Override
	public boolean equals(Object right) {
	    if (right instanceof IndexDocSizePair) {
		IndexDocSizePair r = (IndexDocSizePair) right;
		return index == r.index && ds.equals(r.ds);
	    } else {
		return false;
	    }
	}

	public String toString() {
	    return "(" + index + "," + ds.toString() + ")";
	}
    }

    /**
     * Partition based only on the term
     */
    public static class FirstPartitioner extends HashPartitioner<IndexDocSizePair, DocSize> {
	@Override
	public int getPartition(IndexDocSizePair key, DocSize value, int numPartitions) {
	    return Math.abs(key.getIndex() * 127) % numPartitions;
	}
    }

    /**
     * Compare only the first part of the pair, so that reduce is called once
     * for each value of the first part.
     * 
     * NOTE: first part (i.e. index and term) are serialized first
     */
    public static class FirstGroupingComparator implements RawComparator<IndexDocSizePair> {
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    // Skip the first two integers
	    int intsize = Integer.SIZE / 8;
	    return WritableComparator.compareBytes(b1, s1 + intsize * 2, l1 - intsize * 2, b2, s2 + intsize * 2, l2 - intsize * 2);
	}

	public int compare(IndexDocSizePair o1, IndexDocSizePair o2) {
	    if (o1.index != o2.index) {
		return ((Integer) o1.index).compareTo(o2.index);
	    }
	    return 0;
	}
    }

    public static class MapClass extends Mapper<LongWritable, Document, IndexDocSizePair, DocSize> {
	private RDFDocumentFactory factory;

	@Override
	public void setup(Context context) {
	    Configuration job = context.getConfiguration();
	    
	    // Create an instance of the factory that was used...we only need
	    // this to get the number of fields
	    Class<?> documentFactoryClass = job.getClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, RDFDocumentFactory.class);
	    factory = RDFDocumentFactory.buildFactory(documentFactoryClass, context);
	}

	@Override
	public void map(LongWritable key, Document doc, Context context) throws IOException, InterruptedException {

	    if (doc == null || doc.uri().equals(RDFDocument.NULL_URL)) {
		// Failed parsing
		context.getCounter(Counters.FAILED_PARSING).increment(1);
		System.out.println("Document failed parsing");
		return;
	    }

	    int docID = factory.resourcesHashLookup(doc.uri().toString()).intValue();

	    if (docID < 0) {
		throw new RuntimeException("Negative DocID for URI: " + doc.uri());
	    }

	    // Iterate over all indices
	    for (int i = 0; i < factory.numberOfFields(); i++) {
		if (factory.fieldName(i).startsWith("NOINDEX")) {
		    continue;
		}

		// Iterate in parallel over the words of the indices
		MutableString term = new MutableString("");
		MutableString nonWord = new MutableString("");
		WordReader termReader = (WordReader) doc.content(i);
		int position = 0;

		while (termReader.next(term, nonWord)) {
		    // Read next property as well
		    if (term != null) {
			// Report progress
			context.setStatus(factory.fieldName(i) + "=" + term.substring(0, Math.min(term.length(), 50)));
			position++;
			context.getCounter(Counters.INDEXED_OCCURRENCES).increment(1);
		    } else {
			System.out.println("Nextterm is null");
		    }
		}
		// Position now contains the size of the current field
		if (position > 0) {
		    DocSize ds = new DocSize(docID, position);
		    context.write(new IndexDocSizePair(i, ds), ds);
		}
	    }

	    context.getCounter(Counters.NUMBER_OF_RECORDS).increment(1);

	}
    }

    public static class ReduceClass extends Reducer<IndexDocSizePair, DocSize, Text, Text> {
	private String outputDir;
	private FileSystem fs;
	private DocumentFactory factory;
	private int numdocs;

	@Override
	public void setup(Context context) {
	    Configuration job = context.getConfiguration();
	    try {
		// Create an instance of the factory that was used...we only
		// need this to get the number of fields
		Class<?> documentFactoryClass = job.getClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, RDFDocumentFactory.class);
		factory = RDFDocumentFactory.buildFactory(documentFactoryClass, context);

		// Creating the output dir if necessary
		outputDir = job.get(OUTPUT_DIR_ARG);
		if (!outputDir.endsWith("/"))
		    outputDir = outputDir + "/";

		fs = FileSystem.get(job);

		// Path path = new Path(outputDir + uuid);
		Path path = new Path(outputDir);
		if (!fs.exists(path)) {
		    fs.mkdirs(path);
		    fs.setPermission(path, ALL_PERMISSIONS);
		}

		// Number of documents
		numdocs = job.getInt(NUMBER_OF_DOCUMENTS_ARG, 0);
	    } catch (IOException e) {

		throw new RuntimeException(e);
	    }
	}

	@Override
	public void reduce(IndexDocSizePair key, Iterable<DocSize> values, Context context) throws IOException, InterruptedException {
	    if (key == null || key.equals("")) {
		return;
	    }
	    
	    System.out.println("Processing index: " + factory.fieldName(key.index));

	    // Decide which file we are going to write to
	    Path sizesPath = new Path(outputDir + factory.fieldName(key.index) + DiskBasedIndex.SIZES_EXTENSION);
	    OutputBitStream stream = new OutputBitStream(fs.create(sizesPath, true));// overwrite
	    fs.setPermission(sizesPath, ALL_PERMISSIONS);

	    long occurrences = 0;
	    int prevDocID = -1;
	    Iterator<DocSize> valueIt = values.iterator();
	    while (valueIt.hasNext()) {
		DocSize value = valueIt.next();
		if ((prevDocID + 1) < value.document) {
		    System.out.println("Writing zeroes from " + (prevDocID + 1) + " to " + value.document);
		}
		for (int i = prevDocID + 1; i < value.document; i++) {
		    stream.writeGamma(0);
		}
		stream.writeGamma(value.size);
		occurrences += value.size;
		prevDocID = value.document;
	    }
	    if ((prevDocID + 1) < numdocs) {
		System.out.println("Writing zeroes  from " + (prevDocID + 1) + " to " + numdocs);
	    }
	    for (int i = prevDocID + 1; i < numdocs; i++) {
		stream.writeGamma(0);
	    }

	    stream.close();

	    System.out.println("Total number of occurrences: " + occurrences);
	}
    }

    public int run(String[] arg) throws Exception {
	SimpleJSAP jsap = new SimpleJSAP(DocSizesGenerator.class.getName(), "Generates doc sizes from RDF data.", new Parameter[] {
		new FlaggedOption(METHOD_ARG, JSAP.STRING_PARSER, METHOD_ARG_VALUE_HORIZONTAL, JSAP.REQUIRED, 'm', METHOD_ARG, "horizontal or vertical."),
		new FlaggedOption(PROPERTIES_ARGS, JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', PROPERTIES_ARGS,
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
	    System.err.println("Usage: java " + DocSizesGenerator.class.getName());
	    System.err.println("                " + jsap.getUsage());
	    System.err.println();
	    System.exit(1);
	}

	Job job = new Job(getConf());
	job.setJarByClass(DocSizesGenerator.class);

	job.setJobName("DocSizesGenerator" + System.currentTimeMillis());

	job.setInputFormatClass(RDFInputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.setMapperClass(MapClass.class);
	job.setReducerClass(ReduceClass.class);

	job.setMapOutputKeyClass(IndexDocSizePair.class);
	job.setMapOutputValueClass(DocSize.class);

	job.setPartitionerClass(FirstPartitioner.class);

	job.setGroupingComparatorClass(FirstGroupingComparator.class);

	job.getConfiguration().set(RDFDocumentFactory.RESOURCES_FILENAME_KEY, args.getString(RESOURCES_HASH_ARG));

	job.getConfiguration().setInt(NUMBER_OF_DOCUMENTS_ARG, args.getInt("numdocs"));

	job.getConfiguration().set(OUTPUT_DIR_ARG, args.getString("output"));

	FileInputFormat.setInputPaths(job, new Path(args.getString("input")));

	FileOutputFormat.setOutputPath(job, new Path(args.getString("output") + "/docsizes-temp"));

	// Set the document factory class: HorizontalDocumentFactory or
	// VerticalDocumentFactory
	if (args.getString(METHOD_ARG).equalsIgnoreCase(METHOD_ARG_VALUE_HORIZONTAL)) {
	    job.getConfiguration().setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, HorizontalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	} else if (args.getString(METHOD_ARG).equalsIgnoreCase(METHOD_ARG_VALUE_VERTICAL)) {
	    job.getConfiguration().setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, VerticalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	    if (args.getString(PROPERTIES_ARGS) != null) {
		DistributedCache.addCacheFile(new URI(args.getString(PROPERTIES_ARGS)), job.getConfiguration());
		//job.getConfiguration().set(RDFDocumentFactory.INDEXEDPROPERTIES_FILENAME_KEY, args.getString("properties"));
	    }
	} else {
	    throw new IllegalArgumentException(METHOD_ARG + " should be '" + METHOD_ARG_VALUE_HORIZONTAL + "' or '" + METHOD_ARG_VALUE_VERTICAL + "'");
	}

	job.getConfiguration().setInt("mapred.linerecordreader.maxlength", 10000);

	boolean success = job.waitForCompletion(true);

	return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new DocSizesGenerator(), args);
	System.exit(ret);
    }
}
