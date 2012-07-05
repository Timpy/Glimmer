package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentFactory;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.mg4j.document.DocumentFactory.FieldType;
import it.unimi.dsi.mg4j.tool.Scan.VirtualDocumentFragment;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedCollectionBuilder extends Configured implements Tool {
    private static enum Counters {
	NUMBER_OF_RECORDS
    }
    
    public static class MapClass extends Mapper<LongWritable, Document, Text, Text> {

	private SimpleCompressedDocumentCollectionBuilder builder;
	private DocumentFactory factory;

	@Override
	public void setup(Context context) {
	    Configuration job = context.getConfiguration();

	    String outputDir = job.get(OUTPUT_DIR);
	    if (!outputDir.endsWith("/"))
		outputDir = outputDir + "/";

	    try {

		FileSystem fs = FileSystem.get(job);
		Path path = new Path(outputDir);
		if (!fs.exists(path)) {
		    fs.mkdirs(path);
		    FsPermission allPermissions = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
		    fs.setPermission(path, allPermissions);
		}

		// Create an instance of the factory that was used...we only
		// need this to get the number of fields
		Class<?> documentFactoryClass = job.getClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, RDFDocumentFactory.class);
		factory = RDFDocumentFactory.buildFactory(documentFactoryClass, context);

		// basename is actually the complete path
		builder = new SimpleCompressedDocumentCollectionBuilder("datarss-", factory, false);
		// Use a UUID as suffix
		String uuid = UUID.randomUUID().toString();
		builder.open(outputDir, uuid, fs);
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }

	}

	// Similar to SimpleCompressedDocumentBuilder.build() but we don't have
	// a Sequence
	// and our WordArrayReader doesn't support setReader
	@Override
	public void map(LongWritable key, Document document, Context context) throws IOException {

	    System.out.println("Processing: " + document.uri());

	    int numberOfFields = factory.numberOfFields();

	    MutableString word = new MutableString();
	    MutableString nonWord = new MutableString();

	    builder.startDocument(document.title(), document.uri());
	    for (int field = 0; field < numberOfFields; field++) {
		WordReader content = (WordReader) document.content(field);

		if (factory.fieldType(field) == FieldType.TEXT) {

		    builder.startTextField();
		    while (content.next(word, nonWord))
			builder.add(word, nonWord);
		    builder.endTextField();
		} else if (factory.fieldType(field) == FieldType.VIRTUAL) {
		    @SuppressWarnings("unchecked")
		    ObjectList<VirtualDocumentFragment> objectList = (ObjectList<VirtualDocumentFragment>) content;
		    builder.virtualField(objectList);
		} else {
		    builder.nonTextField(content);
		}
	    }
	    context.getCounter(Counters.NUMBER_OF_RECORDS).increment(1);
	    document.close();
	    builder.endDocument();

	}

	@Override
	public void cleanup(Context context) throws IOException {
	    builder.close();
	}
    }

    private void printUsage() {
	System.out.println("Usage : DistributedCollectionBuilder <input_dir> <output_dir> horizontal|vertical datarss|ntuples {indexedproperties_file}");
    }

    private static final String OUTPUT_DIR = "OUTPUT_DIR";

    public int run(String[] args) throws Exception {

	if (args.length < 2) {
	    printUsage();
	    return 1;
	}

	Job job = new Job(getConf());
	;
	job.setJarByClass(DistributedCollectionBuilder.class);

	Configuration conf = job.getConfiguration();

	job.setInputFormatClass(RDFInputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.setOutputFormatClass(TextOutputFormat.class);
	job.setMapperClass(MapClass.class);

	job.setMapOutputValueClass(Text.class);

	// No reduce tasks
	job.setNumReduceTasks(0);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	// FileOutputFormat.setCompressOutput(job, true);
	// FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

	conf.set(OUTPUT_DIR, args[1]);

	// Set the document factory class: HorizontalDocumentFactory or
	// VerticalDocumentFactory
	if (args[2].equalsIgnoreCase("horizontal")) {
	    conf.setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, HorizontalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	} else {
	    conf.setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, VerticalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	}

	if (args.length > 3) {
	    DistributedCache.addCacheFile(new URI(args[3]), job.getConfiguration());
	    //conf.set(RDFDocumentFactory.INDEXEDPROPERTIES_FILENAME_KEY, args[3]);
	}

	conf.setInt("mapred.linerecordreader.maxlength", 10000);

	boolean success = job.waitForCompletion(true);

	return success ? 0 : 1;
    }

    /**
     * Launcher for the Dump PreProcessing job.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new DistributedCollectionBuilder(), args);
	System.exit(ret);
    }
}
