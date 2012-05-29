package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.CompressionFlags;
import it.unimi.dsi.mg4j.index.CompressionFlags.Coding;
import it.unimi.dsi.mg4j.index.CompressionFlags.Component;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.mg4j.index.SkipBitStreamIndexWriter;
import it.unimi.dsi.util.Properties;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

/**
 * Generate an inverted index from an input of <url, term> pairs using MG4J
 */

public class WordIndexGenerator extends Configured implements Tool {

	static enum Counters {
		NUMBER_OF_DOCS, NUMBER_OF_OCCURRENCES
	}

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void configure(JobConf job) {

		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();

			// Report progress
			reporter.setStatus(value.toString().substring(0, 20));

			String[] parts = line.split("\t");

			if (parts.length < 2) {
				return;
			}

			FastBufferedReader fbr = new FastBufferedReader(new StringReader(
					value.toString()));
			MutableString word = new MutableString(""), nonWord = new MutableString(
					"");
			while (fbr.next(word, nonWord)) {
				if (word != null && !word.equals("")) {
					// First part is docid, second part is term
					output.collect(new Text(word.toString()), new IntWritable(Integer
							.parseInt(parts[0])));
					reporter.incrCounter(Counters.NUMBER_OF_OCCURRENCES, 1);
				}
			}
			fbr.close();

			reporter.incrCounter(Counters.NUMBER_OF_DOCS, 1);

		}
	}

	public static class ReduceClass extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, Text> {

		private static final String INDEX_NAME = "foo";

		private static final int MAX_POSTING_LIST_SIZE = 100000;

		private PrintWriter terms;
		private OutputBitStream index, offsets;
		private OutputStream properties;
		private SkipBitStreamIndexWriter skipBitStreamIndexWriter;

		@Override
		public void configure(JobConf job) {
			// Get the cached archives/files

			String outputDir = job.get(OUTPUT_DIR);
			if (!outputDir.endsWith("/"))
				outputDir = outputDir + "/";
			outputDir += "index/";

			FsPermission allPermissions = new FsPermission(FsAction.ALL,
					FsAction.ALL, FsAction.ALL);
			try {
				FileSystem fs = FileSystem.get(job);
				String uuid = UUID.randomUUID().toString();
				Path path = new Path(outputDir + uuid);
				if (!fs.exists(path)) {
					fs.mkdirs(path);
					fs.setPermission(path, allPermissions);

				}

				Path indexPath = new Path(outputDir + uuid + "/" + INDEX_NAME
						+ DiskBasedIndex.INDEX_EXTENSION);
				index = new OutputBitStream(fs.create(indexPath, true));// overwrite
				fs.setPermission(indexPath, allPermissions);

				Path termsPath = new Path(outputDir + uuid + "/" + INDEX_NAME
						+ DiskBasedIndex.TERMS_EXTENSION);
				terms = new PrintWriter(new BufferedWriter(
						new OutputStreamWriter(fs.create(termsPath, true),
								"UTF-8")));// overwrite
				fs.setPermission(termsPath, allPermissions);

				Path offsetsPath = new Path(outputDir + uuid + "/" + INDEX_NAME
						+ DiskBasedIndex.OFFSETS_EXTENSION);
				offsets = new OutputBitStream(fs.create(offsetsPath, true));// overwrite
				fs.setPermission(offsetsPath, allPermissions);
				
				Path propertiesPath = new Path(outputDir + uuid + "/" + INDEX_NAME + DiskBasedIndex.PROPERTIES_EXTENSION);
				properties = fs.create(propertiesPath, true);//overwrite
				fs.setPermission(propertiesPath, allPermissions);

				Map<Component, Coding> defaultStandardIndex = new Object2ObjectOpenHashMap<Component, Coding>(
						CompressionFlags.DEFAULT_STANDARD_INDEX);
				defaultStandardIndex.remove(Component.POSITIONS);
				defaultStandardIndex.remove(Component.COUNTS);
				// null for positions means no position indexing
				skipBitStreamIndexWriter = new SkipBitStreamIndexWriter(index,
						offsets, null, job.getInt(NUMBER_OF_DOCUMENTS, -1),
						1024 * 1024 * 32, defaultStandardIndex, 16, 10);

			} catch (IOException e) {

				throw new RuntimeException(e);
			}
		}

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// We need to store the values temporarily to be able to get the
			// size and sort by docid
			int count = 0;
			List<Integer> documentIDs = new ArrayList<Integer>();

			while (values.hasNext()) {
				// Abort if larger than a certain size, because we will not be
				// able to sort in memory
				if (count++ > MAX_POSTING_LIST_SIZE) {
					System.err.println("Posting list is too big for key:"
							+ key.toString());
					return;
				}
				documentIDs.add(values.next().get());
			}

			// Sort by document ID
			java.util.Collections.sort(documentIDs);
			
			System.out.print(key.toString() + ":" );
			for (int i = 0; i < documentIDs.size(); i++) {
				System.out.print(documentIDs.get(i) + ",");
			}
			System.out.println();
			

			terms.println(key.toString());
			skipBitStreamIndexWriter.newInvertedList();
			skipBitStreamIndexWriter.writeFrequency(documentIDs.size());

			for (int i = 0; i < documentIDs.size(); i++) {
				OutputBitStream out = skipBitStreamIndexWriter
						.newDocumentRecord();
				skipBitStreamIndexWriter.writeDocumentPointer(out, documentIDs
						.get(i));
			}

			reporter.setStatus("output " + key);
		}

		@Override
		public void close() throws IOException {
			try {
				Properties props = skipBitStreamIndexWriter.properties();
				props.setProperty("occurrences", -1);
				props.setProperty("maxcount", -1);
				props.setProperty("field", "text");
				props.setProperty("termprocessor",
						"it.unimi.dsi.mg4j.index.NullTermProcessor");
				props.save(properties);
			} catch (ConfigurationException e) {
				throw new IOException(e.getMessage());
			}

			terms.close();
			skipBitStreamIndexWriter.close();
			super.close();
		}

	}

	private static final String NUMBER_OF_DOCUMENTS = "NUMBER_OF_DOCUMENTS";
	private static final int LARGE_NUMBER = 100000000;
	private static final String OUTPUT_DIR = "OUTPUT_DIR";


	private void printUsage() {
		System.out
				.println("Usage : WordIndexGenerator <input_dir> <output_dir>");
	}

	/**
   * 
   */
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			printUsage();
			return 1;
		}

		JobConf job = new JobConf(getConf(), WordIndexGenerator.class);
		job.setJobName("WordIndexGenerator" + System.currentTimeMillis());

		job.setInputFormat(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setMapOutputValueClass(IntWritable.class);

		// Here we would need to pass in the number of documents, which we can
		// get using mph.size()
		job.setInt(NUMBER_OF_DOCUMENTS, LARGE_NUMBER);
		job.set(OUTPUT_DIR, args[1]);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
		int ret = ToolRunner.run(new WordIndexGenerator(), args);
		System.exit(ret);
	}
}
