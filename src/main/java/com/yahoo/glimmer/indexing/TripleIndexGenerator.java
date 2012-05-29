package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentFactory;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.mg4j.index.CompressionFlags;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.mg4j.index.SkipBitStreamIndexWriter;
import it.unimi.dsi.mg4j.index.CompressionFlags.Coding;
import it.unimi.dsi.mg4j.index.CompressionFlags.Component;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.util.Properties;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.charset.CharacterCodingException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
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

/**
 * Generate an inverted index from an input of <url, docfeed> pairs using MG4J
 */

public class TripleIndexGenerator extends Configured implements Tool {

	static enum Counters {
		FAILED_PARSING, NUMBER_OF_RECORDS, INDEXED_TRIPLES, INDEXED_OCCURRENCES, BLACKLISTED_TRIPLES, BLACKLISTED_OCCURRENCES, UNINDEXED_PREDICATE_TRIPLES, CANNOT_CREATE_FIELDNAME_TRIPLES, OBJECTPROPERTY_TRIPLES, EMPTY_DOCUMENTS, EMPTY_LINES, NEGATIVE_PREDICATE_ID, POSTINGLIST_SIZE_OVERFLOW, POSITIONLIST_SIZE_OVERFLOW, REDUCE_WRITTEN_OCCURRENCES, RDF_TYPE_TRIPLES
	}

	public final static FsPermission allPermissions = new FsPermission(
			FsAction.ALL, FsAction.ALL, FsAction.ALL);

	// Job configuration attribute names
	private static final String OUTPUT_DIR = "OUTPUT_DIR";
	private static final String NUMBER_OF_DOCUMENTS = "NUMBER_OF_DOCUMENTS";

	public static final int MAX_POSITIONLIST_SIZE = 1000000;
	public static final int MAX_INVERTEDLIST_SIZE = 50000000; // 10m docs

	public static final String RDFFORMAT_KEY = "RDFFORMAT";

	public static final String DATARSS_FORMAT = "DATARSS";

	public static final String NTUPLES_FORMAT = "NTUPLES";

	public static final String INDEXEDPROPERTIES_FILENAME_KEY = "INDEXEDPROPERTIES_FILENAME";

	public static final int ALIGNMENT_INDEX = -1; // special index for
													// alignments
	private static final String ALIGNMENT_INDEX_NAME = "alignment";

	public static class Index {

		private static final int HEIGHT = 10;

		private static final int QUANTUM = 8;

		private static final int TEMP_BUFFER_SIZE = 512 * 1024; // 512KB buffer
																// per index

		private PrintWriter terms;
		private OutputBitStream index, offsets, posNumBits;
		private OutputStream properties;
		private SkipBitStreamIndexWriter skipBitStreamIndexWriter;

		private FileSystem fs;
		private String outputDir, indexName;
		private int numDocs;

		private boolean positions;

		public Index(FileSystem fs, String outputDir, String indexName,
				int numDocs, boolean positions) {
			this.fs = fs;
			this.outputDir = outputDir;
			// It seems like MG4J doesn't like index names with the '-' char
			this.indexName = indexName.replaceAll("\\-", "_");
			this.numDocs = numDocs;
			this.positions = positions;
		}

		public void open() throws IOException {

			Path indexPath = new Path(outputDir + "/" + indexName
					+ DiskBasedIndex.INDEX_EXTENSION);
			index = new OutputBitStream(fs.create(indexPath, true));// overwrite
			fs.setPermission(indexPath, allPermissions);

			Path termsPath = new Path(outputDir + "/" + indexName
					+ DiskBasedIndex.TERMS_EXTENSION);
			terms = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
					fs.create(termsPath, true), "UTF-8")));// overwrite
			fs.setPermission(termsPath, allPermissions);

			Path offsetsPath = new Path(outputDir + "/" + indexName
					+ DiskBasedIndex.OFFSETS_EXTENSION);
			offsets = new OutputBitStream(fs.create(offsetsPath, true));// overwrite
			fs.setPermission(offsetsPath, allPermissions);

			if (positions) {
				Path posNumBitsPath = new Path(outputDir + "/" + indexName
						+ DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION);
				posNumBits = new OutputBitStream(
						fs.create(posNumBitsPath, true));// overwrite
				fs.setPermission(posNumBitsPath, allPermissions);
			}

			Path propertiesPath = new Path(outputDir + "/" + indexName
					+ DiskBasedIndex.PROPERTIES_EXTENSION);
			properties = fs.create(propertiesPath, true);// overwrite
			fs.setPermission(propertiesPath, allPermissions);

			Map<Component, Coding> defaultStandardIndex = new Object2ObjectOpenHashMap<Component, Coding>(
					CompressionFlags.DEFAULT_STANDARD_INDEX);
			if (!positions) {
				defaultStandardIndex
						.remove(CompressionFlags.Component.POSITIONS);
				defaultStandardIndex.remove(CompressionFlags.Component.COUNTS);
			}

			skipBitStreamIndexWriter = new SkipBitStreamIndexWriter(index,
					offsets, posNumBits, numDocs, TEMP_BUFFER_SIZE,
					defaultStandardIndex, QUANTUM, HEIGHT);

		}

		public PrintWriter getTermsWriter() {
			return terms;
		}

		public boolean hasPositions() {
			return positions;
		}

		public SkipBitStreamIndexWriter getIndexWriter() {
			return skipBitStreamIndexWriter;
		}

		public OutputStream getPropertiesStream() {
			return properties;
		}

		public void close(long writtenOccurrences) throws IOException {
			try {
				Properties props = skipBitStreamIndexWriter.properties();
				System.out.println("Closing index " + indexName + " which has " + props.getProperty(it.unimi.dsi.mg4j.index.Index.PropertyKeys.TERMS)  + " terms ");
				if (positions) {
					props.setProperty(
							it.unimi.dsi.mg4j.index.Index.PropertyKeys.OCCURRENCES,
							writtenOccurrences);
				}
				props.setProperty(
						it.unimi.dsi.mg4j.index.Index.PropertyKeys.MAXCOUNT, -1);
				props.setProperty(
						it.unimi.dsi.mg4j.index.Index.PropertyKeys.FIELD,
						indexName);
				/*
				 * String[] classNames = new
				 * String[RDFDocumentFactory.TERM_PROCESSORS.length]; for (int
				 * i=0; i < RDFDocumentFactory.TERM_PROCESSORS.length; i++) {
				 * classNames[i] =
				 * RDFDocumentFactory.TERM_PROCESSORS[i].getClass().getName(); }
				 */
				props.setProperty(
						it.unimi.dsi.mg4j.index.Index.PropertyKeys.TERMPROCESSOR,
						RDFDocumentFactory.TERMPROCESSOR);

				props.save(properties);
			} catch (ConfigurationException e) {
				throw new IOException(e.getMessage());
			}

			properties.close();
			terms.close();
			skipBitStreamIndexWriter.close();

		}
	}

	public static class Occurrence implements WritableComparable<Occurrence>,
			Cloneable {
		private int document, position;
		private static final OccurrenceComparator comparator = new OccurrenceComparator();

		// Hadoop needs this
		public Occurrence() {

		}

		public Occurrence(int document, int position) {
			this.document = document;
			this.position = position;
		}

		public Occurrence(Occurrence p) {
			this.document = p.document;
			this.position = p.position;
		}

		public int getDocument() {
			return document;
		}

		public void setDocument(int document) {
			this.document = document;
		}

		public int getPosition() {
			return position;
		}

		public void setPosition(int position) {
			this.position = position;
		}

		public void readFields(DataInput in) throws IOException {
			document = in.readInt();
			position = in.readInt();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(document);
			out.writeInt(position);
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof Occurrence) {
				if (((Occurrence) o).document == document
						&& ((Occurrence) o).position == position) {
					return true;
				}
			}
			return false;
		}

		@Override
		public int hashCode() {
			int hash = 7;
			hash = 31 * hash + document;
			hash = 31 * hash + position;
			return hash;
		}

		public String toString() {
			return document + ":" + position;
		}

		public int compareTo(Occurrence o) {
			return comparator.compare(this, o);

		}

		public Object clone() {
			return new Occurrence(document, position);
		}
	}

	public static class OccurrenceComparator implements Comparator<Occurrence> {

		public int compare(Occurrence o1, Occurrence o2) {
			if (o1.document < o2.document) {
				return -1;
			} else if (o1.document > o2.document) {
				return +1;
			} else {
				// o1.document == o2.document
				if (o1.position < o2.position) {
					return -1;
				} else if (o1.position > o2.position) {
					return +1;
				}
			}
			return 0;
		}

	}

	/**
	 * 
	 * @author pmika
	 * 
	 */
	public static class TermOccurrencePair implements
			WritableComparable<TermOccurrencePair> {

		private String term;
		private Occurrence occ = new Occurrence();
		private int index;

		/*
		 * Required for Hadoop
		 */
		public TermOccurrencePair() {
		}

		public TermOccurrencePair(String term, int index, Occurrence occurrence) {
			this.index = index;
			this.term = term;
			this.occ = occurrence;
		}

		public String getTerm() {
			return term;
		}

		public void setTerm(String term) {
			this.term = term;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public void readFields(DataInput in) throws IOException {
			occ.readFields(in);
			index = in.readInt();
			term = Text.readString(in);
		}

		public void write(DataOutput out) throws IOException {
			occ.write(out);
			out.writeInt(index);
			Text.writeString(out, term);
		}

		public int compareTo(TermOccurrencePair top) {
			if (!term.equals(top.term)) {
				return term.compareTo(top.term);
			} else if (index != top.index) {
				// System.out.println("TermOccurrencePair.compareTo( " +
				// this.toString() + "== " + top.toString() + ") = " +
				// occ.compareTo(top.occ));
				return ((Integer) index).compareTo(top.index);
			} else {
				return occ.compareTo(top.occ);
			}
		}

		@Override
		public int hashCode() {
			int hash = 31 * occ.hashCode() + index;
			return 31 * hash + term.hashCode();
		}

		@Override
		public boolean equals(Object right) {
			if (right instanceof TermOccurrencePair) {
				TermOccurrencePair r = (TermOccurrencePair) right;
				return term.equals(r.term) && index == r.index
						&& occ.equals(r.occ);
			} else {
				return false;
			}
		}

		public String toString() {
			return "(" + index + "," + term + "," + occ.document + ","
					+ occ.position + ")";
		}

	}

	/** A Comparator that compares serialized TermOccurrencePair. */
	public static class TermOccurrencePairComparator extends WritableComparator {

		public TermOccurrencePairComparator() {
			super(TermOccurrencePair.class, true);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// Compare the term
			// int result = Text.Comparator.compareBytes(b1, s1 +
			// 2*Integer.SIZE/8, l1 - 2*Integer.SIZE/8, b2, s2 +
			// 2*Integer.SIZE/8, l2 - 2*Integer.SIZE/8);

			try {
				// first byte of string encodes the length of the size
				int length1 = WritableUtils.decodeVIntSize(b1[s1 + 3
						* Integer.SIZE / 8]);
				int length2 = WritableUtils.decodeVIntSize(b2[s2 + 3
						* Integer.SIZE / 8]);

				String term1 = Text.decode(b1, s1 + 3 * Integer.SIZE / 8
						+ length1, l1 - (3 * Integer.SIZE / 8 + length1));
				String term2 = Text.decode(b2, s2 + 3 * Integer.SIZE / 8
						+ length2, l2 - (3 * Integer.SIZE / 8 + length2));

				int result = term1.compareTo(term2);

				if (result != 0) {
					return result;
				} else {
					// Compare the index
					int index1 = WritableComparator.readInt(b1, s1 + 2
							* Integer.SIZE / 8);
					int index2 = WritableComparator.readInt(b2, s2 + 2
							* Integer.SIZE / 8);
					if (index1 > index2) {
						return 1;
					} else if (index1 < index2) {
						return -1;
					} else {
						// Compare the doc
						int doc1 = WritableComparator.readInt(b1, s1);
						int doc2 = WritableComparator.readInt(b2, s2);
						if (doc1 > doc2) {
							return 1;
						} else if (doc1 < doc2) {
							return -1;
						} else {
							// Compare the position
							int pos1 = WritableComparator.readInt(b1, s1
									+ Integer.SIZE / 8);
							int pos2 = WritableComparator.readInt(b2, s2
									+ Integer.SIZE / 8);
							if (pos1 > pos2) {
								return 1;
							} else if (pos1 < pos2) {
								return -1;
							} else {
								return 0;
							}
						}
					}
				}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
			}
			return 0;

		}

	}

	static { // register this comparator
		WritableComparator.define(TermOccurrencePair.class,
				new TermOccurrencePairComparator());
	}

	/**
	 * Partition based only on the term
	 */
	public static class FirstPartitioner extends
			HashPartitioner<TermOccurrencePair, Occurrence> {

		@Override
		public int getPartition(TermOccurrencePair key, Occurrence value,
				int numPartitions) {
			return Math.abs(key.getTerm().hashCode() * 127) % numPartitions;
		}

	}

	/**
	 * Compare only the first part of the pair, so that reduce is called once
	 * for each value of the first part.
	 * 
	 * NOTE: first part (i.e. index and term) are serialized first
	 */
	public static class FirstGroupingComparator implements
			RawComparator<TermOccurrencePair> {

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// Skip the first two integers
			int intsize = Integer.SIZE / 8;
			return WritableComparator.compareBytes(b1, s1 + intsize * 2, l1
					- intsize * 2, b2, s2 + intsize * 2, l2 - intsize * 2);
		}

		public int compare(TermOccurrencePair o1, TermOccurrencePair o2) {
			if (!o1.term.equals(o2.term)) {
				return o1.term.compareTo(o2.term);
			} else if (o1.index != o2.index) {
				// System.out.println("FirstGroupingComparator.compareTo( " +
				// this.toString() + "== " + top.toString() + ") = " +
				// occ.compareTo(top.occ));
				return ((Integer) o1.index).compareTo(o2.index);
			}
			return 0;
		}
	}

	public static DocumentFactory initFactory(Class<?> documentFactoryClass,
			Configuration job,
			@SuppressWarnings("rawtypes") Mapper.Context context,
			boolean loadMPH) {
		DocumentFactory factory = null;

		try {
			Reference2ObjectMap<Enum<?>, Object> defaultMetadata = new Reference2ObjectArrayMap<Enum<?>, Object>();
			defaultMetadata
					.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING,
							"UTF-8");
			defaultMetadata.put(RDFDocumentFactory.MetadataKeys.RDFFORMAT,
					job.get(TripleIndexGenerator.RDFFORMAT_KEY));
			if (job.get(TripleIndexGenerator.INDEXEDPROPERTIES_FILENAME_KEY) != null) {
				defaultMetadata
						.put(RDFDocumentFactory.MetadataKeys.INDEXED_PROPERTIES_FILENAME,
								job.get(TripleIndexGenerator.INDEXEDPROPERTIES_FILENAME_KEY));

			}
			if (loadMPH) {
				FileSystem fs = FileSystem.getLocal(job);
				Path subjectsLocation = DistributedCache
						.getLocalCacheFiles(job)[0];
				@SuppressWarnings("unchecked")
				LcpMonotoneMinimalPerfectHashFunction<CharSequence> subjects = (LcpMonotoneMinimalPerfectHashFunction<CharSequence>) BinIO
						.loadObject(fs.open(subjectsLocation));
				defaultMetadata.put(
						RDFDocumentFactory.MetadataKeys.SUBJECTS_MPH, subjects);
			}

			if (context != null) {
				defaultMetadata
						.put(RDFDocumentFactory.MetadataKeys.MAPPER_CONTEXT,
								context);
			}
			Constructor<?> constr = documentFactoryClass
					.getConstructor(Reference2ObjectMap.class);
			factory = ((DocumentFactory) constr.newInstance(defaultMetadata));
		} catch (InstantiationException e1) {
			throw new RuntimeException(e1);
		} catch (IllegalAccessException e1) {
			throw new RuntimeException(e1);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return factory;
	}

	public static class MapClass extends
			Mapper<LongWritable, Document, TermOccurrencePair, Occurrence> {

		private LcpMonotoneMinimalPerfectHashFunction<CharSequence> subjects,
				predicates;

		private DocumentFactory factory;

		@SuppressWarnings("unchecked")
		public void setup(Context context) {
			Configuration job = context.getConfiguration();

			// Create an instance of the factory that was used...we only need
			// this to get the number of fields
			// Unfortunately, this means that we will read the objects mph
			Class<?> documentFactoryClass = job.getClass(
					RDFInputFormat.DOCUMENTFACTORY_CLASS,
					RDFDocumentFactory.class);
			factory = initFactory(documentFactoryClass, job, context, false);

			// Get the cached archives/files
			FSDataInputStream subjectsInput = null, predicatesInput = null;
			try {
				FileSystem fs = FileSystem.getLocal(job);
				Path subjectsLocation = DistributedCache
						.getLocalCacheFiles(job)[0];
				subjectsInput = fs.open(subjectsLocation);
				subjects = (LcpMonotoneMinimalPerfectHashFunction<CharSequence>) BinIO
						.loadObject(subjectsInput);

				if (DistributedCache.getLocalCacheFiles(job).length > 1) {
					Path predicatesLocation = DistributedCache
							.getLocalCacheFiles(job)[1];
					predicatesInput = fs.open(predicatesLocation);
					predicates = (LcpMonotoneMinimalPerfectHashFunction<CharSequence>) BinIO
							.loadObject(predicatesInput);
				}

				// Set the number of documents, will be used by the reducers
				// job.setInt(NUMBER_OF_DOCUMENTS, mph.size());
			} catch (IOException e) {

				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} finally {
				try {
					subjectsInput.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (predicatesInput != null)
					try {
						predicatesInput.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}

		}

		@Override
		public void map(LongWritable key, Document doc, Context context)
				throws IOException, InterruptedException {

			if (doc == null || doc.uri().equals(RDFDocumentFactory.NULL_URL)) {
				// Failed parsing
				context.getCounter(Counters.FAILED_PARSING).increment(1);
				System.out.println("Document failed parsing");
				return;
			}

			int docID = (int) subjects.getLong(doc.uri().toString());
			// System.out.println("Processing: " + doc.uri() + " DOCID: " +
			// docID);

			if (docID < 0) {
				throw new RuntimeException("Negative DocID for URI: "
						+ doc.uri());
			}

			// Collect the keys (term+index) of this document
			HashSet<TermOccurrencePair> keySet = new HashSet<TermOccurrencePair>();

			// First part is URL, second part is term
			// output.collect(new Text(parts[1]), new IntWritable((int)
			// mph.getLong(parts[0])));

			// Positionless occurrence to indicate the presence of a term in a
			// doc
			Occurrence fakeDocOccurrrence = new Occurrence(-1, docID);

			// Iterate over all indices
			for (int i = 0; i < factory.numberOfFields(); i++) {

				String fieldName = factory.fieldName(i);
				if (fieldName.startsWith("NOINDEX")) {
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
						context.setStatus(factory.fieldName(i)
								+ "="
								+ term.substring(0, Math.min(term.length(), 50)));

						// Create an occurrence at the next position
						Occurrence occ = new Occurrence(docID, position);
						context.write(new TermOccurrencePair(term.toString(),
								i, occ), occ);
						// Create fake occurrences for each term (this will be
						// used for counting # of docs per term
						keySet.add(new TermOccurrencePair(term.toString(), i,
								fakeDocOccurrrence));
						position++;
						context.getCounter(Counters.INDEXED_OCCURRENCES)
								.increment(1);

						if (factory instanceof VerticalDocumentFactory) {
							// Create an entry in the alignment index
								int predicateID = (int) predicates
										.getLong(fieldName);
							// System.out.println("Processing: " + doc.uri() +
							// " DOCID: " + docID);
	
							if (predicateID < 0) {
									System.err
											.println("Negative predicateID for URI: "
										+ fieldName);
									context.getCounter(
											Counters.NEGATIVE_PREDICATE_ID)
										.increment(1);
							} else {
								Occurrence predicateOcc = new Occurrence(
										predicateID, -1);
								context.write(
										new TermOccurrencePair(term.toString(),
												ALIGNMENT_INDEX, predicateOcc),
										predicateOcc);
								Occurrence fakePredicateOccurrrence = new Occurrence(
										-1, predicateID);
									keySet.add(new TermOccurrencePair(term
											.toString(), ALIGNMENT_INDEX,
											fakePredicateOccurrrence));
							}
						}


					} else {
						System.out.println("Nextterm is null");
					}
				}
			}

			context.getCounter(Counters.NUMBER_OF_RECORDS).increment(1);

			for (TermOccurrencePair term : keySet) {
				context.write(term, term.occ);
			}
			// Close document
			doc.close();

		}
	}

	public static class ReduceClass extends
			Reducer<TermOccurrencePair, Occurrence, Text, Text> {

		private Map<Integer, Index> indices = new HashMap<Integer, Index>();

		private long writtenOccurrences;

		@Override
		public void setup(Context context) {
			Configuration job = context.getConfiguration();
			try {

				// Create an instance of the factory that was used...we only
				// need this to get the number of fields
				Class<?> documentFactoryClass = job.getClass(
						RDFInputFormat.DOCUMENTFACTORY_CLASS,
						RDFDocumentFactory.class);
				DocumentFactory factory = initFactory(documentFactoryClass,
						job, null, false);

				// Creating the output dir
				String outputDir = job.get(OUTPUT_DIR);
				if (!outputDir.endsWith("/"))
					outputDir = outputDir + "/";
				outputDir += "index/";
				FileSystem fs = FileSystem.get(job);
				// Adding a UUID to the name of the outputdir to make sure
				// different mappers write to different directories
				String uuid = UUID.randomUUID().toString();
				Path path = new Path(outputDir + uuid);
				if (!fs.exists(path)) {
					fs.mkdirs(path);
					fs.setPermission(path, allPermissions);

				}

				if (factory instanceof VerticalDocumentFactory) {
					// Open the alignment index
					Index index = new Index(fs, outputDir + uuid,
							ALIGNMENT_INDEX_NAME, job.getInt(NUMBER_OF_DOCUMENTS,
									-1), false);
					index.open();
					indices.put(ALIGNMENT_INDEX, index);
				}

				// Open one index per field
				for (int i = 0; i < factory.numberOfFields(); i++) {
					String name = RDFDocumentFactory.encodeFieldName(factory
							.fieldName(i));
					if (!name.startsWith("NOINDEX")) {

						// Get current size of heap in bytes
						long heapSize = Runtime.getRuntime().totalMemory();
						// Get maximum size of heap in bytes. The heap cannot
						// grow beyond this size.
						// Any attempt will result in an OutOfMemoryException.
						long heapMaxSize = Runtime.getRuntime().maxMemory();
						// Get amount of free memory within the heap in bytes.
						// This size will increase
						// after garbage collection and decrease as new objects
						// are created.
						long heapFreeSize = Runtime.getRuntime().freeMemory();

						System.out.println("Opening index for field:" + name
								+ " Heap size: current/max/free: " + heapSize
								+ "/" + heapMaxSize + "/" + heapFreeSize);

						Index index = new Index(fs, outputDir + uuid, name,
								job.getInt(NUMBER_OF_DOCUMENTS, -1), true);
						index.open();

						indices.put(i, index);

					}
				}

			} catch (IOException e) {

				throw new RuntimeException(e);
			}
		}

		@Override
		public void reduce(TermOccurrencePair key, Iterable<Occurrence> values,
				Context context) throws IOException {

			if (key == null || key.equals(""))
				return;

			// System.out.println("Processing: " + key.term + " index: " +
			// key.index);

			context.setStatus(key.index + ":" + key.term);

			// Decide which index we are going to write to
			Index currentIndex = indices.get(key.index);

			// For every term, the first instances are fake instances introduced
			// for document counting
			boolean firstDoc = false;
			int numDocs = 0;
			Occurrence occ = null, prevOcc = null;
			Iterator<Occurrence> valueIt = values.iterator();
			while (valueIt.hasNext() && !firstDoc) {
				occ = valueIt.next();

				// System.out.println("Occurrence: " + occ + " Previous: "+
				// prevOcc);
				if (occ.document == -1) {
					if (!occ.equals(prevOcc)) {
						numDocs++;
					} else {
						// System.err.println("Duplicate document ID: " +
						// occ.position + " for term: " + key.term + " index: "
						// + key.index);
					}
					prevOcc = (Occurrence) occ.clone();
				} else {
					firstDoc = true;
				}

			}

			// Cut off the index type prefix from the key
			currentIndex.getTermsWriter().println(key.term);
			currentIndex.getIndexWriter().newInvertedList();
			// System.out.println("Setting frequency to " + numDocs);
			currentIndex.getIndexWriter().writeFrequency(
					Math.min(numDocs, MAX_INVERTEDLIST_SIZE));

			int prevDocID = occ.document;
			int[] buf = new int[MAX_POSITIONLIST_SIZE];
			int posIndex = 0;
			int writtenDocs = 0;
			while (occ != null) {

				// System.out.println("Occurrence: " + occ + " Previous: "+
				// prevOcc);

				int docID = occ.document;
				if (docID != prevDocID) {
					// New document, write out previous postings
					OutputBitStream out = currentIndex.getIndexWriter()
							.newDocumentRecord();
					currentIndex.getIndexWriter().writeDocumentPointer(out,
							prevDocID);
					if (currentIndex.hasPositions()) {
						currentIndex.getIndexWriter().writePositionCount(out,
								posIndex);
						currentIndex.getIndexWriter().writeDocumentPositions(
								out, buf, 0, posIndex, -1);
					}
					// System.out.println("Writing " + prevDocID);
					writtenDocs++;
					writtenOccurrences += posIndex;
					// context.getCounter(Counters.REDUCE_WRITTEN_OCCURRENCES).increment(posIndex);

					if (writtenDocs == MAX_INVERTEDLIST_SIZE) {
						context.getCounter(Counters.POSTINGLIST_SIZE_OVERFLOW)
								.increment(1);
						System.err.println("More than " + MAX_INVERTEDLIST_SIZE
								+ " documents for term " + key.term);
						break;
					}
					posIndex = 0;
					buf[posIndex++] = occ.position;
				} else {
					if (posIndex > MAX_POSITIONLIST_SIZE - 1) {
						context.getCounter(Counters.POSITIONLIST_SIZE_OVERFLOW)
								.increment(1);
						System.err.println("More than " + MAX_POSITIONLIST_SIZE
								+ " positions for term " + key.term);
					} else {
						buf[posIndex++] = occ.position;
					}
				}

				prevDocID = docID;
				prevOcc = (Occurrence) occ.clone();

				boolean last = false;
				if (valueIt.hasNext()) {
					occ = valueIt.next();
					// Skip equivalent occurrences
					while (occ.equals(prevOcc) && valueIt.hasNext()) {
						occ = valueIt.next();
					}
					if (occ.equals(prevOcc) && !valueIt.hasNext()) {
						last = true;
					}
				} else {
					last = true;
				}
				if (last) {
					// This is the last occurrence: write out the remaining
					// positions
					OutputBitStream out = currentIndex.getIndexWriter()
							.newDocumentRecord();
					currentIndex.getIndexWriter().writeDocumentPointer(out,
							prevDocID);
					if (currentIndex.hasPositions()) {
						currentIndex.getIndexWriter().writePositionCount(out,
								posIndex);
						currentIndex.getIndexWriter().writeDocumentPositions(
								out, buf, 0, posIndex, -1);
					}
					// System.out.println("Writing last " + prevDocID);
					writtenOccurrences += posIndex;
					// context.getCounter(Counters.REDUCE_WRITTEN_OCCURRENCES).increment(posIndex);
					occ = null;
				}

			}

		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			try {
				for (Index index : indices.values()) {
					index.close(writtenOccurrences);
				}
				super.cleanup(context);
			} catch (Throwable throwable) {
				throwable.printStackTrace();
			}
		}

	}

	public int run(String[] arg) throws Exception {

		SimpleJSAP jsap = new SimpleJSAP(
				TripleIndexGenerator.class.getName(),
				"Generates a keyword index from RDF data.",
				new Parameter[] {
						new FlaggedOption("method", JSAP.STRING_PARSER,
								"horizontal", JSAP.REQUIRED, 'm', "method",
								"horizontal or vertical."),
						new FlaggedOption("format", JSAP.STRING_PARSER,
								JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f',
								"format", "datarss or ntuples."),
						new FlaggedOption("properties", JSAP.STRING_PARSER,
								JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p',
								"properties",
								"Subset of the properties to be indexed."),

						new UnflaggedOption("input", JSAP.STRING_PARSER,
								JSAP.REQUIRED,
								"HDFS location for the input data."),
						new UnflaggedOption("numdocs", JSAP.INTEGER_PARSER,
								JSAP.REQUIRED, "Number of documents to index"),
						new UnflaggedOption("output", JSAP.STRING_PARSER,
								JSAP.REQUIRED, "HDFS location for the output."),
						new UnflaggedOption("subjects", JSAP.STRING_PARSER,
								JSAP.REQUIRED,
								"HDFS location of the MPH for subjects."),
						new UnflaggedOption("predicates", JSAP.STRING_PARSER,
								JSAP.REQUIRED,
								"HDFS location of the MPH for predicates."),

				});

		JSAPResult args = jsap.parse(arg);

		// check whether the command line was valid, and if it wasn't,
		// display usage information and exit.
		if (!args.success()) {
			System.err.println();
			System.err.println("Usage: java "
					+ TripleIndexGenerator.class.getName());
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

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setMapOutputKeyClass(TermOccurrencePair.class);
		job.setMapOutputValueClass(Occurrence.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.getConfiguration().setClass("mapred.output.key.comparator.class",
				TermOccurrencePairComparator.class, WritableComparator.class);
		job.getConfiguration().set("mapreduce.user.classpath.first", "true");
		job.setGroupingComparatorClass(FirstGroupingComparator.class);

		DistributedCache.addCacheFile(new URI(args.getString("subjects")),
				job.getConfiguration());

		DistributedCache.addCacheFile(new URI(args.getString("predicates")),
				job.getConfiguration());

		// Load the MPH to get its size
		// or otherwise we could provide it as a param
		/*
		 * FSDataInputStream input = null; try { FileSystem fs =
		 * FileSystem.get(conf); // Get the cached archives/files input =
		 * fs.open(new Path(mphLocation.toString()));
		 * LcpMonotoneMinimalPerfectHashFunction<CharSequence> mph =
		 * (LcpMonotoneMinimalPerfectHashFunction<CharSequence>)
		 * BinIO.loadObject(input); conf.setInt(NUMBER_OF_DOCUMENTS,
		 * mph.size()); System.out.println("Size of the MPH: " + mph.size()); }
		 * catch (IOException e) { throw new RuntimeException(e); } catch
		 * (ClassNotFoundException e) { throw new RuntimeException(e); } finally
		 * { if (input != null) { try { input.close(); } catch (IOException e) {
		 * throw new RuntimeException(e); } } }
		 */

		job.getConfiguration().setInt(NUMBER_OF_DOCUMENTS,
				args.getInt("numdocs"));

		job.getConfiguration().set(OUTPUT_DIR, args.getString("output"));

		FileInputFormat.setInputPaths(job, new Path(args.getString("input")));

		FileOutputFormat.setOutputPath(job, new Path(args.getString("output")));

		// Set the document factory class: HorizontalDocumentFactory or
		// VerticalDocumentFactory
		if (args.getString("method").equalsIgnoreCase("horizontal")) {
			job.getConfiguration().setClass(
					RDFInputFormat.DOCUMENTFACTORY_CLASS,
					HorizontalDocumentFactory.class,
					PropertyBasedDocumentFactory.class);
		} else {
			job.getConfiguration().setClass(
					RDFInputFormat.DOCUMENTFACTORY_CLASS,
					VerticalDocumentFactory.class,
					PropertyBasedDocumentFactory.class);
		}

		if (args.getString("format").equalsIgnoreCase("datarss")) {
			job.getConfiguration().set(RDFFORMAT_KEY, DATARSS_FORMAT);
		} else {
			job.getConfiguration().set(RDFFORMAT_KEY, NTUPLES_FORMAT);
		}

		if (args.getString("properties") != null) {
			job.getConfiguration().set(INDEXEDPROPERTIES_FILENAME_KEY,
					args.getString("properties"));
		}

		job.getConfiguration().setInt("mapred.linerecordreader.maxlength",
				10000);

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new TripleIndexGenerator(), args);
		System.exit(ret);
	}
}
