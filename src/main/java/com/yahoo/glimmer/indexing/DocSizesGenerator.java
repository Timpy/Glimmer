package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentFactory;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.Iterator;

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

public class DocSizesGenerator extends Configured implements Tool {

 
  static enum Counters {NUMBER_OF_RECORDS, INDEXED_OCCURRENCES, FAILED_PARSING }
  
  public final static FsPermission allPermissions = new FsPermission(
		  FsAction.ALL, 
		  FsAction.ALL, 
		  FsAction.ALL); 

  //Job configuration attribute names
  private static final String OUTPUT_DIR = "OUTPUT_DIR";

  private static final String NUMBER_OF_DOCUMENTS = "NUMBER_OF_DOCUMENTS";
    

  public static class DocSize implements WritableComparable<DocSize>, Cloneable {
	private int document, size;
	private static final DocSizeComparator comparator = new DocSizeComparator();
	
	//Hadoop needs this
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
	public boolean equals (Object o) {
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
				//o1.document == o2.document
				if (o1.size < o2.size) {
					return -1;
				} else if (o1.size > o2.size) {
					return +1;
				}
			}
			return 0;
		}
		  
}
  /* TODO: custom comparator that operates on the byte level
   * 
   * @author pmika
   *
   */
  public static class IndexDocSizePair implements WritableComparable<IndexDocSizePair> {
		
		private int index;

		private DocSize ds = new DocSize(); 
	
		
		/* Required for Hadoop
		 * 
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
				//System.out.println("TermOccurrencePair.compareTo( " + this.toString() + "== " + top.toString() + ") = " + occ.compareTo(top.occ));
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
	    	return "(" + index + "," + ds.toString() +")";
	    }
	    
	  }
  
  /**
	  * Partition based only on the term
	  */
	public static class FirstPartitioner extends HashPartitioner<IndexDocSizePair,DocSize>{
		
	  @Override	
	  public int getPartition(IndexDocSizePair key, DocSize value,
	                          int numPartitions) {
	    return Math.abs(key.getIndex() * 127) % numPartitions;
	  }
	
	}
	
	/**
	* Compare only the first part of the pair, so that reduce is called once
	* for each value of the first part.
	* 
	* NOTE: first part (i.e. index and term) are serialized first 
	*/
	  public static class FirstGroupingComparator
	                implements RawComparator<IndexDocSizePair> {
	 
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	      //Skip the first two integers	
	      int intsize = Integer.SIZE/8;
	      return WritableComparator.compareBytes(b1, s1 + intsize * 2, l1 - intsize * 2,
	                                             b2, s2 + intsize * 2, l2 - intsize * 2);
	    }

		public int compare(IndexDocSizePair o1, IndexDocSizePair o2) {
			if (o1.index != o2.index) {
				//System.out.println("FirstGroupingComparator.compareTo( " + this.toString() + "== " + top.toString() + ") = " + occ.compareTo(top.occ));
				return ((Integer) o1.index).compareTo(o2.index);
			}
			return 0;
		}
	  }
	  

  public static class MapClass extends Mapper<LongWritable, Document, IndexDocSizePair, DocSize> {
    

	private Path mphLocation;
	private LcpMonotoneMinimalPerfectHashFunction<CharSequence> mph;
	
	private DocumentFactory factory;
		  	  
	@SuppressWarnings("unchecked")
	@Override
	public void setup(Context context) {
		Configuration job = context.getConfiguration();
		
		//Create an instance of the factory that was used...we only need this to get the number of fields
		Class<?> documentFactoryClass = job.getClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, RDFDocumentFactory.class);
		
		factory = TripleIndexGenerator.initFactory(documentFactoryClass, job, context, false);
		
	    // Get the cached archives/files
    	FSDataInputStream input = null;
    	try {
			FileSystem fs = FileSystem.getLocal(job);
    		mphLocation = DistributedCache.getLocalCacheFiles(job)[0];
			input = fs.open(mphLocation);
			mph = (LcpMonotoneMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(input);
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		
      }
	  
	
	  @Override
	  public void map(LongWritable key, Document doc, Context context)
	  throws IOException, InterruptedException {
		  
    
		if (doc == null || doc.uri().equals(RDFDocumentFactory.NULL_URL)) {
			//Failed parsing
			context.getCounter(Counters.FAILED_PARSING).increment(1);
			System.out.println("Document failed parsing");
			return;
		}
		
		int docID = (int) mph.getLong(doc.uri().toString());
		//System.out.println("Processing: " + doc.uri() + " DOCID: " + docID);
		
		if (docID < 0) {
			throw new RuntimeException("Negative DocID for URI: " + doc.uri());
		}
		
    	
    	//First part is URL, second part is term
    	//output.collect(new Text(parts[1]), new IntWritable((int) mph.getLong(parts[0])));        
        
		//Iterate over all indices
		for (int i=0; i < factory.numberOfFields(); i++) {
			if (factory.fieldName(i).startsWith("NOINDEX")) {
				continue;
			}
			
			//Iterate in parallel over the words of the indices
			MutableString term= new MutableString("");
			MutableString nonWord = new MutableString("");
			WordReader termReader = (WordReader) doc.content(i);
			int position = 0;
			
		 	while (termReader.next(term, nonWord)) {
		 		
		 		//Read next property as well
				if (term != null) {
						
						//Report progress
		    			context.setStatus(factory.fieldName(i) + "=" + term.substring(0,Math.min(term.length(), 50)));
		    			
		        		position++;
		        		
		        		context.getCounter(Counters.INDEXED_OCCURRENCES).increment(1);
	    			
	    		} else {
	    			System.out.println("Nextterm is null");
	    		}
	    	}
		 	//Position now contains the size of the current field
		 	if (position > 0) {
			 	DocSize ds = new DocSize(docID, position);
			 	context.write(new IndexDocSizePair(i, ds), ds );
		 	}
		}
	 
		context.getCounter(Counters.NUMBER_OF_RECORDS).increment(1);

	  }
  }
  
  public static class ReduceClass extends Reducer<IndexDocSizePair, DocSize, Text, Text> {

	  //private Vector<DataOutputStream> sizes = new Vector<DataOutputStream>();
	 
	  private String outputDir;
	  private FileSystem fs;
	  private DocumentFactory factory;
	  private int numdocs;
	  
	  @Override
	  public void setup(Context context) {
	        Configuration job = context.getConfiguration();
	    	try {
	
	    		//Create an instance of the factory that was used...we only need this to get the number of fields
	    		Class<?> documentFactoryClass = job.getClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, RDFDocumentFactory.class);
	    		factory = TripleIndexGenerator.initFactory(documentFactoryClass, job, null, false);
	    		  
	    		//Creating the output dir if necessary
	  		  	outputDir = job.get(OUTPUT_DIR);
	  		  	if (!outputDir.endsWith("/")) outputDir = outputDir + "/";

	  		  	fs = FileSystem.get(job);

				//Path path = new Path(outputDir + uuid);
				Path path = new Path(outputDir);
				if (!fs.exists(path)) {			
					fs.mkdirs(path);
					fs.setPermission(path, allPermissions);
					
				}
				
				//Number of documents
				numdocs = job.getInt(NUMBER_OF_DOCUMENTS, 0);
	    	} catch (IOException e) {

				throw new RuntimeException(e);
			} 
	      }
	  
	    @Override
		public void reduce(IndexDocSizePair key, Iterable<DocSize> values, Context context) 
			throws IOException, InterruptedException {
						 
			if (key== null || key.equals("")) return;
			
			long occurrences = 0;
			
			System.out.println("Processing index: " + factory.fieldName(key.index));

    		Path sizesPath = new Path(outputDir + factory.fieldName(key.index) + DiskBasedIndex.SIZES_EXTENSION);
			OutputBitStream stream = new OutputBitStream(fs.create(sizesPath, true));//overwrite
    		//BufferedWriter stream = new BufferedWriter(new OutputStreamWriter(fs.create(sizesPath, true)));//overwrite
			
    		fs.setPermission(sizesPath, allPermissions);
			//Decide which file we are going to write to
			//DataOutputStream stream = sizes.get(key.get());
			
			int prevDocID = -1;
			Iterator<DocSize> valueIt = values.iterator();
			while (valueIt.hasNext()) {
				DocSize value = valueIt.next();
				if ((prevDocID + 1) < value.document) {
					System.out.println("Writing zeroes from " + (prevDocID + 1) + " to " + value.document);
				}
				for (int i=prevDocID + 1; i < value.document; i++) {
					stream.writeGamma(0);
					//stream.write("0\n");
				}
				//stream.writeInt(value.document);
				stream.writeGamma(value.size);
				occurrences += value.size;
				//stream.write(value.size + "\n");
				System.out.println(value.document + ":" + value.size);
				prevDocID = value.document;
			}
			if ((prevDocID + 1) < numdocs) {
				System.out.println("Writing zeroes  from " + (prevDocID + 1) + " to " + numdocs);
			}
			for (int i=prevDocID + 1; i < numdocs; i++) {
				stream.writeGamma(0);
				//stream.write("0\n");
			}
		
			stream.close();
			
			System.out.println("Total number of occurrences: " + occurrences);
		}

	}
  

public int run(String[] arg) throws Exception {

	

    SimpleJSAP jsap = new SimpleJSAP( TripleIndexGenerator.class.getName(), "Generates a keyword index from RDF data.",
			new Parameter[] {
			new FlaggedOption( "method", JSAP.STRING_PARSER, "horizontal", JSAP.REQUIRED, 'm', "method", "horizontal or vertical." ),
			new FlaggedOption( "format", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "format", "datarss or ntuples." ),
			new FlaggedOption( "properties", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "properties", "Subset of the properties to be indexed." ),
			
			new UnflaggedOption( "input", JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location for the input data." ),
			new UnflaggedOption( "numdocs", JSAP.INTEGER_PARSER, JSAP.REQUIRED, "Number of documents to index" ),
			new UnflaggedOption( "output", JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location for the output." ),
			new UnflaggedOption( "subjects", JSAP.STRING_PARSER, JSAP.REQUIRED, "HDFS location of the MPH for subjects." ),
			
	});
	
    JSAPResult args = jsap.parse(arg);    

    // check whether the command line was valid, and if it wasn't,
    // display usage information and exit.
    if (!args.success()) {
        System.err.println();
        System.err.println("Usage: java " + DocSizesGenerator.class.getName());
        System.err.println("                "  + jsap.getUsage());
        System.err.println();
        System.exit(1);
    }

    Job job = new Job(getConf());
    job.setJarByClass(DocSizesGenerator.class); 
    
    job.setJobName("DocSizesGenerator"+System.currentTimeMillis());


    job.setInputFormatClass(RDFInputFormat.class);
         
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);

    job.setMapOutputKeyClass(IndexDocSizePair.class);
    job.setMapOutputValueClass(DocSize.class);
    
    job.setPartitionerClass(FirstPartitioner.class);
    
    job.setGroupingComparatorClass(FirstGroupingComparator.class);
    
    DistributedCache.addCacheFile(new URI(args.getString("subjects")), job.getConfiguration());
    
    job.getConfiguration().setInt(NUMBER_OF_DOCUMENTS, args.getInt("numdocs"));

    job.getConfiguration().set(OUTPUT_DIR, args.getString("output"));
    
    FileInputFormat.setInputPaths(job, new Path(args.getString("input")));
   
    FileOutputFormat.setOutputPath(job, new Path(args.getString("output") + "/docsizes-temp"));
    
   
	//Set the document factory class: HorizontalDocumentFactory or VerticalDocumentFactory
	if (args.getString("method").equalsIgnoreCase("horizontal")) {
		job.getConfiguration().setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, HorizontalDocumentFactory.class, PropertyBasedDocumentFactory.class);	
	} else {
		job.getConfiguration().setClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, VerticalDocumentFactory.class, PropertyBasedDocumentFactory.class);
	}
	
	
	 if (args.getString("format").equalsIgnoreCase("datarss")) {
	    job.getConfiguration().set(TripleIndexGenerator.RDFFORMAT_KEY, TripleIndexGenerator.DATARSS_FORMAT);
	 } else {
	    job.getConfiguration().set(TripleIndexGenerator.RDFFORMAT_KEY, TripleIndexGenerator.NTUPLES_FORMAT);
	 }
	 
	 
	 if (args.getString("properties") != null) {
		job.getConfiguration().set(TripleIndexGenerator.INDEXEDPROPERTIES_FILENAME_KEY, args.getString("properties"));
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
