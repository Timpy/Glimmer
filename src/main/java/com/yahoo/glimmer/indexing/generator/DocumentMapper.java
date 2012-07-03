package com.yahoo.glimmer.indexing.generator;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.AbstractObject2LongFunction;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentFactory;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.yahoo.glimmer.indexing.RDFDocumentFactory;
import com.yahoo.glimmer.indexing.RDFInputFormat;
import com.yahoo.glimmer.indexing.VerticalDocumentFactory;

public class DocumentMapper extends Mapper<LongWritable, Document, TermOccurrencePair, Occurrence> {
    static final int ALIGNMENT_INDEX = -1; // special index for alignments
    
    private DocumentFactory factory;
    private boolean verticalNotHorizontal;
    private AbstractObject2LongFunction<CharSequence> resourcesHash;

    enum Counters {
	FAILED_PARSING, INDEXED_OCCURRENCES, NEGATIVE_PREDICATE_ID, NUMBER_OF_RECORDS
    }
    
    public void setFactory(DocumentFactory factory) {
	this.factory = factory;
    }
    public void setVerticalNotHorizontal(boolean verticalNotHorizontal) {
	this.verticalNotHorizontal = verticalNotHorizontal;
    }
    public void setResourcesHash(AbstractObject2LongFunction<CharSequence> resourcesHash) {
	this.resourcesHash = resourcesHash;
    }

    @SuppressWarnings("unchecked")
    public void setup(Context context) {
	Configuration job = context.getConfiguration();

	// Create an instance of the factory that was used...we only need
	// this to get the number of fields
	// Unfortunately, this means that we will read the objects hash
	Class<?> documentFactoryClass = job.getClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, RDFDocumentFactory.class);
	factory = RDFDocumentFactory.initFactory(documentFactoryClass, job, context, false);
	verticalNotHorizontal = (factory instanceof VerticalDocumentFactory);
	
	// Get the cached archives/files
	FSDataInputStream subjectsInput = null, predicatesInput = null;
	try {
	    FileSystem fs = FileSystem.getLocal(job);
	    Path subjectsLocation = DistributedCache.getLocalCacheFiles(job)[0];
	    subjectsInput = fs.open(subjectsLocation);
	    resourcesHash = (AbstractObject2LongFunction<CharSequence>) BinIO.loadObject(subjectsInput);
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
	    if (predicatesInput != null) {
		try {
		    predicatesInput.close();
		} catch (IOException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    }
	}
    }

    @Override
    public void map(LongWritable key, Document doc, Context context) throws IOException, InterruptedException {

	if (doc == null || doc.uri().equals(RDFDocumentFactory.NULL_URL)) {
	    // Failed parsing
	    context.getCounter(Counters.FAILED_PARSING).increment(1);
	    System.out.println("Document failed parsing");
	    return;
	}

	int docID = resourcesHash.get(doc.uri().toString()).intValue();

	if (docID < 0) {
	    throw new RuntimeException("Negative DocID for URI: " + doc.uri());
	}

	// Collect the keys (term+index) of this document
	HashSet<TermOccurrencePair> keySet = new HashSet<TermOccurrencePair>();

	// used for counting # of docs per term
	Occurrence fakeDocOccurrrence = new Occurrence(null, docID);

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
		    context.setStatus(factory.fieldName(i) + "=" + term.substring(0, Math.min(term.length(), 50)));

		    // Create an occurrence at the next position
		    Occurrence occ = new Occurrence(docID, position);
		    context.write(new TermOccurrencePair(term.toString(), i, occ), occ);
		    
		    // Create fake occurrences for each term (this will be
		    // used for counting # of docs per term
		    keySet.add(new TermOccurrencePair(term.toString(), i, fakeDocOccurrrence));
		    
		    position++;
		    context.getCounter(Counters.INDEXED_OCCURRENCES).increment(1);

		    if (verticalNotHorizontal) {
			// Create an entry in the alignment index
			int predicateID = resourcesHash.get(fieldName).intValue();
			if (predicateID >= 0) {
			    Occurrence predicateOcc = new Occurrence(predicateID, null);
			    // TODO Why not add to keySet?  
			    context.write(new TermOccurrencePair(term.toString(), ALIGNMENT_INDEX, predicateOcc), predicateOcc);
			    
			    Occurrence fakePredicateOccurrrence = new Occurrence(null, predicateID);
			    keySet.add(new TermOccurrencePair(term.toString(), ALIGNMENT_INDEX, fakePredicateOccurrrence));
			} else {
			    System.err.println("Negative predicateID for URI: " + fieldName);
			    context.getCounter(Counters.NEGATIVE_PREDICATE_ID).increment(1);
			}
		    }
		} else {
		    System.out.println("Nextterm is null");
		}
	    }
	}

	context.getCounter(Counters.NUMBER_OF_RECORDS).increment(1);

	for (TermOccurrencePair term : keySet) {
	    context.write(term, term.getOccurrence());
	}
	// Close document
	doc.close();
    }
}