package com.yahoo.glimmer.indexing;

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.IdentityDocumentFactory;
import it.unimi.dsi.mg4j.document.SimpleCompressedDocumentCollectionBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Build a Collection from the following format:
 * 
 * url \t document \n
 * 
 * where the document is in NTuples format.
 * 
 * Within the document, the tuples must be separated by NTUPLES_LINE_SEPARATOR.
 * 
 * Non-distributed version.
 * 
 * @author pmika@yahoo-inc.com
 * 
 */
public class CollectionBuilder {

    /**
     * 
     * @param args
     * @throws IOException
     * @throws ParseException
     */
    public static void main(String[] args) throws IOException, ParseException {
	if (args.length < 2) {
	    System.out.println("Usage: " + CollectionBuilder.class.getName() + " <inputdir> <outputdir>");
	    System.exit(1);
	}
	// basename is actually the complete path
	SimpleCompressedDocumentCollectionBuilder builder = new SimpleCompressedDocumentCollectionBuilder("", new IdentityDocumentFactory(), true);

	builder.open(args[1] + "foo");

	int count = 0;
	for (File file : new File(args[0]).listFiles()) {
	    /*
	     * if (!file.getName().endsWith(".bz2")) {
	     * System.err.println("Skipping file: " + file); continue; }
	     */
	    FileInputStream finstream = new FileInputStream(file);
	    finstream.read();
	    finstream.read();
	    BufferedReader reader = new BufferedReader(new InputStreamReader(new CBZip2InputStream(finstream)));
	    // BufferedReader reader = new BufferedReader(new
	    // InputStreamReader(finstream));

	    String nextLine;

	    while ((nextLine = reader.readLine()) != null) {
		if (count++ % 100000 == 0) {
		    System.out.println("Processed " + count + " lines.");

		    // Get current size of heap in bytes
		    long heapSize = Runtime.getRuntime().totalMemory();

		    // Get maximum size of heap in bytes. The heap cannot grow
		    // beyond this size.
		    // Any attempt will result in an OutOfMemoryException.
		    long heapMaxSize = Runtime.getRuntime().maxMemory();

		    // Get amount of free memory within the heap in bytes. This
		    // size will increase
		    // after garbage collection and decrease as new objects are
		    // created.
		    long heapFreeSize = Runtime.getRuntime().freeMemory();

		    System.out.println("Heap size: current/max/free: " + heapSize + "/" + heapMaxSize + "/" + heapFreeSize);

		}
		String url = nextLine.substring(0, nextLine.indexOf('\t'));
		String doc = nextLine.substring(nextLine.indexOf('\t') + 1, nextLine.length());
		builder.startDocument(url, url); // both title and uri are url
		builder.startTextField();
		// Parse using FastBufferedReader
		FastBufferedReader fbr = new FastBufferedReader(new StringReader(doc));
		MutableString word = new MutableString(""), nonWord = new MutableString("");

		while (fbr.next(word, nonWord)) {

		    builder.add(word, nonWord);
		}
		builder.endTextField();
		builder.endDocument();
	    }

	    reader.close();
	}
	builder.close();

    }

}
