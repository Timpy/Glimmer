package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentCollection;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class CollectionReader {

	  
	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws SecurityException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, SecurityException, IOException, ClassNotFoundException {
		if (args.length < 1) {
			System.out.println("Usage: " + CollectionBuilder.class.getName() + " <dir> {<mph>} {<uri>}");
			System.out.println("Without the uri argument it prints all uris in the collection.");
			
			System.exit(1);
		}
		
		String[] collectionNames = new File(args[0]).list(new FilenameFilter(){
			public boolean accept(File dir, String name) {
				return name.endsWith(".collection");
			}
		});
		
		//make the paths absolute
//		for (int i=0; i < collectionNames.length; i++) {
//			collectionNames[i] = args[0] + collectionNames[i];
//			System.err.println(collectionNames[i]);
//		}
//		
		
		//final SimpleCompressedDocumentCollection test = (SimpleCompressedDocumentCollection) AbstractDocumentSequence.load( "/mnt/scratch2/pmika/data/btc/collection/small/triples-04993e8d-7d5b-4511-b85a-cc5db85338ac.collection");
		//System.err.println(test.basename);
		
		DocumentCollection[] collections = new DocumentCollection[collectionNames.length];
		for (int i=0; i < collectionNames.length; i++) {
			System.out.println(args[0] + collectionNames[i]);
			String basename = args[0] + collectionNames[i].replace('.', '/');
			System.out.println(basename);
			//File parent = new File(args[0] + collectionNames[i]).getParentFile();
			try {
				//documentCollection = (SimpleCompressedDocumentCollection) AbstractDocumentSequence.load(collectionString);
				 DocumentCollection documentCollection = (SimpleCompressedDocumentCollection)BinIO.loadObject( args[0] + collectionNames[i]);
				 documentCollection.filename( basename );
				 collections[i] = documentCollection;
				 System.out.println(collections[i].size() + " " + i);
			} catch (IOException io) {
				System.err.println(io);
			}
		}
		
		final ConcatenatedDocumentCollection collection = new ConcatenatedDocumentCollection(collectionNames, collections);
		/*
		final ConcatenatedDocumentCollection collection = new ConcatenatedDocumentCollection(collectionNames);
		
		String filename = args[0] + collectionNames[0];
		
		File parent = filename != null ? new File( filename.toString() ).getParentFile() : null;
		System.err.println(new File( parent, collectionNames[ 0 ] ).toString() );
		
		collection.filename(args[0] + collectionNames[0]); //ConcatenatedDocumentCollection will take the parent of this
	*/
		if (args.length > 2) {
			@SuppressWarnings("unchecked")
			final LcpMonotoneMinimalPerfectHashFunction<CharSequence> mph = (LcpMonotoneMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(args[1]);
			Document doc = collection.document((int) mph.getLong(args[2]));
			
			FastBufferedReader reader = (FastBufferedReader) doc.content(0);
			MutableString line = new MutableString();
			while ((reader.readLine(line)) != null) {
				System.out.println(line);
			}
			doc.close();
		} else {
			for (int i=0; i < collection.size(); i++) {
				
				Document doc = collection.document(i);
				System.out.println(doc.uri());

				doc.close();
			}
		}
		collection.close();
	}

}
