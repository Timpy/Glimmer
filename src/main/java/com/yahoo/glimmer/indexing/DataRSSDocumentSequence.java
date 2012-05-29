package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentFactory;
import it.unimi.dsi.mg4j.document.DocumentIterator;
import it.unimi.dsi.mg4j.document.DocumentSequence;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys;

import java.io.IOException;
import java.io.InputStream;

/** TODO: deprecated? **/

/** A document sequence obtained by breaking an input stream at a specified separator. 
 * 
 * <p>This document sequences blindly passes to the indexer sequences of characters read
 * in a specified encoding and separated by a specified byte. 
 */

public class DataRSSDocumentSequence extends FastBufferedInputStream implements DocumentSequence {
	/** The byte separating documents in the input stream. */
	private final int separator;
	/** The factory used to return {@link Document}s. */
	private final DocumentFactory factory;
	/** If true, the last returned stream has been exhausted. This variable
	 * will be reset by a call to <code>nextDocument()</code>. */
	private boolean eof = true;
	/** The sequence will not return more than this number of documents. */
	private final int maxDocs;
	
	/** Creates a new document sequence based on a given input stream and separator; the
	 * sequence will not return more than the given number of documents.
	 * 
	 * @param inputStream the input stream containing all documents.
	 * @param separator the separator.
	 * @param factory the factory that will be used to create documents.
	 * @param maxDocs the maximum number of documents returned.
	 */
	
	public DataRSSDocumentSequence( final InputStream inputStream, final int separator, final DocumentFactory factory, final int maxDocs ) {
		super( inputStream );
		this.separator = separator;
		this.factory = factory;
		this.maxDocs = maxDocs;
	}
	
	/** Creates a new document sequence based on a given input stream and separator.
	 * 
	 * @param inputStream the input stream containing all documents.
	 * @param separator the separator.
	 * @param factory the factory that will be used to create documents.
	 */
	
	public DataRSSDocumentSequence( final InputStream inputStream, final int separator, final DocumentFactory factory ) {
		this( inputStream, separator, factory, Integer.MAX_VALUE );
	}
	
	public DocumentIterator iterator() {
		final Reference2ObjectArrayMap<Enum<?>,Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( 2 );
		
		return new DocumentIterator() {
			private int i;
			private Document last;
			
			public Document nextDocument() throws IOException {
				if ( last != null ) last.close();
				if ( i >= maxDocs ) return last = null;
				// If eof is not true, the caller did not exhaust the current document. We do it, however.
				if ( ! eof ) DataRSSDocumentSequence.this.close();
				eof = false;
				final String documentIndex = Integer.toString( i++ );
				metadata.put( MetadataKeys.TITLE, documentIndex );
				metadata.put( MetadataKeys.URI, documentIndex );

				return last = DataRSSDocumentSequence.this.noMoreBytes() ? null : factory.getDocument( DataRSSDocumentSequence.this, metadata );
			}

			public void close() {}
		};
	}

	public DocumentFactory factory() {
		return factory;
	}
	
	public boolean noMoreBytes() throws IOException {
		if ( avail > 0 ) return false;
		
    	avail = is.read( buffer );
    	if ( avail <= 0 ) {
    		// Ooops, there's nothing more to read. Let us set up eof and return.
    		avail = 0;
    		eof = true;
    		return true;
    	}
    	pos = 0;
    	return false;
	}
	
	public int read() throws IOException {
		if ( eof ) return -1;
		final int nextByte = super.read();

		if ( nextByte == separator || nextByte == -1 ) {
			eof = true;
			return -1;
		}
		
		return nextByte;
	}
	
	public int read( final byte[] b ) throws IOException {
		if ( eof ) return -1;
		return read( b, 0, b.length );
	}
	
    public int read( final byte[] b, int offset, int length ) throws IOException {
    	if ( eof ) return -1;
    	if ( length == 0 ) return 0;
    	
    	final int startOffset = offset;
    	int i, l;
    	
        for(;;) {
        	l = Math.min( length, avail );
       		// We scan the buffer for the separator, copying elements in the mean time. 
       		for( i = 0; i < l; i++ ) {
       			if ( buffer[ pos + i ] == separator ) break;
       			b[ offset + i ] = buffer[ pos + i ];
       		}

        	pos += i;
        	avail -= i;
        	
        	offset += i;
        	length -= i;

        	// If we were able to read enough characters, it's over.
        	if ( length == 0 ) return offset - startOffset;
        	// Otherwise, if we found a separator we return.
        	if ( i < l ) {
        		// This will set up eof.
        		read();
        		return offset - startOffset != 0 ? offset - startOffset : -1;
        	}
        	// Finally, in the last case (i == avail) we try to fill the buffer.
        	if ( noMoreBytes() ) return offset - startOffset != 0 ? offset - startOffset : -1;
        }
    }

	public void mark( final int readlimit ) {}

	public boolean markSupported() {
		return false;
	}
	
	// TODO: rewrite skip()
	public long skip( final long skip ) {
		throw new UnsupportedOperationException();
	}
	
	@Deprecated
	public void reset() {
		throw new UnsupportedOperationException();
	}
	
	public void flush() {
		throw new UnsupportedOperationException();
	}
	
	public void close() throws IOException {
		if ( ! eof ) while( read() != -1 );
		super.close();
	}

	public void filename( CharSequence filename ) throws IOException {
	}
}
