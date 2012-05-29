package com.yahoo.glimmer.indexing;

/*		 
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.mg4j.document.AbstractDocumentCollection;
import it.unimi.dsi.mg4j.document.AbstractDocumentSequence;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentCollection;
import it.unimi.dsi.mg4j.document.DocumentFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;

/** A document collection exhibiting a list of underlying document collections, called <em>segments</em>,
 * as a single collection. The underlying collections are (virtually) <em>concatenated</em>&mdash;that is,
 * the first document of the second collection is renumbered to the size of the first collection, and so on.
 * All underlying collections must use the same {@linkplain DocumentFactory factory class}.
 * @author Sebastiano Vigna
 *
 */

public class ConcatenatedDocumentCollection extends AbstractDocumentCollection implements Serializable {
	private static final long serialVersionUID = 1L;
	/** The name of the collections composing this concatenated document collection. */
	private final String[] collectionName;
	/** The collections composing this concatenated document collection. */
	private transient DocumentCollection[] collection;
	/** The length of {@link #collection}. */
	private final int n;
	/** The array of starting documents (the last element is the overall number of documents). */
	private long startDocument[];
	
	/** Creates a new concatenated document collection using giving component collections.
	 * 
	 * @param collection a list of component collections.
	 */
	public ConcatenatedDocumentCollection( final String[] collectionName, final DocumentCollection[] collection ) {
		if ( collection.length != collectionName.length ) throw new IllegalArgumentException();
		this.collectionName = collectionName;
		this.collection = collection;
		this.n = collection.length;
		startDocument = new long[ n + 1 ];
		for( int i = 0; i < n; i++ ) startDocument[ i + 1 ] = startDocument[ i ] + collection[ i ].size();
		
	}

	private void initCollections( final CharSequence filename, boolean rethrow ) throws IllegalArgumentException, SecurityException, IOException, ClassNotFoundException {
		try {
			collection = new DocumentCollection[ n ];
			File parent = filename != null ? new File( filename.toString() ).getParentFile() : null;
			for( int i = n; i-- != 0; ) collection[ i ] = (DocumentCollection)AbstractDocumentSequence.load( new File( parent, collectionName[ i ] ).toString() );
			if ( n > 0  ) {
				Class<? extends DocumentFactory> factoryClass = collection[ 0 ].factory().getClass();
				// TODO: this is crude. We should have a contract for equality of factories, and use equals().
				for( int i = 0; i < n; i++ ) if ( collection[ i ].factory().getClass() != factoryClass ) throw new IllegalArgumentException( "All segment in a concatenated document collection must used the same factory class" );
			}
			startDocument = new long[ n + 1 ];
			for( int i = 0; i < n; i++ ) startDocument[ i + 1 ] = startDocument[ i ] + collection[ i ].size();
		}
		catch( IOException e ) {
			if ( rethrow ) throw e;
		}
	}

	private void ensureCollections() {
		if ( collection == null ) throw new IllegalStateException( "The collections composing this " + ConcatenatedDocumentCollection.class.getName() + " have not been loaded correctly; please use " + AbstractDocumentSequence.class.getSimpleName() + ".load() or call filename() after deserialising this instance, and ensure that the names stored are correct " );
	}
	
	/** Creates a new, partially uninitialised concatenated document collection using giving component collections names.
	 * 
	 * @param collectionName a list of names of component collections.
	 */
	public ConcatenatedDocumentCollection( String[] collectionName ) throws IllegalArgumentException, SecurityException {
		this.collectionName = collectionName;
		n = collectionName.length;
	}

	public void filename( CharSequence filename ) {
		try {
			initCollections( filename, true );
		}
		catch ( IllegalArgumentException e ) {
			throw new RuntimeException( e );
		}
		catch ( SecurityException e ) {
			throw new RuntimeException( e );
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
		catch ( ClassNotFoundException e ) {
			throw new RuntimeException( e );
		}
	}
	
	public DocumentCollection copy() {
		final DocumentCollection[] collection = new DocumentCollection[ n ];
		for( int i = n; i-- != 0; ) collection[ i ] = this.collection[ i ].copy();
		return new ConcatenatedDocumentCollection( collectionName, collection );
	}

	
	public Document document( int index ) throws IOException {
		ensureDocumentIndex( index );
		ensureCollections();
		int segment = Arrays.binarySearch( startDocument, index );
		if ( segment < 0 ) segment = -segment - 2;
		return collection[ segment ].document(  (int)( index - startDocument[ segment ] ) );
	}

	
	public Reference2ObjectMap<Enum<?>,Object> metadata( int index ) throws IOException {
		ensureDocumentIndex( index );
		ensureCollections();
		int segment = Arrays.binarySearch( startDocument, index );
		if ( segment < 0 ) segment = -segment - 2;
		return collection[ segment ].metadata(  (int)( index - startDocument[ segment ] ) );
	}

	
	public int size() {
		return (int)startDocument[ n ];
	}

	
	public InputStream stream( int index ) throws IOException {
		ensureDocumentIndex( index );
		ensureCollections();
		int segment = Arrays.binarySearch( startDocument, index );
		if ( segment < 0 ) segment = -segment - 2;
		return collection[ segment ].stream(  (int)( index - startDocument[ segment ] ) );
	}

	
	public DocumentFactory factory() {
		ensureCollections();
		return collection[ 0 ].factory();
	}
	
	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		initCollections( null, false );
	}

	public void close() throws IOException {
		super.close();
		for( DocumentCollection c: collection ) c.close();
	}
}
