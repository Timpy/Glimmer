package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.util.MG4JClassParser;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;

import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.configuration.ConfigurationException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.semanticweb.yars.nx.parser.ParseException;

import com.yahoo.glimmer.util.DataRSSToNTuples;
import com.yahoo.glimmer.util.Util;


/* Common superclass to HorizontalDocumentFactory and VerticalDocumentFactory.
 * 
 */
public abstract class RDFDocumentFactory extends PropertyBasedDocumentFactory {
	public static final String NULL_URL = "NULL_URL";
	
	private static final long serialVersionUID = 1L;

	/** The word reader used for all documents. */
	protected transient WordReader wordReader;	
	
	protected transient DataRSSToNTuples converter;
	
	public final static String[] PREDICATE_BLACKLIST = {"stag", "tagspace", "ctag", "rel", "mm"};
	public static final char NAMESPACE_SEPARATOR = '_';
	
	/** Determines if we use the namespaces table for abbreviating field names */
	public static final boolean USE_NAMESPACES = false;
		
	protected static final TermProcessor TERMPROCESSOR = CombinedTermProcessor.getInstance();
	
	/** Used by the documents, not by the factory
	 * 
	 * @author pmika
	 *
	 */
	public static enum MetadataKeys {
		MAPPER_CONTEXT, RDFFORMAT, INDEXED_PROPERTIES, INDEXED_PROPERTIES_FILENAME, SUBJECTS_MPH
	};
	
	

	public RDFDocumentFactory( final Properties properties ) throws ConfigurationException {
		super( properties );
		init();
	}

	public RDFDocumentFactory( final Reference2ObjectMap<Enum<?>,Object> defaultMetadata ) {
		super( defaultMetadata );
		init();
	}

	public RDFDocumentFactory( final String[] property ) throws ConfigurationException {
		super( property );
		init();
	}

	public RDFDocumentFactory() {
		super();
		init();
	}
	protected boolean parseProperty( final String key, final String[] values, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws ConfigurationException {
		if ( sameKey( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, key ) ) {
			metadata.put( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, Charset.forName( ensureJustOne( key, values ) ).toString() );
			return true;
		}
		else if ( sameKey( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER, key ) ) {
			try {
				final String spec = ( ensureJustOne( key, values ) ).toString();
				metadata.put( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER, spec );
				// Just to check
				ObjectParser.fromSpec( spec, WordReader.class, MG4JClassParser.PACKAGE );
			}
			catch ( ClassNotFoundException e ) {
				throw new ConfigurationException( e );
			}
			// TODO: this must turn into a more appropriate exception
			catch ( Exception e ) {
				throw new ConfigurationException( e );
			}
			return true;
		} else if ( sameKey( MetadataKeys.MAPPER_CONTEXT, key ) ) {
			metadata.put( MetadataKeys.MAPPER_CONTEXT, ensureJustOne( key, values ));
			return true;
		} else if ( sameKey( MetadataKeys.RDFFORMAT, key ) ) {
			metadata.put( MetadataKeys.RDFFORMAT, ensureJustOne( key, values ));
			return true;
		}
		return super.parseProperty( key, values, metadata );
	}
 

/*	public String predicateToName(String predicate) {
		String result = null;
		
		//Look up predicate in the namespaces table or null if not in table
		String prefix = null, localName = null;
		for (String key : prefixToNSMap.keySet()) {
			String ns = prefixToNSMap.get(key);
			if (predicate.startsWith(ns)) {
				prefix = key;
				localName = predicate.substring(ns.length());
				break;
			}
		}
		if (prefix != null && localName != null) {
			result = localName + NAMESPACE_SEPARATOR + prefix;
		} 
		//System.out.println(predicate + "->" + (result==null? "NULL": result));
	
		
		else {
			//Generate a short name from the URI
			if (predicate != null && predicate.startsWith("http://")) {
				URI pred = new URIImpl(predicate);
				result = pred.getNamespace().substring("http://".length()).replaceAll("[\\./#\\-\\~]+", "_");
				if (result != null && result.length() > 0) {
					if (result.charAt(result.length()-1) != NAMESPACE_SEPARATOR) {
						result += NAMESPACE_SEPARATOR;
					}		
					result += pred.getLocalName().replaceAll("[\\./#\\-\\~]+", "_");
				} else {
					//something strange is going on
					result = null;
				}
			}
			
		} 
		return result;
	}


	protected String nameToPredicate(String name) {
		String result = null;
		String prefix = name.substring(name.lastIndexOf(NAMESPACE_SEPARATOR) + 1);
		String localName = name.substring(0, name.lastIndexOf(NAMESPACE_SEPARATOR));
		result = prefixToNSMap.get(prefix) + localName;;
		//System.out.println(name + "->" + (result==null? "NULL": result));
		return result;
	}
	*/
	
	 public static String encodeFieldName(String name) {
		  
		  return name.replaceAll("[^a-zA-Z0-9]+", "_");

	  }
	
	protected boolean onPredicateBlackList(String name) {
		//Filter out namespaces on blacklist
		for (String black: PREDICATE_BLACKLIST) {
			if (name.startsWith(black)) {
				return true;
			}
		}
		return false;
	}
	
	protected StatementCollectorHandler parseStatements(String url, String data, String format) throws TransformerConfigurationException, RDFParseException, RDFHandlerException, MalformedURLException, IOException, TransformerException, ParseException {
		StatementCollectorHandler handler = new StatementCollectorHandler();	
		if (format.equals(TripleIndexGenerator.DATARSS_FORMAT)) {
			//DataRSS format
	
			converter.convert(new URL(url), data, true, handler); //remove datatypes
			
		} else {
			//NTuples format where tuples are separated by double spaces
			String[] lines = data.split("  ");
			for (String line: lines) {
				
				handler.handleStatement(Util.parseStatement(line));
			}

		}
		return handler;
	}
	
	protected void init() {

		Object o;
		try {
			o = defaultMetadata.get( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER );
			wordReader = o == null ? new FastBufferedReader() : ObjectParser.fromSpec( o.toString(), WordReader.class, MG4JClassParser.PACKAGE );
		}
		catch ( Exception e ) {
			throw new RuntimeException( e );
		}
		
		// Configure converter
		try {
			converter = new DataRSSToNTuples(); 
		} catch (TransformerConfigurationException e1) {
			throw new RuntimeException(e1);
		} 
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		init();
	}

   

}
