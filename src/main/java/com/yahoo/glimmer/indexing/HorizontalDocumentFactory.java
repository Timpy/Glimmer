package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.util.Properties;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.semanticweb.yars.nx.namespace.RDF;

public class HorizontalDocumentFactory extends RDFDocumentFactory {
    private static final long serialVersionUID = 7360010820859436049L;
    
    /**
     * Returns a copy of this document factory. A new parser is allocated for
     * the copy.
     */
    public HorizontalDocumentFactory copy() {
	return new HorizontalDocumentFactory(defaultMetadata);
    }

    public HorizontalDocumentFactory(final Properties properties) throws ConfigurationException {
	super(properties);
    }

    public HorizontalDocumentFactory(final Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
	super(defaultMetadata);
    }

    public HorizontalDocumentFactory(final String[] property) throws ConfigurationException {
	super(property);
    }

    public HorizontalDocumentFactory() {
	super();
    }

    public int numberOfFields() {
	return 4;
    }

    public String fieldName(final int field) {
	ensureFieldIndex(field);
	switch (field) {
	case 0:
	    return "token";
	case 1:
	    return "property";
	case 2:
	    return "context";
	case 3:
	    return "uri";
	default:
	    throw new IllegalArgumentException();
	}
    }

    public int fieldIndex(final String fieldName) {
	for (int i = 0; i < numberOfFields(); i++)
	    if (fieldName(i).equals(fieldName))
		return i;
	return -1;
    }

    public FieldType fieldType(final int field) {
	ensureFieldIndex(field);
	switch (field) {
	case 0:
	    return FieldType.TEXT;
	case 1:
	    return FieldType.TEXT;
	case 2:
	    return FieldType.TEXT;
	case 3:
	    return FieldType.TEXT;
	default:
	    throw new IllegalArgumentException();
	}
    }

    public Document getDocument(final InputStream rawContent, final Reference2ObjectMap<Enum<?>, Object> metadata) throws IOException {
	RDFDocument result = new HorizontalDataRSSDocument(metadata);
	result.setContent(rawContent);
	return result;
    }

    /**
     * A DataRSS document.
     * 
     * <p>
     * We delay the actual parsing until it is actually necessary, so operations
     * like getting the document URI will not require parsing.
     */

    protected class HorizontalDataRSSDocument extends RDFDocument {
	private Reference2ObjectMap<Enum<?>, Object> metadata;
	/** Whether we already parsed the document. */
	private boolean parsed;
	/** The cached raw content. */
	private InputStream rawContent;

	/** Field content **/
	private List<String> tokens = new ArrayList<String>();
	private List<String> properties = new ArrayList<String>();
	private List<String> contexts = new ArrayList<String>();
	private List<String> uri = new ArrayList<String>();

	private String url = NULL_URL;

	protected HorizontalDataRSSDocument(final Reference2ObjectMap<Enum<?>, Object> metadata) {
	    this.metadata = metadata;
	}

	public void setContent(InputStream rawContent) {

	    // HACK:
	    if (rawContent == null) {
		this.rawContent = new ByteArrayInputStream(new byte[0]);
	    } else {
		this.rawContent = rawContent;
	    }
	    tokens.clear();
	    properties.clear();
	    contexts.clear();
	    uri.clear();

	    parsed = false;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void ensureParsed() throws IOException {
	    if (parsed) {
		return;
	    }
	    parsed = true;

	    if (rawContent == null) {
		throw new IOException("Trying to parse null rawContent");
	    }

	    Mapper.Context mapContext = (Mapper.Context) resolve(RDFDocumentFactory.MetadataKeys.MAPPER_CONTEXT, metadata);
	    BufferedReader r = new BufferedReader(new InputStreamReader(rawContent, (String) resolveNotNull(PropertyBasedDocumentFactory.MetadataKeys.ENCODING,
		    metadata)));
	    String line = r.readLine();
	    r.close();

	    if (line == null || line.trim().equals("")) {
		if (mapContext != null)
		    mapContext.getCounter(TripleIndexGenerator.Counters.EMPTY_LINES).increment(1);
		return;
	    }
	    // First part is URL, second part is docfeed
	    url = line.substring(0, line.indexOf('\t')).trim();
	    String data = line.substring(line.indexOf('\t')).trim();

	    if (data.trim().equals("")) {
		if (mapContext != null) {
		    mapContext.getCounter(TripleIndexGenerator.Counters.EMPTY_DOCUMENTS).increment(1);
		}
		return;
	    }

	    // Index URI except the protocol, if exists
	    FastBufferedReader fbr;
	    MutableString word = new MutableString();
	    MutableString nonWord = new MutableString();
	    int firstColon = url.indexOf(':');
	    if (firstColon < 0) {
		fbr = new FastBufferedReader(new StringReader(url));
	    } else {
		fbr = new FastBufferedReader(new StringReader(url.substring(firstColon + 1)));
	    }
	    while (fbr.next(word, nonWord)) {
		if (word != null && !word.equals("")) {
		    if (TERMPROCESSOR.processTerm(word)) {
			uri.add(word.toString().toLowerCase());
		    }
		}
	    }
	    fbr.close();

	    // Docfeed parsing
	    StatementCollectorHandler handler;
	    try {
		handler = parseStatements(url, data);
	    } catch (IOException e) {
		throw e;
	    } catch (Exception e) {
		System.err.println("Parsing failed for " + url + ": " + e.getMessage() + "Content was: \n" + line);
		return;
	    }

	    for (Statement stmt : handler.getStatements()) {
		String predicate = stmt.getPredicate().toString();
		String fieldName = encodeFieldName(predicate);
		
		// Check if prefix is on blacklist
		if (onPredicateBlackList(fieldName)) {
		    if (mapContext != null)
			mapContext.getCounter(TripleIndexGenerator.Counters.BLACKLISTED_TRIPLES).increment(1);
		    continue;
		}
		
		String contextValue = NULL_URL;
		if (withContext && stmt.getContext() != null) {
		    String s = stmt.getContext().toString();
		    Long l = resourcesHash.get(s);
		    if (l == null) {
			throw new IllegalStateException("Context " + stmt.getContext().toString() + " not in resources hash function!");
		    }
		    contextValue = l.toString(); 
		}
		
		if (stmt.getObject() instanceof Resource) {
		    if (predicate.equals(RDF.TYPE.toString())) {
			if (mapContext != null)
			    mapContext.getCounter(TripleIndexGenerator.Counters.RDF_TYPE_TRIPLES).increment(1);
			tokens.add(stmt.getObject().toString());
		    } else {
			tokens.add(resourcesHash.get(stmt.getObject().toString()).toString());
		    }
		    properties.add(fieldName);
		    if (contextValue != null) {
			contexts.add(contextValue);
		    }
		} else {
		    String object = ((Literal) stmt.getObject()).getLabel();
		    // Iterate over the words of the value
		    fbr = new FastBufferedReader(new StringReader(object));
		    while (fbr.next(word, nonWord)) {
			if (word != null && !word.equals("")) {
			    if (TERMPROCESSOR.processTerm(word)) {
				// Lowercase terms
				tokens.add(word.toString());

				// Preserve casing for properties and
				// contexts
				properties.add(fieldName);
				
				if (contextValue != null) {
				    contexts.add(contextValue);
				}
			    }

			}
		    }
		    fbr.close();
		}
		
		if (mapContext != null)
		    mapContext.getCounter(TripleIndexGenerator.Counters.INDEXED_TRIPLES).increment(1);
	    }
	}

	public CharSequence title() {
	    CharSequence title = (CharSequence) resolve(PropertyBasedDocumentFactory.MetadataKeys.TITLE, metadata);
	    return title == null ? "" : title;
	}

	public String toString() {
	    return uri().toString();
	}

	public CharSequence uri() {
	    try {
		ensureParsed();
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	    return url;
	}

	public Object content(final int field) throws IOException {
	    ensureFieldIndex(field);
	    ensureParsed();
	    switch (field) {
	    case 0:
		return new WordArrayReader(tokens);
	    case 1:
		return new WordArrayReader(properties);
	    case 2:
		return new WordArrayReader(contexts);
	    case 3:
		return new WordArrayReader(uri);
	    default:
		throw new IllegalArgumentException();
	    }
	}

	// All fields use the same reader
	public WordReader wordReader(final int field) {
	    ensureFieldIndex(field);
	    return wordReader;
	}
    }
}
