package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.util.Properties;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.semanticweb.yars.nx.namespace.RDF;

/**
 * The fields to be indexed are read from the file INDEXED_PROPERTIES_FILENAME,
 * which may contain either short names of the form prefix_localname, e.g.
 * fn_vcard or full URIs. In case of short names, the URI of the predicate
 * should be possible to convert to a shortname using the namespaces table. In
 * case of URIs, the URI in the file should be convertible using the namespaces
 * table.
 * 
 * @author pmika@yahoo-inc.com
 * 
 */
public class VerticalDocumentFactory extends RDFDocumentFactory {
    private static final long serialVersionUID = -1996948102296996229L;
    private List<String> indexedProperties;

    protected void init() {
	super.init();

	// Should only be called once per
	if (this.indexedProperties != null) {
	    throw new IllegalStateException("init() has aready been called.");
	}
	indexedProperties = new ArrayList<String>();

	// Load predicates(properties) from the DistributedCache
	String predicatesFilename = (String) resolveNotNull(MetadataKeys.PREDICATES_FILENAME, defaultMetadata);
	Configuration conf = (Configuration) resolveNotNull(MetadataKeys.HADOOP_CONF, defaultMetadata);
	
	CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	
	try {
	    FileSystem fs = FileSystem.getLocal(conf);
	    Path predicatesPath = null;
	    for (Path distributedPath : DistributedCache.getLocalCacheFiles(conf)) {
		if (distributedPath.getName().equals(predicatesFilename)) {
		    predicatesPath = distributedPath;
		    break;
		}
	    }
	    if (predicatesPath == null) {
		throw new IllegalStateException("The predicates file " + predicatesFilename + " was not found in the distributed cache.");
	    }
	    InputStream predicatesInputStream = fs.open(predicatesPath);

	    CompressionCodec codecIfAny = factory.getCodec(predicatesPath);
	    if (codecIfAny != null) {
		predicatesInputStream = codecIfAny.createInputStream(predicatesInputStream);
	    }

	    BufferedReader reader = new BufferedReader(new InputStreamReader(predicatesInputStream));
	    String nextLine = "";

	    while ((nextLine = reader.readLine()) != null) {
		nextLine = nextLine.trim();
		if (!nextLine.isEmpty()) {
		    // Take the first column
		    String property = nextLine.split("\\s+")[0];
		    // if no match, returns the whole string

		    // Only include if it's in the namespaces table and not
		    // blacklisted
		    if (property != null && !onPredicateBlackList(property)) {
			System.out.println("Going to index predicate:" + property);
			indexedProperties.add(property);
		    }
		}
	    }
	    reader.close();
	    
	    System.out.println("Read " + indexedProperties.size() + " properties from predicates file " + predicatesPath.toString());
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    /**
     * Returns a copy of this document factory. A new parser is allocated for
     * the copy.
     */
    public VerticalDocumentFactory copy() {
	return new VerticalDocumentFactory(defaultMetadata);
    }

    public VerticalDocumentFactory(final Properties properties) throws ConfigurationException {
	super(properties);
    }

    public VerticalDocumentFactory(final Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
	super(defaultMetadata);
    }

    public VerticalDocumentFactory(final String[] property) throws ConfigurationException {
	super(property);
    }

    public VerticalDocumentFactory() {
	super();
    }

    public int numberOfFields() {
	return indexedProperties.size();
    }

    public String fieldName(final int field) {
	ensureFieldIndex(field);
	String name = indexedProperties.get(field);
	if (name == null) {
	    // Not an indexed field
	    return "NOINDEX-" + field;
	}
	return name;
    }

    public int fieldIndex(final String fieldName) {
	for (int i = 0; i < numberOfFields(); i++) {
	    if (fieldName(i).equals(fieldName))
		return i;
	}
	return -1;
    }

    public FieldType fieldType(final int field) {
	ensureFieldIndex(field);
	return FieldType.TEXT;
    }

    private void readObject(final ObjectInputStream s) throws IOException, ClassNotFoundException {
	s.defaultReadObject();
	init();
    }

    /**
     * A DataRSS document.
     * 
     * <p>
     * We delay the actual parsing until it is actually necessary, so operations
     * like getting the document URI will not require parsing.
     */

    protected class VerticalDataRSSDocument extends RDFDocument {
	private Reference2ObjectMap<Enum<?>, Object> metadata;
	/** Whether we already parsed the document. */
	private boolean parsed;
	/** The cached raw content. */
	private InputStream rawContent;
	private List<List<String>> fields = new ArrayList<List<String>>();
	private String url = NULL_URL;

	protected VerticalDataRSSDocument(final Reference2ObjectMap<Enum<?>, Object> metadata) {
	    this.metadata = metadata;
	}

	public void setContent(InputStream rawContent) {
	    this.rawContent = rawContent;

	    // Initialize fields
	    fields.clear();
	    for (int i = 0; i < numberOfFields(); i++) {
		fields.add(new ArrayList<String>());
	    }

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
		    mapContext.getCounter(Counters.EMPTY_LINES).increment(1);
		return;
	    }
	    // First part is URL, second part is docfeed
	    url = line.substring(0, line.indexOf('\t')).trim();
	    String data = line.substring(line.indexOf('\t')).trim();

	    if (data.trim().equals("")) {
		if (mapContext != null) {
		    mapContext.getCounter(Counters.EMPTY_DOCUMENTS).increment(1);
		}
		return;
	    }

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

		String fieldName = predicate;

		// Check if prefix is on blacklist
		if (onPredicateBlackList(fieldName)) {
		    if (mapContext != null)
			mapContext.getCounter(Counters.BLACKLISTED_TRIPLES).increment(1);
		    continue;
		}

		// Determine whether we need to index, and the field
		int fieldIndex = fieldIndex(fieldName);
		if (fieldIndex == -1) {
		    System.err.println("Field not indexed: " + fieldName);
		    if (mapContext != null)
			mapContext.getCounter(Counters.UNINDEXED_PREDICATE_TRIPLES).increment(1);
		    continue;
		}

		if (stmt.getObject() instanceof Resource) {
		    // For all fields except type, encode the resource URI
		    // or bnode ID using the resources hash
		    if (predicate.equals(RDF.TYPE.toString())) {
			if (mapContext != null)
			    mapContext.getCounter(Counters.RDF_TYPE_TRIPLES).increment(1);
			fields.get(fieldIndex).add(stmt.getObject().toString());
		    } else {
			fields.get(fieldIndex).add(resourcesHash.get(stmt.getObject().stringValue()).toString());
		    }
		} else {
		    Value object = stmt.getObject();
		    String objectAsString;
		    if (object instanceof Literal) {
			// If we treat a Literal as just a Value we index the
			// @lang and ^^<type> too
			objectAsString = ((Literal) object).stringValue();
		    } else {
			objectAsString = object.stringValue();
		    }

		    // Iterate over the words of the value
		    FastBufferedReader fbr = new FastBufferedReader(new StringReader(objectAsString));
		    MutableString word = new MutableString();
		    MutableString nonWord = new MutableString();

		    while (fbr.next(word, nonWord)) {
			if (word != null && !word.equals("")) {
			    if (CombinedTermProcessor.getInstance().processTerm(word)) {
				fields.get(fieldIndex).add(word.toString());
			    }
			}
		    }
		    fbr.close();
		}

		if (mapContext != null) {
		    Counter counter = mapContext.getCounter(Counters.INDEXED_TRIPLES);
		    counter.increment(1);
		}
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
	    return new WordArrayReader(fields.get(field));
	}

	// All fields use the same reader
	public WordReader wordReader(final int field) {
	    ensureFieldIndex(field);
	    return wordReader;
	}
    }

    public Document getDocument(final InputStream rawContent, final Reference2ObjectMap<Enum<?>, Object> metadata) throws IOException {
	RDFDocument result = new VerticalDataRSSDocument(metadata);
	result.setContent(rawContent);
	return result;
    }
}
