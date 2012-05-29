package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.util.Properties;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.semanticweb.yars.nx.parser.ParseException;

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

    private static final long serialVersionUID = 1L;

    private List<String> indexedProperties;

    LcpMonotoneMinimalPerfectHashFunction<CharSequence> subjectsMph;

    protected boolean parseProperty(final String key, final String[] values, final Reference2ObjectMap<Enum<?>, Object> metadata) throws ConfigurationException {
	if (sameKey(MetadataKeys.INDEXED_PROPERTIES_FILENAME, key)) {
	    metadata.put(MetadataKeys.INDEXED_PROPERTIES_FILENAME, ensureJustOne(key, values));
	    return true;
	}

	return super.parseProperty(key, values, metadata);
    }

    @SuppressWarnings("unchecked")
    protected void init() {
	super.init();
	// Retrieve properties to index from metadata, or if fails from the
	// filesystem or jar
	this.indexedProperties = (List<String>) resolve(MetadataKeys.INDEXED_PROPERTIES, defaultMetadata);
	if (this.indexedProperties == null) {
	    indexedProperties = new ArrayList<String>();
	    BufferedReader reader;
	    try {
		String fileName = (String) resolve(MetadataKeys.INDEXED_PROPERTIES_FILENAME, defaultMetadata);
		try {
		    reader = new BufferedReader(new FileReader(fileName));
		} catch (Exception e) {
		    System.err.println("INDEXED_PROPERTIES_FILENAME: " + fileName);
		    reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream(fileName)));
		}
		String nextLine = "";
		while ((nextLine = reader.readLine()) != null) {
		    if (!nextLine.trim().equals("")) {
			// Take the first column
			String property = nextLine.split("\\s")[0].trim(); // if
									   // no
									   // match,
									   // returns
									   // the
									   // whole
									   // string

			// Only include if it's in the namespaces table and not
			// blacklisted
			if (property != null && !onPredicateBlackList(property)) {
			    System.out.println("Going to index predicate:" + property);
			    indexedProperties.add(property);
			}
		    }
		}
		reader.close();
	    } catch (Exception e) {
		throw new RuntimeException(e);

	    }
	}

	// Retrieve MPH for objects encoding
	subjectsMph = (LcpMonotoneMinimalPerfectHashFunction<CharSequence>) resolve(MetadataKeys.SUBJECTS_MPH, defaultMetadata);

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
	init();
    }

    public VerticalDocumentFactory(final Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
	super(defaultMetadata);
	init();
    }

    public VerticalDocumentFactory(final String[] property) throws ConfigurationException {
	super(property);
	init();
    }

    public VerticalDocumentFactory() {
	super();
	init();
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
	    if (parsed)
		return;

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
		parsed = true;
		return;
	    }
	    // First part is URL, second part is docfeed
	    url = line.substring(0, line.indexOf('\t')).trim();
	    String data = line.substring(line.indexOf('\t')).trim();

	    if (data.trim().equals("")) {
		if (mapContext != null)
		    mapContext.getCounter(TripleIndexGenerator.Counters.EMPTY_DOCUMENTS).increment(1);
		parsed = true;
		return;
	    }

	    // Docfeed parsing
	    try {
		StatementCollectorHandler handler = parseStatements(url, data, (String) resolve(RDFDocumentFactory.MetadataKeys.RDFFORMAT, metadata));

		// TODO: sort by subject

		for (Statement stmt : handler.getStatements()) {

		    String predicate = stmt.getPredicate().toString();

		    String fieldName = predicate;

		    /*
		     * if (fieldName == null) {
		     * System.err.println("Could not convert URI to field name: "
		     * + predicate); if (reporter != null)
		     * reporter.incrCounter(Counters
		     * .CANNOT_CREATE_FIELDNAME_TRIPLES, 1); //Not in table,
		     * don't index continue; }
		     */

		    // Check if prefix is on blacklist
		    if (onPredicateBlackList(fieldName)) {
			if (mapContext != null)
			    mapContext.getCounter(TripleIndexGenerator.Counters.BLACKLISTED_TRIPLES).increment(1);
			continue;
		    }

		    // Determine whether we need to index, and the field
		    int fieldIndex = fieldIndex(fieldName);
		    if (fieldIndex == -1) {
			System.err.println("Field not indexed: " + fieldName);
			if (mapContext != null)
			    mapContext.getCounter(TripleIndexGenerator.Counters.UNINDEXED_PREDICATE_TRIPLES).increment(1);
			continue;
		    }

		    if (stmt.getObject() instanceof Resource) {
			// Encode the resource URI or bnode ID using the MPH for
			// objects
			fields.get(fieldIndex).add(subjectsMph.get(stmt.getObject().toString()).toString());
		    } else {

			// Iterate over the words of the value
			FastBufferedReader fbr = new FastBufferedReader(new StringReader(stmt.getObject().toString()));
			MutableString word = new MutableString(""), nonWord = new MutableString("");

			while (fbr.next(word, nonWord)) {
			    if (word != null && !word.equals("")) {

				if (TERMPROCESSOR.processTerm(word)) {
				    fields.get(fieldIndex).add(word.toString());
				    // System.out.println("Adding to field " +
				    // fieldIndex + " " + word.toString());
				}
			    }
			}
			fbr.close();
		    }

		    if (mapContext != null)
			mapContext.getCounter(TripleIndexGenerator.Counters.INDEXED_TRIPLES).increment(1);

		}
	    } catch (TransformerConfigurationException e) {
		System.err.println("Parsing failed for " + url + ": " + e.getMessage());
	    } catch (RDFParseException e) {
		System.err.println("Parsing failed for " + url + ": " + e.getMessage());
	    } catch (RDFHandlerException e) {
		System.err.println("Parsing failed for " + url + ": " + e.getMessage());
	    } catch (TransformerException e) {
		System.err.println("Parsing failed for " + url + ": " + e.getMessage() + "Content was: \n" + line);
	    } catch (ParseException e) {
		System.err.println("Parsing failed for " + url + ": " + e.getMessage() + "Content was: \n" + line);
	    } catch (IllegalArgumentException e) {
		System.err.println("Parsing failed for " + url + ": " + e.getMessage() + "Content was: \n" + line);
	    }
	    parsed = true;
	}

	public CharSequence title() {
	    try {
		ensureParsed();
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
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

    /*
     * public final static void main(String[] args) { String result = null;
     * String predicate = "http://creativecommons.org/ns#attributionName"; if
     * (predicate != null && predicate.startsWith("http://")) { URI pred = new
     * URIImpl(predicate); result =
     * pred.getNamespace().substring("http://".length()).replaceAll("[\\./#]",
     * "_"); if (result != null && result.length() > 0) { if
     * (result.charAt(result.length()-1) != NAMESPACE_SEPARATOR) { result +=
     * NAMESPACE_SEPARATOR; } result += pred.getLocalName(); } else {
     * //something strange is going on result = null; } }
     * System.out.println(result); }
     */

}
