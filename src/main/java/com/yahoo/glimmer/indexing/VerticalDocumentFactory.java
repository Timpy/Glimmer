package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.util.Properties;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

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
    public static final String PREDICATES_FILENAME_KEY = "PredicatesFilename";

    private List<String> indexedProperties;

    protected void init() {
	super.init();

	// Should only be called once per
	if (this.indexedProperties != null) {
	    throw new IllegalStateException("init() has aready been called.");
	}
	indexedProperties = new ArrayList<String>();

	Configuration conf = getTaskAttemptContext().getConfiguration();
	String predicatesCacheFilename = conf.get(PREDICATES_FILENAME_KEY);
	if (predicatesCacheFilename == null) {
	    System.out.println("No predicates file given to load properties from.");
	} else {
	    try {
		Path predicatesPath = new Path(predicatesCacheFilename);
		FileSystem fs = FileSystem.get(conf);
		CompressionCodecFactory compressionCodecfactory = new CompressionCodecFactory(conf);

		if (!fs.exists(predicatesPath)) {
		    throw new IllegalStateException("The predicates file " + predicatesCacheFilename + " was not found in the distributed cache.");
		}
		InputStream predicatesInputStream = fs.open(predicatesPath);

		CompressionCodec codecIfAny = compressionCodecfactory.getCodec(predicatesPath);
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
    }

    public void setIndexedProperties(List<String> indexedProperties) {
	this.indexedProperties = indexedProperties;
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

    @Override
    public Document getDocument(InputStream rawContent, Reference2ObjectMap<Enum<?>, Object> docMetadata) throws IOException {
	RDFDocument result = new VerticalDocument(this, docMetadata);
	result.setContent(rawContent);
	return result;
    }
}
