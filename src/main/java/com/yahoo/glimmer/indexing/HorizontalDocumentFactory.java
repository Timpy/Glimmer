package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationException;

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

    @Override
    public Document getDocument(InputStream rawContent, Reference2ObjectMap<Enum<?>, Object> docMetadata) throws IOException {
	RDFDocument result = new HorizontalDocument(this, docMetadata);
	result.setContent(rawContent);
	return result;
    }
}
