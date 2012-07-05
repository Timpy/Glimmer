package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.mg4j.document.AbstractDocument;

import java.io.IOException;
import java.io.InputStream;

public abstract class RDFDocument extends AbstractDocument {
    public static final String NULL_URL = "NULL_URL";

    protected final RDFDocumentFactory factory;
    private Reference2ObjectMap<Enum<?>, Object> docMetadata;
    /** Whether we already parsed the document. */
    protected boolean parsed;
    /** The cached raw content. */
    protected InputStream rawContent;
    protected String url = NULL_URL;

    protected abstract void ensureParsed() throws IOException;

    public RDFDocument(RDFDocumentFactory factory, Reference2ObjectMap<Enum<?>, Object> docMetadata) {
	this.factory = factory;
	if (docMetadata == null) {
	    this.docMetadata = new Reference2ObjectArrayMap<Enum<?>, Object>();
	} else {
	    this.docMetadata = docMetadata;
	}
    }

    public void setContent(InputStream rawContent) {
	this.rawContent = rawContent;
	parsed = false;
    }

    public Object resolveNotNull(Enum<?> key) {
	return factory.resolveNotNull(key, docMetadata);
    }

    public Object resolve(Enum<?> key) {
	return factory.resolve(key, docMetadata);
    }

    public CharSequence uri() {
	try {
	    ensureParsed();
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
	return url;
    }

    // All fields use the same reader
    @Override
    public WordReader wordReader(final int field) {
	factory.ensureFieldIndex(field);
	return factory.wordReader;
    }
}