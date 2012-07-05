package com.yahoo.glimmer.indexing;

import it.unimi.dsi.mg4j.document.DocumentCollection;

public class ConcatenatedDocumentCollection extends it.unimi.dsi.mg4j.document.ConcatenatedDocumentCollection {
    private static final long serialVersionUID = 2474118181169933929L;

    public ConcatenatedDocumentCollection(final String[] collectionName, final DocumentCollection[] collection) {
	super(collectionName, collection);
    }
}
