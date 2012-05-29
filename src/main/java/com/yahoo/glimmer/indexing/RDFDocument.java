package com.yahoo.glimmer.indexing;

import java.io.InputStream;

import it.unimi.dsi.mg4j.document.AbstractDocument;

abstract public class RDFDocument extends AbstractDocument {

    abstract public void setContent(InputStream rawContent);

}
