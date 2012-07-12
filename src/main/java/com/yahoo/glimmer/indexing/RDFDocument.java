package com.yahoo.glimmer.indexing;

/*
 * Copyright (c) 2012 Yahoo! Inc. All rights reserved.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is 
 *  distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *  See accompanying LICENSE file.
 */

import it.unimi.dsi.io.WordReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.IndexType;
import com.yahoo.glimmer.indexing.RDFDocumentFactory.RdfCounters;

public abstract class RDFDocument {
    public static final String NO_CONTEXT = "";
    
    // TODO RDFDocumnet should implement Hadoop's Writable but as we have a ref to an InputStream for lazy parsing this isn't possible.
    protected final RDFDocumentFactory factory;
    /** Whether we already parsed the document. */
    private boolean parsed;
    /** The cached raw content. */
    private InputStream rawContent;
    private Integer id;
    private String subject;

    public abstract WordReader content(final int field) throws IOException;
    public abstract IndexType getIndexType();
    protected abstract void ensureParsed_(StatementCollectorHandler handler) throws IOException;

    public RDFDocument(RDFDocumentFactory factory) {
	this.factory = factory;
    }

    public void setContent(InputStream rawContent) {
	this.rawContent = rawContent;
	parsed = false;
	id = null;
	subject = null;
    }
    
    protected void ensureParsed() throws IOException {
	if (parsed) {
	    return;
	}
	parsed = true;

	if (rawContent == null) {
	    throw new IOException("Trying to parse null rawContent");
	}

	BufferedReader r = new BufferedReader(new InputStreamReader(rawContent, factory.getInputStreamEncodeing()));
	String line = r.readLine();
	r.close();

	if (line == null || line.trim().equals("")) {
	    factory.incrementCounter(RdfCounters.EMPTY_LINES, 1);
	    return;
	}
	// First part is subject URL, second part is it's relations
	subject = line.substring(0, line.indexOf('\t')).trim();
	id = factory.lookupResource(subject);
	if (id == null) {
	    throw new IllegalStateException("Subject " + subject + " not in resources hash function!");
	}

	String relations = line.substring(line.indexOf('\t')).trim();
	if (relations.isEmpty()) {
	    factory.incrementCounter(RdfCounters.EMPTY_DOCUMENTS, 1);
	    return;
	}
	
	// parsing relations
	StatementCollectorHandler handler;
	try {
	    handler = factory.parseStatements(subject, relations);
	} catch (IOException e) {
	    throw e;
	} catch (Exception e) {
	    System.err.println("Parsing failed for " + subject + ": " + e.getMessage() + "Content was: \n" + line);
	    return;
	}
	
	ensureParsed_(handler);
    }
    
    public int getId() {
	try {
	    ensureParsed();
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
	return id;
    }

    public String getSubject() {
	try {
	    ensureParsed();
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
	return subject;
    }
    
    public String toString() {
	return getSubject().toString();
    }
}