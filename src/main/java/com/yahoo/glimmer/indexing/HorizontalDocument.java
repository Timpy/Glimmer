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

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.semanticweb.yars.nx.namespace.RDF;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.RdfCounters;
import com.yahoo.glimmer.indexing.RDFDocumentFactory.IndexType;
import com.yahoo.glimmer.util.Util;

/**
 * A RDF document.
 * 
 * <p>
 * We delay the actual parsing until it is actually necessary, so operations
 * like getting the document URI will not require parsing.
 */

class HorizontalDocument extends RDFDocument {
    /** Field content **/
    private List<String> tokens = new ArrayList<String>();
    private List<String> properties = new ArrayList<String>();
    private List<String> contexts = new ArrayList<String>();
    // uri is strings extracted from the url
    private List<String> uri = new ArrayList<String>();

    protected HorizontalDocument(HorizontalDocumentFactory factory) {
	super(factory);
    }

    @Override
    public IndexType getIndexType() {
	return IndexType.HORIZONTAL;
    }

    protected void ensureParsed() throws IOException {
	if (parsed) {
	    return;
	}
	parsed = true;

	if (rawContent == null) {
	    rawContent = new ByteArrayInputStream(new byte[0]);
	}

	tokens.clear();
	properties.clear();
	contexts.clear();
	uri.clear();

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
	// First part is URL, second part is docfeed
	url = line.substring(0, line.indexOf('\t')).trim();
	String data = line.substring(line.indexOf('\t')).trim();

	if (data.trim().equals("")) {
	    factory.incrementCounter(RdfCounters.EMPTY_DOCUMENTS, 1);
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
		if (CombinedTermProcessor.getInstance().processTerm(word)) {
		    uri.add(word.toString().toLowerCase());
		}
	    }
	}
	fbr.close();

	// Docfeed parsing
	StatementCollectorHandler handler;
	try {
	    handler = factory.parseStatements(url, data);
	} catch (IOException e) {
	    throw e;
	} catch (Exception e) {
	    System.err.println("Parsing failed for " + url + ": " + e.getMessage() + "Content was: \n" + line);
	    return;
	}

	for (Statement stmt : handler.getStatements()) {
	    String predicate = stmt.getPredicate().toString();
	    String fieldName = Util.encodeFieldName(predicate);

	    // Check if prefix is on blacklist
	    if (RDFDocumentFactory.isOnPredicateBlacklist(fieldName)) {
		factory.incrementCounter(RdfCounters.BLACKLISTED_TRIPLES, 1);
		continue;
	    }

	    String context = NULL_URL;
	    if (factory.isWithContexts() && stmt.getContext() != null) {
		String s = stmt.getContext().toString();
		Long l = ResourcesHashLoader.lookup(s);
		if (l == null) {
		    throw new IllegalStateException("Context " + stmt.getContext().toString() + " not in resources hash function!");
		}
		context = l.toString();
	    }

	    if (stmt.getObject() instanceof Resource) {
		if (predicate.equals(RDF.TYPE.toString())) {
		    factory.incrementCounter(RdfCounters.RDF_TYPE_TRIPLES, 1);
		    tokens.add(stmt.getObject().toString());
		} else {
		    tokens.add(ResourcesHashLoader.lookup(stmt.getObject().toString()).toString());
		}
		properties.add(fieldName);
		if (context != null) {
		    contexts.add(context);
		}
	    } else {
		String object = ((Literal) stmt.getObject()).getLabel();
		// Iterate over the words of the value
		fbr = new FastBufferedReader(new StringReader(object));
		while (fbr.next(word, nonWord)) {
		    if (word != null && !word.equals("")) {
			if (CombinedTermProcessor.getInstance().processTerm(word)) {
			    // Lowercase terms
			    tokens.add(word.toString());

			    // Preserve casing for properties and
			    // contexts
			    properties.add(fieldName);

			    if (context != null) {
				contexts.add(context);
			    }
			}

		    }
		}
		fbr.close();
	    }

	    factory.incrementCounter(RdfCounters.INDEXED_TRIPLES, 1);
	}
    }

    public String toString() {
	return uri().toString();
    }

    @Override
    public WordReader content(final int field) throws IOException {
	factory.ensureFieldIndex(field);
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
}