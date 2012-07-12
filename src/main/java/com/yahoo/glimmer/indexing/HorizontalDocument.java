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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.semanticweb.yars.nx.namespace.RDF;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.IndexType;
import com.yahoo.glimmer.indexing.RDFDocumentFactory.RdfCounters;
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

    protected void ensureParsed_(StatementCollectorHandler handler) throws IOException {
	tokens.clear();
	properties.clear();
	contexts.clear();
	uri.clear();

	// Index URI except the protocol, if exists
	FastBufferedReader fbr;
	MutableString word = new MutableString();
	MutableString nonWord = new MutableString();
	int firstColon = getSubject().indexOf(':');
	if (firstColon < 0) {
	    fbr = new FastBufferedReader(new StringReader(getSubject()));
	} else {
	    fbr = new FastBufferedReader(new StringReader(getSubject().substring(firstColon + 1)));
	}
	while (fbr.next(word, nonWord)) {
	    if (word != null && !word.equals("")) {
		if (CombinedTermProcessor.getInstance().processTerm(word)) {
		    uri.add(word.toString().toLowerCase());
		}
	    }
	}
	fbr.close();

	for (Statement stmt : handler.getStatements()) {
	    String predicate = stmt.getPredicate().toString();
	    String fieldName = Util.encodeFieldName(predicate);

	    // Check if prefix is on blacklist
	    if (RDFDocumentFactory.isOnPredicateBlacklist(fieldName)) {
		factory.incrementCounter(RdfCounters.BLACKLISTED_TRIPLES, 1);
		continue;
	    }

	    String context = NO_CONTEXT;
	    if (factory.isWithContexts() && stmt.getContext() != null) {
		context = stmt.getContext().toString();
		Integer contextId = factory.lookupResource(context);
		if (contextId == null) {
		    throw new IllegalStateException("Context " + context + " not in resources hash function!");
		}
		context = contextId.toString();
	    }

	    if (stmt.getObject() instanceof Resource) {
		if (predicate.equals(RDF.TYPE.toString())) {
		    factory.incrementCounter(RdfCounters.RDF_TYPE_TRIPLES, 1);
		    tokens.add(stmt.getObject().toString());
		} else {
		    Integer objectId = factory.lookupResource(stmt.getObject().toString());
		    if (objectId == null) {
			throw new IllegalStateException("Object " + stmt.getObject().toString() + " not in resources hash function!");
		    }
		    tokens.add(objectId.toString());
		}
		properties.add(fieldName);
		contexts.add(context);
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
			    contexts.add(context);
			}

		    }
		}
		fbr.close();
	    }

	    factory.incrementCounter(RdfCounters.INDEXED_TRIPLES, 1);
	}
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