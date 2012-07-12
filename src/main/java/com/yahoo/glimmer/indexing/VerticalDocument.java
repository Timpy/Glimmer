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
import org.openrdf.model.Value;
import org.semanticweb.yars.nx.namespace.RDF;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.IndexType;
import com.yahoo.glimmer.indexing.RDFDocumentFactory.RdfCounters;

/**
 * A RDF document.
 * 
 * <p>
 * We delay the actual parsing until it is actually necessary, so operations
 * like getting the document URI will not require parsing.
 */

class VerticalDocument extends RDFDocument {
    private List<List<String>> fields = new ArrayList<List<String>>();

    protected VerticalDocument(VerticalDocumentFactory factory) {
	super(factory);
    }

    @Override
    public IndexType getIndexType() {
	return IndexType.VERTICAL;
    }

    protected void ensureParsed_(StatementCollectorHandler handler) throws IOException {
	// Initialize fields
	fields.clear();
	for (int i = 0; i < factory.numberOfFields(); i++) {
	    fields.add(new ArrayList<String>());
	}

	for (Statement stmt : handler.getStatements()) {

	    String predicate = stmt.getPredicate().toString();

	    String fieldName = predicate;

	    // Check if prefix is on blacklist
	    if (RDFDocumentFactory.isOnPredicateBlacklist(fieldName)) {
		factory.incrementCounter(RdfCounters.BLACKLISTED_TRIPLES, 1);
		continue;
	    }

	    // Determine whether we need to index, and the field
	    int fieldIndex = factory.fieldIndex(fieldName);
	    if (fieldIndex == -1) {
		System.err.println("Field not indexed: " + fieldName);
		factory.incrementCounter(RdfCounters.UNINDEXED_PREDICATE_TRIPLES, 1);
		continue;
	    }

	    if (stmt.getObject() instanceof Resource) {
		// For all fields except type, encode the resource URI
		// or bnode ID using the resources hash
		if (predicate.equals(RDF.TYPE.toString())) {
		    factory.incrementCounter(RdfCounters.RDF_TYPE_TRIPLES, 1);
		    fields.get(fieldIndex).add(stmt.getObject().toString());
		} else {
		    Integer objectId = factory.lookupResource(stmt.getObject().stringValue());
		    if (objectId == null) {
			throw new IllegalStateException("Object " + stmt.getObject().toString() + " not in resources hash function!");
		    }
		    fields.get(fieldIndex).add(objectId.toString());
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
	    factory.incrementCounter(RdfCounters.INDEXED_TRIPLES, 1);
	}
    }
    
    @Override
    public WordReader content(final int field) throws IOException {
	factory.ensureFieldIndex(field);
	ensureParsed();
	return new WordArrayReader(fields.get(field));
    }
}