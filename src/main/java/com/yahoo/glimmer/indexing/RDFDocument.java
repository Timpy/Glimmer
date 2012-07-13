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
import java.util.ArrayList;
import java.util.List;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.IndexType;
import com.yahoo.glimmer.indexing.RDFDocumentFactory.RdfCounters;

public abstract class RDFDocument {
    public static final String NO_CONTEXT = "";

    // TODO RDFDocumnet should implement Hadoop's Writable but as we have a ref
    // to an InputStream for lazy parsing this isn't possible.
    protected final RDFDocumentFactory factory;
    /** Whether we already parsed the document. */
    private boolean parsed;
    /** The cached raw content. */
    private InputStream rawContent;
    private Integer id;
    private String subject;

    public abstract WordReader content(final int field) throws IOException;

    public abstract IndexType getIndexType();

    protected abstract void ensureParsed_(List<Relation> relations) throws IOException;

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
	// First part is subjects URL or BNode, second is the relations
	subject = line.substring(0, line.indexOf('\t')).trim();
	id = factory.lookupResource(subject);
	if (id == null) {
	    throw new IllegalStateException("Subject " + subject + " not in resources hash function!");
	}

	String relationsString = line.substring(line.indexOf('\t')).trim();
	if (relationsString.isEmpty()) {
	    factory.incrementCounter(RdfCounters.EMPTY_DOCUMENTS, 1);
	    return;
	}

	String[] relationsSplit = relationsString.split("  ");
	List<Relation> relations = new ArrayList<Relation>(relationsSplit.length);
	for (String relationLine : relationsSplit) {
	    try {
		Node[] relationNodes = NxParser.parseNodes(relationLine);
		Relation relation = new Relation(relationNodes);
		relations.add(relation);
	    } catch (ParseException e) {
		System.err.println("Parsing failed for " + subject + ": " + e.getMessage() + "Content was: \n" + line);
		return;
	    }
	}

	ensureParsed_(relations);
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
    
    protected static class Relation {
	private final Node[] nodes;
	public Relation(Node[] nodes) {
	    this.nodes = nodes;
	}
	
	public Node getSubject() {
	    return nodes[0];
	}
	public Node getPredicate() {
	    return nodes[1];
	}
	public Node getObject() {
	    return nodes[2];
	}
	public Node getContext() {
	    if (nodes.length > 3) {
		return nodes[3];
	    }
	    return null;
	}
    }
}