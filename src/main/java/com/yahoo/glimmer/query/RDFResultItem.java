package com.yahoo.glimmer.query;

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

import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentCollection;
import it.unimi.dsi.mg4j.query.ResultItem;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Quad;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Triple;
import org.semanticweb.yars.nx.parser.NxParser;


public class RDFResultItem extends ResultItem {

    private static final String DOUBLE_SPACES = "  ";

    private static final Node DEFAULT_CONTEXT = new Resource("default:");

    private List<Value> quads = new ArrayList<Value>();
    private final Node subject;

    private Set<RDFResultItem> duplicates = new HashSet<RDFResultItem>();

    public List<Value> getValues() {
	return quads;
    }

    public void addDuplicate(RDFResultItem duplicate) {
	duplicates.add(duplicate);
    }

    public Set<RDFResultItem> getDuplicates() {
	return duplicates;
    }

    public static class Value {
	public Triple triple;
	public Set<Node> source;
	public String field;
	public boolean indexed;
	public String label;

	public Value(Triple triple, Set<Node> source, String field, boolean isIndexed, String label) {
	    this.triple = triple;
	    this.source = source;
	    this.field = field;
	    this.indexed = isIndexed;
	    this.label = label;
	}
    }

    // Label of this object from rdfs:label or any property ending with label
    public String label = null;

    public String getLabel() {
	return label;
    }

    private transient Set<String> fieldNames;
    private transient DocumentCollection collection;

    public RDFResultItem(Set<String> fieldNames, DocumentCollection collection, int id, double score) throws IOException {
	super(id, score);

	this.fieldNames = fieldNames;
	this.collection = collection;

	if (collection != null) {
	    Document d = collection.document(id);
	    title = d.title();
	    uri = d.uri();
	    String titleString = title.toString();
	    if (titleString.startsWith("http://")) {
		subject = new Resource(titleString);
	    } else {
		subject = new BNode(titleString);
	    }
	    parseData(subject, getText(d).toString(), fieldNames);
	    d.close();
	} else {
	    title = "Document #" + doc;
	    subject = null;
	}
    }

    /**
     * Return all distinct values for a given predicate
     * 
     * @param field
     * @param readthru
     *            return values from duplicates
     * @return
     */
    public List<Node> getValues(String field, boolean readthru) {
	List<Node> results = new ArrayList<Node>();
	for (Value quad : quads) {
	    if (quad.field.equals(field)) {
		results.add(quad.triple.getObject());
	    }
	}
	if (readthru) {
	    for (RDFResultItem dupe : duplicates) {
		for (Value quad : dupe.quads) {
		    if (quad.field.equals(field)) {
			results.add(quad.triple.getObject());
		    }
		}
	    }
	}
	return results;
    }

    /**
     * Return all sources that provided triples
     * 
     * @return
     */
    public Set<String> getSources() {
	Set<String> sources = new HashSet<String>();
	for (Value quad : quads) {
	    for (Node src : quad.source) {
		sources.add(src.toString());
	    }
	}
	return sources;
    }

    /**
     * Return all text content concatenated, separated by newline
     * 
     * @return
     */
    public String getText() {
	StringBuffer result = new StringBuffer();
	for (Value quad : quads) {
	    if (quad.triple.getObject() instanceof Literal) {
		result.append(((Literal) quad.triple.getObject()).getUnescapedData() + "\n");
	    }
	}
	return result.toString();
    }

    private void parseData(Set<Triple> data, Set<String> fieldNames) {
	Map<Triple, Set<Node>> quadsByTriple = new HashMap<Triple, Set<Node>>();

	for (Triple triple : data) {
	    Set<Node> values = quadsByTriple.get(triple);
	    if (values == null) {
		values = new HashSet<Node>();
		quadsByTriple.put(triple, values);
	    }
	    Node context = new Literal("default");
	    if (triple.toArray().length > 3) {
		context = triple.toArray()[3];
	    }
	    values.add(context);
	}

	for (Triple key : quadsByTriple.keySet()) {
	    // indexField will be null for triples with predicates that are not
	    // indexed.
	    String field = com.yahoo.glimmer.util.Util.encodeFieldName(((Resource) key.getPredicate()).toString());
	    boolean isIndexed = false;
	    for (String fn : fieldNames) {
		if (field.equalsIgnoreCase(fn)) {
		    isIndexed = true;
		    break;
		}
	    }

	    // if rdfs:label or woo:label found, use either to assign a value to
	    // label
	    if (key.getPredicate().toString().endsWith("label")) {
		label = key.getObject().toString();
	    }

	    quads.add(new Value(key, quadsByTriple.get(key), field, isIndexed, null));
	}

    }

    private void parseData(Node subject, String unparsed, Set<String> fieldNames) {

	String[] tuples = unparsed.split(DOUBLE_SPACES);
	Set<Triple> data = new HashSet<Triple>();
	for (int i = 0; i < tuples.length; i++) {
	    String tuple = tuples[i];
	    try {
		if (tuple.trim().equals("")) {
		    // Empty line
		    continue;
		}
		Node[] predicateObjectContext = NxParser.parseNodes(tuple);

		if (predicateObjectContext.length < 3) {
		    data.add(new Quad(subject, predicateObjectContext[0], predicateObjectContext[1], DEFAULT_CONTEXT));
		} else {
		    data.add(new Quad(subject, predicateObjectContext[0], predicateObjectContext[1], predicateObjectContext[2]));
		}

	    } catch (Exception e) {
		System.err.println("Error parsing tuple: " + tuple);
		e.printStackTrace();
	    }
	}

	parseData(data, fieldNames);
    }

    public void dereference(Object2LongFunction<CharSequence> mph) {
	for (Value value : quads) {
	    Node object = value.triple.getObject();
	    if (object instanceof Resource) {
		// Create a result item for this object... this might fail
		long id = mph.get(object.toString());
		try {
		    RDFResultItem objectItem = new RDFResultItem(fieldNames, collection, (int) id, 0.0d);
		    if (objectItem.label != null) {
			value.label = objectItem.label;
		    }
		} catch (IOException e) {
		    e.printStackTrace();
		} catch (java.lang.IndexOutOfBoundsException e) {

		}

	    }
	}

    }

    public String toString() {
	StringBuffer sb = new StringBuffer();
	for (Value quad : quads) {
	    sb.append(quad.triple.getSubject() + " " + quad.triple.getPredicate() + " " + quad.triple.getObject() + " " + quad.source + "\n");
	}
	return sb.toString();
    }
    
    public static String getText(Document d) throws IOException {

	// should depend on the factory and the factory fields, but the
	// collection we have has only one field - > spit it all
	Reader r = (Reader) d.content(0);
	WordReader rr = d.wordReader(0).setReader(r);
	MutableString text = new MutableString();
	MutableString word = new MutableString(), nonWord = new MutableString();
	try {
	    while (rr.next(word, nonWord)) {
		text.append(word);
		text.append(nonWord);
	    }
	} catch (IOException e) {
	    // throw new RuntimeException( e );
	    e.printStackTrace();
	}
	return text.toString();
    }
}
