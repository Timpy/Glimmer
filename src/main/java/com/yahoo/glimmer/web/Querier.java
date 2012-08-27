package com.yahoo.glimmer.web;

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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.mg4j.query.SelectedInterval;
import it.unimi.dsi.mg4j.query.nodes.Query;
import it.unimi.dsi.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.dsi.mg4j.search.score.DocumentScoreInfo;

import java.io.IOException;
import java.io.Reader;

import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.yahoo.glimmer.query.QueryLogger;
import com.yahoo.glimmer.query.RDFIndex;
import com.yahoo.glimmer.query.RDFQueryResult;
import com.yahoo.glimmer.query.RDFResultItem;
import com.yahoo.glimmer.util.Util;

/**
 * Wraps the details of doing a query against an RDFIndex.
 * 
 */
public class Querier {
    private final static Logger LOGGER = Logger.getLogger(Querier.class);
    private static final String RELATION_END = " .";
    private static final String RELATION_DELIMITOR = "  ";
    private static final String DEFAULT_CONTEXT = "default:";

    public RDFQueryResult doQuery(RDFIndex index, Query query, int startItem, int maxNumItems, boolean deref) throws QueryBuilderVisitorException, IOException {
	if (startItem < 0 || maxNumItems < 0 || maxNumItems > 10000) {
	    throw new IllegalArgumentException("Bad item range - start:" + startItem + " maxNumItems:" + maxNumItems);
	}

	ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>();

	int numResults = index.process(query, startItem, maxNumItems, results);

	if (results.size() > maxNumItems) {
	    results.size(maxNumItems);
	}

	ObjectArrayList<RDFResultItem> resultItems = new ObjectArrayList<RDFResultItem>();
	if (!results.isEmpty()) {
	    for (int i = 0; i < results.size(); i++) {
		DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi = results.get(i);
		LOGGER.debug("Intervals for item " + i);
		LOGGER.debug("score " + dsi.score);
		RDFResultItem item = createRdfResultItem(index, dsi.document, dsi.score, deref);
		if (item == null) {
		    throw new IllegalStateException("Document id " + dsi.document + " isn't in collection(or has null content).");
		}
		resultItems.add(item);
	    }
	}

	// Stop the timer
	long time = 0;
	QueryLogger queryLogger = index.getQueryLogger();
	if (queryLogger != null) {
	    queryLogger.endQuery(query, numResults);
	    time = queryLogger.getTime();
	}

	RDFQueryResult result = new RDFQueryResult(null, query != null ? query.toString() : "", numResults, resultItems, time);
	return result;
    }

    public static RDFResultItem createRdfResultItem(RDFIndex index, int docId, double score, boolean lookupObjectLabels) throws IOException {
	Document doc = index.getDocument(docId);
	if (doc == null || doc.title().length() == 0) {
	    return null;
	}
	RDFResultItem item = new RDFResultItem();
	item.setDocId(docId);
	item.setScore(score);
	String subject = doc.title().toString();
	item.setSubject(subject);

	Reader r = (Reader) doc.content(0);
	// We need to copy the WordReader otherwise the nested calls to createRdfResultItem() share the same instance.
	WordReader wr = doc.wordReader(0).copy().setReader(r);
	StringBuilder sb = new StringBuilder();
	MutableString word = new MutableString();
	MutableString nonWord = new MutableString();
	while (wr.next(word, nonWord)) {
	    int i = nonWord.indexOf(RELATION_END + RELATION_DELIMITOR);
	    if (i >= 0) {
		sb.append(word);
		i += RELATION_END.length();
		sb.append(nonWord.substring(0, i));
		parseRelation(index, sb, item, lookupObjectLabels);
		sb.setLength(0);
		i += RELATION_DELIMITOR.length();
		sb.append(nonWord.substring(i));
	    } else {
		sb.append(word);
		sb.append(nonWord);
	    }
	}
	parseRelation(index, sb, item, lookupObjectLabels);
	doc.close();

	return item;
    }

    private static boolean parseRelation(RDFIndex index, StringBuilder relationSb, RDFResultItem item, boolean lookupObjectLabels) throws IOException {
	if (relationSb.length() == 0) {
	    return false;
	}
	String relation = relationSb.toString().trim();
	if (relation.isEmpty()) {
	    return false;
	}
	Node[] predicateObjectContext;
	try {
	    predicateObjectContext = NxParser.parseNodes(relation);
	} catch (Exception e) {
	    System.err.println("Error parsing tuple: " + relation);
	    return false;
	}

	String predicate = predicateObjectContext[0].toString();
	String object = predicateObjectContext[1].toString();
	String context;
	if (predicateObjectContext.length > 2) {
	    context = predicateObjectContext[2].toString();
	} else {
	    context = DEFAULT_CONTEXT;
	}
	boolean indexed = index.getIndexedPredicates().contains(Util.encodeFieldName(predicate));

	String label = null;
	// if predicate is an rdfs:label or woo:label, assign the object as the
	// items label
	// TODO. Consider ...name too.
	if (predicate.endsWith("label") || predicate.endsWith("name")) {
	    item.setLabel(object);
	    label = object;
	}

	if (label == null && lookupObjectLabels) {
	    // If the object is also a subject Resource/BNode this will
	    // return
	    // that subjects id with is the same as the docId.
	    Long subjectIdForObject = index.getSubjectId(object);
	    if (subjectIdForObject != null) {
		// Parse the subject doc that this object refers too..
		RDFResultItem objectItem = createRdfResultItem(index, subjectIdForObject.intValue(), 0.0d, false);
		if (objectItem != null) {
		    label = objectItem.getLabel();
		}
	    }
	}

	item.addRelation(predicate, object, context, indexed, label);
	return true;
    }
}
