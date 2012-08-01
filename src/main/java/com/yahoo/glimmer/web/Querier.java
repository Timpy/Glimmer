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
import it.unimi.di.mg4j.index.Index;
import it.unimi.di.mg4j.query.SelectedInterval;
import it.unimi.di.mg4j.query.nodes.Query;
import it.unimi.di.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.mg4j.search.score.DocumentScoreInfo;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.yahoo.glimmer.query.QueryLogger;
import com.yahoo.glimmer.query.RDFIndex;
import com.yahoo.glimmer.query.RDFQueryResult;
import com.yahoo.glimmer.query.RDFResultItem;

/**
 * Wraps the details of doing a query against an RDFIndex.
 * 
 */
public class Querier {
    private final static Logger LOGGER = Logger.getLogger(Querier.class);
    
    public RDFQueryResult doQuery(RDFIndex index, Query query, int startItem, int maxNumItems, boolean deref) throws QueryBuilderVisitorException, IOException {
	if (startItem < 0 || maxNumItems < 0 || maxNumItems > 10000) {
	    throw new IllegalArgumentException("Bad item range - start:" + startItem + " maxNumItems:" + maxNumItems);
	}

	// Reconfigure scorer
	// TODO is this supposed to redifine the scoring for everyone or just this query?
//	if (context != null) {
//	    try {
//		context.reload();
//		context.update(request);
//		LOGGER.info("Reconfiguring scorer");
//		index.reconfigure(context);
//	    } catch (Exception e) {
//		e.printStackTrace();
//	    }
//	}

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
		final RDFResultItem resultItem = new RDFResultItem(index.getIndexedFields(), index.getCollection(), dsi.document, dsi.score);
		resultItems.add(resultItem);

	    }
	}

	// Stop the timer
	long time = 0;
	QueryLogger queryLogger = index.getQueryLogger();
	if (queryLogger != null) {
	    queryLogger.endQuery(query, numResults);
	    time = queryLogger.getTime();
	}
	RDFQueryResult result = new RDFQueryResult(null, query, numResults, resultItems, time);

	// Dereferencing
	if (deref) {
	    time = System.currentTimeMillis();
	    result.dereference(index.getSubjectsMPH());
	    LOGGER.info("Dereferencing took " + (System.currentTimeMillis() - time) + " ms");
	}
	return result;
    }
}
