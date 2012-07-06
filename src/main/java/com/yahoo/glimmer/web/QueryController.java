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
import it.unimi.dsi.mg4j.query.nodes.Query;
import it.unimi.dsi.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.dsi.mg4j.query.parser.QueryParserException;
import it.unimi.dsi.mg4j.query.parser.SimpleParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Resource;
import javax.validation.Valid;

import org.apache.log4j.Logger;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.yahoo.glimmer.query.IndexStatistics;
import com.yahoo.glimmer.query.QueryLogger;
import com.yahoo.glimmer.query.RDFIndex;
import com.yahoo.glimmer.query.RDFQueryResult;
import com.yahoo.glimmer.query.RDFResultItem;
import com.yahoo.glimmer.query.Util;

@Controller()
public class QueryController {
    private final static Logger LOGGER = Logger.getLogger(QueryController.class);
    public final static String INDEX_KEY = "index";
    public final static String OBJECT_KEY = "object";
    
    private IndexMap indexMap;
    private Querier querier;
    
    /// For every request populate the index model attribute from the request parameter.
    @ModelAttribute(INDEX_KEY)
    public RDFIndex getIndex(@RequestParam(INDEX_KEY) String indexName) {
        RDFIndex index = indexMap.get(indexName);
        if (index == null) {
            throw new RuntimeException("No index found with name:" + indexName);
        }
        return index;
    }

    @RequestMapping(value = "/indexStatistics", method = RequestMethod.GET)
    public Map<String, ?> getIndextStatistics(@ModelAttribute(INDEX_KEY) RDFIndex index, @RequestParam(required = false) String callback) {
	IndexStatistics statistics = index.getStatistics();
	return Collections.singletonMap(OBJECT_KEY, statistics);
    }

    @RequestMapping(value = "/getDocument", method = RequestMethod.GET)
    public Map<String, ?> getDocument(@ModelAttribute(INDEX_KEY) RDFIndex index, @RequestParam(required = false) String callback, @RequestParam(required = false) Long id, @RequestParam(required = false) String subject) throws IOException {
	LOGGER.info("id=" + id + " subject=" + subject);
	
	
	if (id == null) {
	    if (subject == null) {

	    } else {
		if (index.getSubjectsMPH() == null) {
		    throw new HttpMessageNotReadableException("mph needs to be loaded for subject to work.");
		} else {
		    id = index.getDocID(subject);
		}
	    }
	}

	if (id == -1 || id >= index.getSubjectsMPH().size64()) {
	    throw new HttpMessageNotReadableException("subject not in collection.");
	}

	ObjectArrayList<RDFResultItem> resultItems = new ObjectArrayList<RDFResultItem>();
	int numResults;
	final RDFResultItem resultItem = new RDFResultItem(index.getIndexedFields(), index.getCollection(), id.intValue(), 1.0d);
	if (index.getCollection() != null && subject != null && !resultItem.uri().equals(subject)) {
	    // Ignore the result if the MPH tricked us and returned a
	    // result with a different URI
	    numResults = 0;
	} else {
	    resultItems.add(resultItem);
	    numResults = 1;
	}

	// Stop the timer
	QueryLogger queryLogger = index.getQueryLogger();

	long time;
	if (queryLogger != null) {
	    queryLogger.endQuery(null, numResults);
	    time = queryLogger.getTime();
	} else {
	    time = 0;
	}

	RDFQueryResult result = new RDFQueryResult("", null, numResults, resultItems, time);
	return Collections.singletonMap(OBJECT_KEY, result);
    }

    @RequestMapping(value = "/query", method = RequestMethod.GET)
    public Map<String, ?> query(@ModelAttribute(INDEX_KEY) RDFIndex index, @Valid QueryCommand command) throws QueryParserException, QueryBuilderVisitorException, IOException {
	String rawQuery;
	Query query;
	switch (command.getType()) {
	case MG4J:
	    rawQuery = Util.decodeEntities(command.getQuery());
	    query = new SimpleParser().parse(rawQuery);
	    break;
	case YAHOO:
	    rawQuery = Util.decodeEntities(command.getQuery());
	    try {
		query = index.getParser().parse(rawQuery);
	    } catch (QueryParserException e) {
		throw new HttpMessageNotReadableException("Query failed to parse");
	    }
	    break;
	default:
	    throw new HttpMessageNotReadableException("No query type given.");
	}

	RDFQueryResult result = querier.doQuery(index, query, command.getPageStart(), command.getPageSize(), command.isDeref());
	return Collections.singletonMap(OBJECT_KEY, result);
    }

    @Resource
    public void setQuerier(Querier querier) {
	this.querier = querier;
    }
    @Resource
    public void setIndexMap(IndexMap indexMap) {
	this.indexMap = indexMap;
    }
}
