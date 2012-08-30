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

import it.unimi.dsi.mg4j.query.nodes.Query;
import it.unimi.dsi.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.dsi.mg4j.query.parser.QueryParserException;
import it.unimi.dsi.mg4j.query.parser.SimpleParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Resource;
import javax.validation.Valid;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.yahoo.glimmer.query.RDFIndex;
import com.yahoo.glimmer.query.RDFIndexStatistics;
import com.yahoo.glimmer.query.RDFQueryResult;

@Controller()
public class QueryController {
    public final static String INDEX_KEY = "index";
    public final static String OBJECT_KEY = "object";
    
    private IndexMap indexMap;
    private Querier querier;
    
    /// For every request populate the dataset model attribute from the request parameter.
    @ModelAttribute(INDEX_KEY)
    public RDFIndex getIndex(@RequestParam(required=false) String index) {
	if (index != null) {
            RDFIndex rdfIndex = indexMap.get(index);
            if (rdfIndex == null) {
                throw new IllegalArgumentException("No index found with name:" + index);
            }
            return rdfIndex;
	} else {
	    return null;
	}
    }

    @RequestMapping(value = "/dataSetList", method = RequestMethod.GET)
    public Map<String, ?> getDataSetList(@RequestParam(required = false) String callback) {
	return Collections.singletonMap(OBJECT_KEY, indexMap.keySet());
    }
    
    @RequestMapping(value = "/indexStatistics", method = RequestMethod.GET)
    public Map<String, ?> getIndextStatistics(@ModelAttribute(INDEX_KEY) RDFIndex index, @RequestParam(required = false) String callback) {
	if (index == null) {
	    throw new IllegalArgumentException("No index given");
	}
	
	RDFIndexStatistics statistics = index.getStatistics();
	return Collections.singletonMap(OBJECT_KEY, statistics);
    }

    @RequestMapping(value = "/query", method = RequestMethod.GET)
    public Map<String, ?> query(@ModelAttribute(INDEX_KEY) RDFIndex index, @Valid QueryCommand command) throws QueryParserException, QueryBuilderVisitorException, IOException {
	if (index == null) {
	    throw new IllegalArgumentException("No index given");
	}
	
	String rawQuery;
	Query query;
	RDFQueryResult result;
	switch (command.getType()) {
	case MG4J:
	    rawQuery = decodeEntities(command.getQuery());
	    query = new SimpleParser().parse(rawQuery);
	    result = querier.doQuery(index, query, command.getPageStart(), command.getPageSize(), command.isDeref());
	    break;
	case YAHOO:
	    rawQuery = decodeEntities(command.getQuery());
	    try {
		query = index.getParser().parse(rawQuery);
	    } catch (QueryParserException e) {
		throw new IllegalArgumentException("Query failed to parse");
	    }
	    result = querier.doQuery(index, query, command.getPageStart(), command.getPageSize(), command.isDeref());
	    break;
	case DOCUMENT:
	    rawQuery = command.getQuery().trim();
	    Integer id;
	    if (rawQuery.isEmpty()) {
		throw new IllegalArgumentException("Query is empty");
	    }
	    if (Character.isDigit(rawQuery.charAt(0))) {
		try {
		    id = Integer.parseInt(rawQuery);
		} catch (NumberFormatException e) {
		    throw new IllegalArgumentException("Query failed to parse as a numeric subject ID(int)");
		}
	    } else {
		id = index.getSubjectId(rawQuery);
		if (id == null) {
		    throw new IllegalArgumentException("subject " + rawQuery + " is not in collection.");
		}
	    }
	    result = querier.doQueryForDocId(index, id, command.isDeref());
	    break;
	default:
	    throw new IllegalArgumentException("No query type given.");
	}

	return Collections.singletonMap(OBJECT_KEY, result);
    }

    public static String decodeEntities(String query) {
	if (query == null || query.equals("")) {
	    return null;
	}

	String result = query.replaceAll("&quot;", "\"");
	result = result.replaceAll("&#39;", "'");
	result = result.replaceAll("&#92;", "\\\\");
	return result;
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
