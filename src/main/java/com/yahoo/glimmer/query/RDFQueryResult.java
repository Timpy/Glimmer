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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.mg4j.query.nodes.Query;

public class RDFQueryResult {
    private final ObjectArrayList<RDFResultItem> resultItems;
    private final int numResults;
    private final long time;
    private final String query;
    private final String parsedQuery;

    public RDFQueryResult(String query, Query parsedQuery, int numResults, ObjectArrayList<RDFResultItem> resultItems, long time) {
	super();
	this.resultItems = resultItems;
	this.numResults = numResults;
	this.time = time;
	this.query = query != null ? query : "";
	this.parsedQuery = parsedQuery != null ? parsedQuery.toString() : "";
    }
    
    public ObjectArrayList<RDFResultItem> getResultItems() {
	return resultItems;
    }

    public int getNumResults() {
	return numResults;
    }

    public long getTime() {
	return time;
    }

    public String getQuery() {
	return query;
    }

    public String getParsedQuery() {
	return parsedQuery;
    }

    /**
     * Dereference object labels using a Collection
     * 
     */
    public void dereference(Object2LongFunction<CharSequence> mph) {
	for (RDFResultItem item : resultItems) {
	    item.dereference(mph);
	}
    }
}
