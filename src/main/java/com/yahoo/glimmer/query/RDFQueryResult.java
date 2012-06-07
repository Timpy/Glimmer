package com.yahoo.glimmer.query;

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
