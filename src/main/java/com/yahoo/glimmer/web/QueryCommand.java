package com.yahoo.glimmer.web;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class QueryCommand {
    public static enum QueryType {YAHOO, MG4J};
    
    @NotNull
    public String query;
    @NotNull
    public QueryCommand.QueryType type = QueryType.YAHOO;
    public String callback;
    @Min(0)
    public int pageStart;
    @Min(1)
    @Max(10000)
    public int pageSize = 10;
    public boolean dedupe;
    public boolean deref;
    public String format;
    
    public String getQuery() {
        return query;
    }
    public void setQuery(String query) {
        this.query = query;
    }
    public QueryCommand.QueryType getType() {
        return type;
    }
    public void setType(QueryCommand.QueryType type) {
        this.type = type;
    }
    public String getCallback() {
        return callback;
    }
    public void setCallback(String callback) {
        this.callback = callback;
    }
    public int getPageStart() {
        return pageStart;
    }
    public void setPageStart(int pageStart) {
        this.pageStart = pageStart;
    }
    public int getPageSize() {
        return pageSize;
    }
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
    public boolean isDedupe() {
        return dedupe;
    }
    public void setDedupe(boolean dedupe) {
        this.dedupe = dedupe;
    }
    public boolean isDeref() {
        return deref;
    }
    public void setDeref(boolean deref) {
        this.deref = deref;
    }
    public String getFormat() {
	return format;
    }
    public void setFormat(String format) {
	this.format = format;
    }
}