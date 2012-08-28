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

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class QueryCommand {
    public static enum QueryType {YAHOO, MG4J, DOCUMENT};
    
    @NotNull
    public QueryCommand.QueryType type = QueryType.YAHOO;
    @NotNull
    public String query;
    public String callback;
    @Min(0)
    public int pageStart;
    @Min(1)
    @Max(10000)
    public int pageSize = 10;
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