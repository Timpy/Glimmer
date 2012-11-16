package com.yahoo.glimmer.indexing.preprocessor;

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

import org.semanticweb.yars.nx.Resource;

public class PredicatePrefixTupleFilter implements TupleFilter {
    private String urlPrefix;
    private boolean filterNonMatches;
    private boolean lowercase;
    private StringBuilder sb;

    public PredicatePrefixTupleFilter() {
	readResolve();
    }

    public void setUrlPrefix(String urlPrefix) {
	this.urlPrefix = urlPrefix;
	readResolve();
    }

    public void setLowercase(boolean lowercase) {
	this.lowercase = lowercase;
    }

    public void setFilterNonMatches(boolean filterNonMatches) {
	this.filterNonMatches = filterNonMatches;
    }

    // On instantiation by XStream Java's field initialization or default
    // constructor aren't used used.
    // XStream uses the same mechanism as the JDK serialization.
    private Object readResolve() {
	if (urlPrefix != null) {
	    sb = new StringBuilder(urlPrefix);
	}
	return this;
    }

    @Override
    public boolean filter(Tuple tuple) {
	if (tuple.predicate.type != TupleElement.Type.RESOURCE) {
	    return false;
	}

	String text = tuple.predicate.text;
	
	if (tuple.predicate.text.startsWith(urlPrefix)) {
	    if (lowercase) {
		text = text.toLowerCase();
	    }
	    int end = text.length();
	    while (text.charAt(--end) == '/') {
	    }

	    int start = text.lastIndexOf('/', end) + 1;
	    if (start > urlPrefix.length()) {
		// remove path
		sb.setLength(urlPrefix.length());
		sb.append(tuple.predicate.text.substring(start, end + 1));
		text = sb.toString();
	    }
	    if (!text.equals(tuple.predicate.text)) {
		tuple.predicate.text = text.toLowerCase();
		tuple.predicate.n3 = new Resource(tuple.predicate.text).toN3();
	    }
	    return true;
	} else if (!filterNonMatches) {
	    if (lowercase) {
		text = tuple.predicate.text.toLowerCase();
	    }
	} else {
	    return false;
	}
	
	if (!text.equals(tuple.predicate.text)) {
	    tuple.predicate.text = text;
	    tuple.predicate.n3 = new Resource(text).toN3();
	}
	return true;
    }
}
