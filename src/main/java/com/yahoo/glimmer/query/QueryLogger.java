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

import org.apache.log4j.Logger;

public class QueryLogger {
    private final static Logger LOGGER = Logger.getLogger(QueryLogger.class);
    public static final int ROLLING_WINDOW_SIZE = 50;
    public static final String LOG_SEPARATOR = "\t";

    /**
     * The number of queries seen so far (for statistical purposes).
     */
    private long numQueries;

    /**
     * The total time spent on queries so far in ms (for statistical purposes).
     */
    private long sumTime;

    /**
     * The number of queries within this rolling window.
     */
    private long rollingCount;

    /**
     * The total time spent on queries so far in ms (for statistical purposes).
     */
    private long rollingSum;

    public class QueryTimer {
	private final long startTime = System.currentTimeMillis();
	private int searchDuration;
	private int duration;
	
	public QueryTimer endSearch() {
	    if (searchDuration == 0) {
		searchDuration = (int) (System.currentTimeMillis() - startTime);
	    }
	    return this;
	}
	public QueryTimer end() {
	    if (duration == 0) {
		duration = (int) (System.currentTimeMillis() - startTime);
	    }
	    return this;
	}
	
	public long getStartTime() {
	    return startTime;
	}
	public int getSearchDuration() {
	    return searchDuration;
	}
	public int getDuration() {
	    return duration;
	}
    }

    public QueryTimer start() {
	return new QueryTimer();
    }

    public int endQuery(QueryTimer timer, String query, int numResults) {
	timer.endSearch().end();
	
	numQueries += 1;
	sumTime += timer.duration;
	if (numQueries % ROLLING_WINDOW_SIZE == 0) {
	    rollingSum = timer.duration;
	    rollingCount = 1;
	} else {
	    rollingSum += timer.duration;
	    rollingCount += 1;
	}

	LOGGER.info("#" + LOG_SEPARATOR + numQueries + LOG_SEPARATOR + query + LOG_SEPARATOR + timer.searchDuration + LOG_SEPARATOR + timer.duration + LOG_SEPARATOR
		+ ((double) sumTime / (double) numQueries) + LOG_SEPARATOR + rollingSum + LOG_SEPARATOR + rollingCount + LOG_SEPARATOR
		+ ((double) rollingSum / (double) rollingCount) + LOG_SEPARATOR + numResults);
	return timer.duration;
    }
}
