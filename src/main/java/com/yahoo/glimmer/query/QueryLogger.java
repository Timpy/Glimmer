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

import it.unimi.dsi.mg4j.query.nodes.Query;

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

    private ThreadLocal<Long> threadLocalStartTime = new ThreadLocal<Long>();

    public void start() {
	threadLocalStartTime.set(System.currentTimeMillis());
    }

    public synchronized long endQuery(Query query, int numResults) {
	Long startTime = threadLocalStartTime.get();
	if (startTime == null) {
	    throw new IllegalStateException("start() wasn't called!!");
	}
	long duration = System.currentTimeMillis() - startTime;

	numQueries += 1;
	sumTime += duration;
	if (numQueries % ROLLING_WINDOW_SIZE == 0) {
	    rollingSum = duration;
	    rollingCount = 1;
	} else {
	    rollingSum += duration;
	    rollingCount += 1;
	}

	LOGGER.info("#" + LOG_SEPARATOR + numQueries + LOG_SEPARATOR + ((query != null) ? query.toString() : "") + LOG_SEPARATOR + duration + LOG_SEPARATOR
		+ ((double) sumTime / (double) numQueries) + LOG_SEPARATOR + rollingSum + LOG_SEPARATOR + rollingCount + LOG_SEPARATOR
		+ ((double) rollingSum / (double) rollingCount) + LOG_SEPARATOR + numResults);
	threadLocalStartTime.set(null);
	return duration;
    }
}
