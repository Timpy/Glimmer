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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer(and Combiner)
 * 
 * For the given Subject resource key concatanates all <predicate> <object>
 * <context> . for that key + appends PREDICATE, OBJECT and/or CONTEXT keywords
 * if that keyword occurs once or more as a value.
 * 
 */
public class ResourcesReducer extends Reducer<Text, Text, Text, Text> {
    public static final String VALUE_DELIMITER = "  ";
    private boolean keyPredicate;
    private boolean keyObject;
    private boolean keyContext;
    private StringBuilder relations = new StringBuilder();
    private String[] singleSplit = new String[1];

    public enum Counters {
	TOO_MANY_RELATIONS, LONG_TUPLES;
    }

    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	relations.setLength(0);
	int valueCount = 0;
	for (Text value : values) {
	    String valueString = value.toString();
	    valueCount++;
	    if (valueCount > 10000) {
		System.out.println("Too many relations for subject:" + key.toString());
		context.getCounter(Counters.TOO_MANY_RELATIONS).increment(1);
		return;
	    }

	    String[] split;
	    if (valueString.contains(VALUE_DELIMITER)) {
		split = valueString.split(VALUE_DELIMITER);
	    } else {
		singleSplit[0] = valueString;
		split = singleSplit;
	    }
	    for (String s : split) {
		if (TuplesToResourcesMapper.PREDICATE_VALUE.equals(s)) {
		    keyPredicate = true;
		} else if (TuplesToResourcesMapper.OBJECT_VALUE.equals(s)) {
		    keyObject = true;
		} else if (TuplesToResourcesMapper.CONTEXT_VALUE.equals(s)) {
		    keyContext = true;
		} else {
		    if (s.length() > 100000) {
			System.out.println("Long tuple. Length:" + s.length() + " starting with " + s.substring(0, 100));
			context.getCounter(Counters.LONG_TUPLES).increment(1);
		    } else {
			try {
			    prefixDelimiterAppender(s);
			} catch (OutOfMemoryError e) {
			    System.out.println("OOM l:" + relations.length() + " valueCount:" + valueCount + " when appending " + s.length() + " chars.");
			    throw e;
			}
		    }
		}
	    }
	}

	if (keyPredicate) {
	    prefixDelimiterAppender(TuplesToResourcesMapper.PREDICATE_VALUE);
	    keyPredicate = false;
	}
	if (keyObject) {
	    prefixDelimiterAppender(TuplesToResourcesMapper.OBJECT_VALUE);
	    keyObject = false;
	}
	if (keyContext) {
	    prefixDelimiterAppender(TuplesToResourcesMapper.CONTEXT_VALUE);
	    keyContext = false;
	}

	context.write(key, new Text(relations.toString()));
    };

    private void prefixDelimiterAppender(String s) {
	if (relations.length() > 0) {
	    relations.append(ResourcesReducer.VALUE_DELIMITER);
	}
	relations.append(s);
    }
}
