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

import com.yahoo.glimmer.indexing.preprocessor.ResourceRecordWriter.OUTPUT;
import com.yahoo.glimmer.indexing.preprocessor.TuplesToResourcesMapper.TUPLE_ELEMENTS;

/**
 * Reducer
 * 
 * For the given Subject resource key concatanates all <predicate> <object>
 * <context> . for that key It also appends PREDICATE, OBJECT and/or CONTEXT
 * keywords if that keyword occurs once or more as a value.
 * 
 */
public class ResourcesReducer extends Reducer<Text, Text, Text, Text> {
    private static final int MAX_RELATIONS = 10000;
    public static final String VALUE_DELIMITER = "  ";
    private boolean keyPredicate;
    private boolean keyObject;
    private boolean keyContext;
    private StringBuilder relations = new StringBuilder();
    
    static enum Counters {
	TOO_MANY_RELATIONS;
    }

    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	relations.setLength(0);
	int relationsCount = 0;
	for (Text value : values) {
	    String valueString = value.toString();

	    if (TUPLE_ELEMENTS.PREDICATE.name().equals(valueString)) {
		keyPredicate = true;
	    } else if (TUPLE_ELEMENTS.OBJECT.name().equals(valueString)) {
		keyObject = true;
	    } else if (TUPLE_ELEMENTS.CONTEXT.name().equals(valueString)) {
		keyContext = true;
	    } else {
		relationsCount++;
		if (relationsCount <= MAX_RELATIONS) {
		    try {
			prefixDelimiterAppender(valueString);
		    } catch (OutOfMemoryError e) { // TODO 
			System.out.println("OOM l:" + relations.length() + " relationsCount:" + relationsCount + " when appending " + valueString.length()
				+ " chars.");
			throw e;
		    }
		}
	    }
	}
	
	if (relationsCount > MAX_RELATIONS) {
	    System.out.println("Too many relations. Only indexing " + relationsCount + " of " + MAX_RELATIONS + ". Subject is:" + key.toString());
	    context.getCounter(Counters.TOO_MANY_RELATIONS).increment(1);
	}

	context.write(key,  new Text(OUTPUT.ALL.name()));
	
	if (keyPredicate) {
	    context.write(key, new Text(OUTPUT.PREDICATE.name()));
	    keyPredicate = false;
	}
	if (keyObject) {
	    context.write(key, new Text(OUTPUT.OBJECT.name()));
	    keyObject = false;
	}
	if (keyContext) {
	    context.write(key, new Text(OUTPUT.CONTEXT.name()));
	    keyContext = false;
	}

	if (relationsCount > 0) {
	    context.write(key, new Text(relations.toString()));
	}
    };

    private void prefixDelimiterAppender(String s) {
	if (relations.length() > 0) {
	    relations.append(ResourcesReducer.VALUE_DELIMITER);
	}
	relations.append(s);
    }
}
