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
import com.yahoo.glimmer.indexing.preprocessor.ResourceRecordWriter.OutputCount;
import com.yahoo.glimmer.indexing.preprocessor.TuplesToResourcesMapper.TUPLE_ELEMENTS;
import com.yahoo.glimmer.util.BySubjectRecord;

/**
 * Reducer
 * 
 * For the given Subject resource key concatanates all <predicate> <object>
 * <context> . for that key It also appends PREDICATE, OBJECT and/or CONTEXT
 * keywords if that keyword occurs once or more as a value.
 * 
 */
public class ResourcesReducer extends Reducer<Text, Text, Text, Object> {
    private OutputCount outputCount = new OutputCount();
    private BySubjectRecord bySubjectRecord = new BySubjectRecord();
    // Given that there is only 1 reducer writing a sorted list of subjects we
    // can add the deduce the document ID and add it to bysubjects
    // The alternative would be to generate a MPH over the list of subjects but
    // that would require more memory when building the indices.
    private int docId = 0;

    static enum Counters {
	TOO_MANY_RELATIONS;
    }

    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Object>.Context context) throws IOException, InterruptedException {
	int keyPredicateCount = 0;
	int keyObjectCount = 0;
	int keyContextCount = 0;
	int relationsCount = 0;
	
	outputCount.output = OUTPUT.ALL;
	outputCount.count = 0;
	context.write(key, outputCount);
	
	// The docId's should match with OUTPUT.ALL hash values
	bySubjectRecord.setId(docId++);
	bySubjectRecord.clearRelations();

	for (Text value : values) {
	    String valueString = value.toString();

	    if (TUPLE_ELEMENTS.PREDICATE.name().equals(valueString)) {
		keyPredicateCount++;
	    } else if (TUPLE_ELEMENTS.OBJECT.name().equals(valueString)) {
		keyObjectCount++;
	    } else if (TUPLE_ELEMENTS.CONTEXT.name().equals(valueString)) {
		keyContextCount++;
	    } else {
		bySubjectRecord.addRelation(valueString);
		relationsCount++;
	    }
	}
	
	
	if (relationsCount > 0) {
	    bySubjectRecord.setSubject(key.toString());
	    
	    if (bySubjectRecord.getRelationsCount() != relationsCount) {
		System.out.println("Too many relations. Only indexing " + bySubjectRecord.getRelationsCount() + " of " + relationsCount + ". Subject is:"
			+ key.toString());
		context.getCounter(Counters.TOO_MANY_RELATIONS).increment(1);
	    }
	}


	if (bySubjectRecord.hasRelations()) {
	    context.write(key, bySubjectRecord);
	}

	if (keyPredicateCount > 0) {
	    outputCount.output = OUTPUT.PREDICATE;
	    outputCount.count = keyPredicateCount;
	    context.write(key, outputCount);
	}
	if (keyObjectCount > 0) {
	    outputCount.output = OUTPUT.OBJECT;
	    outputCount.count = keyObjectCount;
	    context.write(key, outputCount);
	}
	if (keyContextCount > 0) {
	    outputCount.output = OUTPUT.CONTEXT;
	    outputCount.count = keyContextCount;
	    context.write(key, outputCount);
	}
    };
}
