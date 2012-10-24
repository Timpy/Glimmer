package com.yahoo.glimmer.indexing.generator;

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
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.yahoo.glimmer.indexing.generator.TermValue.Type;

public class TermReduce extends Reducer<TermKey, TermValue, IntWritable, IndexRecordWriterValue> {
    private static final int MAX_INVERTEDLIST_SIZE = 50000000;
    private static final int MAX_POSITIONLIST_SIZE = 1000000;

    private IntWritable writerKey;
    private IndexRecordWriterTermValue writerTermValue;
    private IndexRecordWriterDocValue writerDocValue;

    private enum Counters {
	POSTINGLIST_SIZE_OVERFLOW, POSITIONLIST_SIZE_OVERFLOW, POSITIONLIST_SIZE_OVERFLOW_TIMES
    }

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer<TermKey, TermValue, IntWritable, IndexRecordWriterValue>.Context context) throws IOException,
	    InterruptedException {
	// The objects we pass to the writer are reused for every call to
	// context.write()
	writerKey = new IntWritable();
	writerTermValue = new IndexRecordWriterTermValue();
	writerDocValue = new IndexRecordWriterDocValue(MAX_POSITIONLIST_SIZE);
    };

    @Override
    public void reduce(TermKey key, Iterable<TermValue> values, Context context) throws IOException, InterruptedException {
	if (key == null || key.equals("")) {
	    return;
	}

	context.setStatus(key.getIndex() + ":" + key.getTerm());

	int termFrequency = 0;
	int termCount = 0;
	int sumOfMaxTermPositions = 0;
	TermValue value = null;
	TermValue prevValue = new TermValue();

	Iterator<TermValue> valuesIt = values.iterator();
	boolean valueIsOccurrence = false;
	while (!valueIsOccurrence && valuesIt.hasNext()) {
	    value = valuesIt.next();
	    // We shouldn't get duplicates.. should we?
	    if (value.equals(prevValue)) {
		throw new IllegalStateException("For index " + key.getIndex() + " term " + key.getTerm());
	    }
	    prevValue.set(value);

	    switch (value.getType()) {
	    case OCCURRENCE_COUNT:
		termFrequency++;
		termCount += value.getV2();
		break;
	    case LAST_OCCURRENCE:
		sumOfMaxTermPositions += value.getV2();
		break;
	    default:
		valueIsOccurrence = true;
		break;
	    }
	}

	writerKey.set(key.getIndex());

	writerTermValue.setTerm(key.getTerm());
	writerTermValue.setOccurrenceCount(termCount);
	writerTermValue.setTermFrequency(Math.min(termFrequency, MAX_INVERTEDLIST_SIZE));
	writerTermValue.setSumOfMaxTermPositions(sumOfMaxTermPositions);

	context.write(writerKey, writerTermValue);
	
	boolean tooManyOccurrences;
	int writtenDocs = 0;
	prevValue.set(value);
	while (value != null) {
	    if (value.getType() != Type.OCCURRENCE && value.getType() != Type.PREDICATE_ID) {
		throw new IllegalStateException("Got a " + value.getType() + " value when expecting only occurrences.");
	    }
	    tooManyOccurrences = false;
	    
	    int docID = value.getV1();
	    if (docID != prevValue.getV1()) {
		// New document, write out previous postings
		writerDocValue.setDocument(prevValue.getV1());
		context.write(writerKey, writerDocValue);
		writerDocValue.clearOccerrences();
		writtenDocs++;

		if (writtenDocs >= MAX_INVERTEDLIST_SIZE) {
		    context.getCounter(Counters.POSTINGLIST_SIZE_OVERFLOW).increment(1);
		    System.err.println("More than " + MAX_INVERTEDLIST_SIZE + " documents for term " + key.getTerm());
		    break;
		}
		if (value.getType() == Type.OCCURRENCE) {
		    writerDocValue.addOccurrence(value.getV2());
		}
	    } else if (value.getType() == Type.OCCURRENCE) {
		if (!tooManyOccurrences && !writerDocValue.addOccurrence(value.getV2())) {
		    tooManyOccurrences = true;
		    context.getCounter(Counters.POSITIONLIST_SIZE_OVERFLOW).increment(1);
		    System.err.println("More than " + MAX_POSITIONLIST_SIZE + " positions for term " + key.getTerm());
		}
	    }

	    prevValue.set(value);

	    boolean last = false;
	    if (valuesIt.hasNext()) {
		value = valuesIt.next();
		// Skip equivalent occurrences
		while (value.equals(prevValue) && valuesIt.hasNext()) {
		    value = valuesIt.next();
		}
		if (value.equals(prevValue) && !valuesIt.hasNext()) {
		    last = true;
		}
	    } else {
		last = true;
	    }
	    if (last) {
		// This is the last occurrence: write out the remaining
		// positions
		writerDocValue.setDocument(prevValue.getV1());
		context.write(writerKey, writerDocValue);

		writerDocValue.clearOccerrences();
		value = null;
	    }
	}
    }
}