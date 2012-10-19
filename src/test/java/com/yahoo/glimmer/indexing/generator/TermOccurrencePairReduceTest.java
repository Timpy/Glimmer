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
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

public class TermOccurrencePairReduceTest {
    private Mockery context;
    private Reducer<TermOccurrencePair, Occurrence, IntWritable, IndexRecordWriterValue>.Context reducerContext;

    @SuppressWarnings("unchecked")
    @Before
    public void before() throws IOException, URISyntaxException {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	reducerContext = context.mock(Context.class, "reducerContext");
    }

    @Test
    public void treeTermsTest() throws Exception {
	context.checking(new Expectations() {{
	    allowing(reducerContext).setStatus(with(any(String.class)));
	    one(reducerContext).write(
		    with(new IntWritable(0)),
		    with(new IndexRecordWriterTermValueMatcher("term1", 3, 6, 15 + 12 + 18)));
	    one(reducerContext).write(
		    with(new IntWritable(0)),
		    with(new IndexRecordWriterDocValueMatcher(3, 11, 15)));
	    one(reducerContext).write(
		    with(new IntWritable(0)),
		    with(new IndexRecordWriterDocValueMatcher(4, 12)));
	    one(reducerContext).write(
		    with(new IntWritable(0)),
		    with(new IndexRecordWriterDocValueMatcher(7, 14, 17, 18)));
	    // Alignement. without counts or positions..
	    one(reducerContext).write(
		    with(new IntWritable(-1)),
		    with(new IndexRecordWriterTermValueMatcher("term1", 1, 0, 0)));
	    one(reducerContext).write(
		    with(new IntWritable(-1)),
		    with(new IndexRecordWriterDocValueMatcher(0)));
	    
	    one(reducerContext).write(
		    with(new IntWritable(0)),
		    with(new IndexRecordWriterTermValueMatcher("term2", 2, 4, 35)));
	    one(reducerContext).write(
		    with(new IntWritable(0)),
		    with(new IndexRecordWriterDocValueMatcher(1, 10, 19)));
	    one(reducerContext).write(
		    with(new IntWritable(0)),
		    with(new IndexRecordWriterDocValueMatcher(7, 13, 16)));
	    
	    one(reducerContext).write(
		    with(new IntWritable(1)),
		    with(new IndexRecordWriterTermValueMatcher("term3", 1, 2, 11)));
	    one(reducerContext).write(
		    with(new IntWritable(1)),
		    with(new IndexRecordWriterDocValueMatcher(2, 10, 11)));
	}});
	
	TermOccurrencePairReduce reducer = new TermOccurrencePairReduce();
	reducer.setup(reducerContext);
	
	TermOccurrencePair key = new TermOccurrencePair("term1", 0, null);
	ArrayList<Occurrence> values = new ArrayList<Occurrence>();
	values.add(new Occurrence(null, 3));
	values.add(new Occurrence(null, 4));
	values.add(new Occurrence(null, 7));
	values.add(new Occurrence(3, 11));
	values.add(new Occurrence(3, 15));
	values.add(new Occurrence(4, 12));
	values.add(new Occurrence(7, 14));
	values.add(new Occurrence(7, 17));
	values.add(new Occurrence(7, 18));
	reducer.reduce(key, values, reducerContext);
	// Alignment
	key = new TermOccurrencePair("term1", -1, null);
	values.clear();
	values.add(new Occurrence(null, 0));
	values.add(new Occurrence(null, 0));
	values.add(new Occurrence(null, 0));
	values.add(new Occurrence(0, null));
	values.add(new Occurrence(0, null));
	values.add(new Occurrence(0, null));
	reducer.reduce(key, values, reducerContext);
	
	key = new TermOccurrencePair("term2", 0, null);
	values.clear();
	values.add(new Occurrence(null, 1));
	values.add(new Occurrence(null, 7));
	values.add(new Occurrence(1, null));
	values.add(new Occurrence(1, 10));
	values.add(new Occurrence(1, 19));
	values.add(new Occurrence(7, null));
	values.add(new Occurrence(7, 13));
	values.add(new Occurrence(7, 16));
	reducer.reduce(key, values, reducerContext);
	
	
	
	key = new TermOccurrencePair("term3", 1, null);
	values.clear();
	values.add(new Occurrence(null, 2));
	values.add(new Occurrence(2, null));
	values.add(new Occurrence(2, 10));
	values.add(new Occurrence(2, 11));
	reducer.reduce(key, values, reducerContext);
	
	context.assertIsSatisfied();
    }
    
    private static class IndexRecordWriterTermValueMatcher extends BaseMatcher<IndexRecordWriterTermValue> {
	private final IndexRecordWriterTermValue termValue;

	public IndexRecordWriterTermValueMatcher(String term, int termFrequency, int occurrenceCount, long sumOfMaxTermPositions) {
	    termValue = new IndexRecordWriterTermValue();
	    termValue.setTerm(term);
	    termValue.setTermFrequency(termFrequency);
	    termValue.setOccurrenceCount(occurrenceCount);
	    termValue.setSumOfMaxTermPositions(sumOfMaxTermPositions);
	}
	
	
	@Override
	public boolean matches(Object object) {
	    return termValue.equals(object);
	}
	
	@Override
	public void describeTo(Description description) {
	    description.appendText(termValue.toString());
	}
    }
    private static class IndexRecordWriterDocValueMatcher extends BaseMatcher<IndexRecordWriterDocValue> {
	private final IndexRecordWriterDocValue docValue;
	
	public IndexRecordWriterDocValueMatcher(int document, int ... occurrences) {
	    docValue = new IndexRecordWriterDocValue(16);
	    docValue.setDocument(document);
	    for (int i = 0 ; i < occurrences.length ; i++) {
		docValue.addOccurrence(occurrences[i]);
	    }
	}
	
	@Override
	public boolean matches(Object object) {
	    return docValue.equals(object);
	}
	
	@Override
	public void describeTo(Description description) {
	    description.appendText(docValue.toString());
	}
    }
}
