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
    private Reducer<TermOccurrencePair, Occurrence, TermOccurrencePair, TermOccurrences>.Context reducerContext;

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
		    with(new TermOccurrencePairMatcher(0, "term1")),
		    with(new TermOccurrencesMatcher(3)));
	    one(reducerContext).write(
		    with(new TermOccurrencePairMatcher(0, "term1")),
		    with(new TermOccurrencesMatcher(3, 11, 15)));
	    one(reducerContext).write(
		    with(new TermOccurrencePairMatcher(0, "term1")),
		    with(new TermOccurrencesMatcher(4, 12)));
	    one(reducerContext).write(
		    with(new TermOccurrencePairMatcher(0, "term1")),
		    with(new TermOccurrencesMatcher(7, 14, 17, 18)));
	    one(reducerContext).write(
		    with(new TermOccurrencePairMatcher(0, "term2")),
		    with(new TermOccurrencesMatcher(2)));
	    one(reducerContext).write(
		    with(new TermOccurrencePairMatcher(0, "term2")),
		    with(new TermOccurrencesMatcher(1, 10, 19)));
	    one(reducerContext).write(
		    with(new TermOccurrencePairMatcher(0, "term2")),
		    with(new TermOccurrencesMatcher(7, 13, 16)));
	    one(reducerContext).write(
		    with(new TermOccurrencePairMatcher(1, "term3")),
		    with(new TermOccurrencesMatcher(1)));
	    one(reducerContext).write(
		    with(new TermOccurrencePairMatcher(1, "term3")),
		    with(new TermOccurrencesMatcher(2, 10, 11)));
	}});
	
	TermOccurrencePairReduce reducer = new TermOccurrencePairReduce();
	reducer.setup(reducerContext);
	
	TermOccurrencePair key = new TermOccurrencePair("term1", 0, null);
	ArrayList<Occurrence> values = new ArrayList<Occurrence>();
	values.add(new Occurrence(null, 3));
	values.add(new Occurrence(null, 4));
	values.add(new Occurrence(null, 7));
	values.add(new Occurrence(3, null));
	values.add(new Occurrence(3, 11));
	values.add(new Occurrence(3, 15));
	values.add(new Occurrence(4, null));
	values.add(new Occurrence(4, 12));
	values.add(new Occurrence(7, null));
	values.add(new Occurrence(7, 14));
	values.add(new Occurrence(7, 17));
	values.add(new Occurrence(7, 18));
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
    
    private static class TermOccurrencesMatcher extends BaseMatcher<TermOccurrences> {
	private final TermOccurrences termOccurrences;
	
	public TermOccurrencesMatcher(int termFrequency) {
	    termOccurrences = new TermOccurrences(0);
	    termOccurrences.setTermFrequency(termFrequency);
	}
	public TermOccurrencesMatcher(int document, int ... occurrences) {
	    termOccurrences = new TermOccurrences(occurrences.length);
	    termOccurrences.setDocument(document);
	    for (int i = 0 ; i < occurrences.length ; i++) {
		termOccurrences.addOccurrence(occurrences[i]);
	    }
	}
	
	@Override
	public boolean matches(Object object) {
	    return termOccurrences.equals(object);
	}
	
	@Override
	public void describeTo(Description description) {
	    description.appendText(termOccurrences.toString());
	}
    }
}
