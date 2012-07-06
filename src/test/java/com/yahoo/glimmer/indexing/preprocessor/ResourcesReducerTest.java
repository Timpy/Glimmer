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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.glimmer.indexing.preprocessor.ResourcesReducer;
import com.yahoo.glimmer.indexing.preprocessor.TuplesToResourcesMapper;


public class ResourcesReducerTest {
    private Mockery context;
    private Reducer<Text, Text, Text, Text>.Context mrContext;

    @SuppressWarnings("unchecked")
    @Before
    public void before() {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	mrContext = context.mock(Reducer.Context.class, "mrContext");
    }

    @Test
    public void subjectText() throws IOException, InterruptedException {
	context.checking(new Expectations() {
	    {
		one(mrContext).write(
			with(new TextMatcher("http://some/subject/uri")),
			with(new TextMatcher("<http://some/predicate/uri/1> <http://some/object/uri1> <http://some/context/uri1> .  "
				+ "<http://some/predicate/uri/2> <http://some/object/uri2> <http://some/context/uri2> .  "
				+ "<http://some/predicate/uri/3> \"Some literal value\" <http://some/context/uri3> .")));
	    }
	});
	ResourcesReducer reducer = new ResourcesReducer();

	Iterable<Text> values = new TextReuseIterable(
		"<http://some/predicate/uri/1> <http://some/object/uri1> <http://some/context/uri1> .",
		"<http://some/predicate/uri/2> <http://some/object/uri2> <http://some/context/uri2> .",
		"<http://some/predicate/uri/3> \"Some literal value\" <http://some/context/uri3> .");

	reducer.reduce(new Text("http://some/subject/uri"), values, mrContext);
    }

    @Test
    public void predicateText() throws IOException, InterruptedException {
	context.checking(new Expectations() {
	    {
		one(mrContext).write(with(new TextMatcher("http://some/resource/uri")), with(new TextMatcher(TuplesToResourcesMapper.PREDICATE_VALUE)));
	    }
	});
	ResourcesReducer reducer = new ResourcesReducer();

	Iterable<Text> values = new TextReuseIterable(TuplesToResourcesMapper.PREDICATE_VALUE);

	reducer.reduce(new Text("http://some/resource/uri"), values, mrContext);
    }

    @Test
    public void objectText() throws IOException, InterruptedException {
	context.checking(new Expectations() {
	    {
		one(mrContext).write(with(new TextMatcher("http://some/resource/uri")), with(new TextMatcher(TuplesToResourcesMapper.OBJECT_VALUE)));
	    }
	});
	ResourcesReducer reducer = new ResourcesReducer();

	Iterable<Text> values = new TextReuseIterable(
		TuplesToResourcesMapper.OBJECT_VALUE,
		TuplesToResourcesMapper.OBJECT_VALUE);

	reducer.reduce(new Text("http://some/resource/uri"), values, mrContext);
    }

    @Test
    public void contextText() throws IOException, InterruptedException {
	context.checking(new Expectations() {
	    {
		one(mrContext).write(with(new TextMatcher("http://some/resource/uri")), with(new TextMatcher(TuplesToResourcesMapper.CONTEXT_VALUE)));
	    }
	});
	ResourcesReducer reducer = new ResourcesReducer();

	Iterable<Text> values = new TextReuseIterable(
		TuplesToResourcesMapper.CONTEXT_VALUE,
		TuplesToResourcesMapper.CONTEXT_VALUE,
		TuplesToResourcesMapper.CONTEXT_VALUE,
		TuplesToResourcesMapper.CONTEXT_VALUE);

	reducer.reduce(new Text("http://some/resource/uri"), values, mrContext);
    }
    
    @Test
    public void subjectAndObjectText() throws IOException, InterruptedException {
	context.checking(new Expectations() {
	    {
		one(mrContext).write(
			with(new TextMatcher("http://some/resource/uri")),
			with(new TextMatcher("<http://some/predicate/uri/1> <http://some/object/uri1> <http://some/context/uri1> .  "
				+ "<http://some/predicate/uri/2> <http://some/object/uri2> <http://some/context/uri2> .  "
				+ "OBJECT")));
	    }
	});
	ResourcesReducer reducer = new ResourcesReducer();

	Iterable<Text> values = new TextReuseIterable(
		"<http://some/predicate/uri/1> <http://some/object/uri1> <http://some/context/uri1> .",
		TuplesToResourcesMapper.OBJECT_VALUE,
		"<http://some/predicate/uri/2> <http://some/object/uri2> <http://some/context/uri2> .",
		TuplesToResourcesMapper.OBJECT_VALUE
		);

	reducer.reduce(new Text("http://some/resource/uri"), values, mrContext);
    }
    
    private static class TextReuseIterable implements Iterable<Text> {
	private final List<String> strings;

	public TextReuseIterable(String ... strings) {
	    this.strings = Arrays.asList(strings);
	}
	
	@Override
	public Iterator<Text> iterator() {
	    return new Iterator<Text>() {
		private Iterator<String> iterator;
		private Text reusedTextObject;
		{
		    iterator = strings.iterator();
		    reusedTextObject = new Text();
		}
		@Override
		public boolean hasNext() {
		    return iterator.hasNext();
		}

		@Override
		public Text next() {
		    reusedTextObject.set(iterator.next());
		    return reusedTextObject;
		}

		@Override
		public void remove() {
		    throw new UnsupportedOperationException();
		}
	    };
	}
    }
}
