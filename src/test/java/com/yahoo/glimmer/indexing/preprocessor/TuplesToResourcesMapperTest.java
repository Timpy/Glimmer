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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.glimmer.indexing.preprocessor.TuplesToResourcesMapper;

public class TuplesToResourcesMapperTest {
    private Mockery context;
    private Mapper<LongWritable,Text,Text,Text>.Context mrContext;
    private Counter nxParserExceptionCounter;
    
    @SuppressWarnings("unchecked")
    @Before
    public void before() {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	mrContext = context.mock(Mapper.Context.class, "mrContext");
	nxParserExceptionCounter = context.mock(Counter.class, "nxParserExceptionCounter");
    }
    
    @Test
    public void literalObjectText() throws IOException, InterruptedException {
	context.checking(new Expectations(){{
	    one(mrContext).write(with(new TextMatcher("http://www.example.org/terms/name")), with(new TextMatcher(TuplesToResourcesMapper.PREDICATE_VALUE)));
	    one(mrContext).write(with(new TextMatcher("http://www.example.org/staffid/85740")), with(new TextMatcher("<http://www.example.org/staffid/85740> <http://www.example.org/terms/name> \"Smith\" .")));
	}});
	TuplesToResourcesMapper mapper = new TuplesToResourcesMapper();
	
	mapper.map(new LongWritable(5l), new Text(
		"<http://www.example.org/staffid/85740> <http://www.example.org/terms/name> \"Smith\" ."), mrContext);
    }
    
    @Test
    public void resourceObjectTest() throws IOException, InterruptedException {
	context.checking(new Expectations(){{
	    one(mrContext).write(with(new TextMatcher("http://purl.org/dc/elements/1.1/creator")), with(new TextMatcher(TuplesToResourcesMapper.PREDICATE_VALUE)));
	    one(mrContext).write(with(new TextMatcher("http://www.example.org/staffid/85740")), with(new TextMatcher(TuplesToResourcesMapper.OBJECT_VALUE)));
	    one(mrContext).write(with(new TextMatcher("http://context/")), with(new TextMatcher(TuplesToResourcesMapper.CONTEXT_VALUE)));
	    one(mrContext).write(with(new TextMatcher("http://www.example.org/index.html")), with(new TextMatcher("<http://www.example.org/index.html> <http://purl.org/dc/elements/1.1/creator> <http://www.example.org/staffid/85740> <http://context/> .")));
	}});
	TuplesToResourcesMapper mapper = new TuplesToResourcesMapper();
	
	mapper.map(new LongWritable(5l), new Text(
		"<http://www.example.org/index.html> <http://purl.org/dc/elements/1.1/creator> <http://www.example.org/staffid/85740> <http://context/> ."), mrContext);
    }
    
    @Test
    public void noContextsObjectTest() throws IOException, InterruptedException {
	context.checking(new Expectations(){{
	    one(mrContext).write(with(new TextMatcher("http://purl.org/dc/elements/1.1/creator")), with(new TextMatcher(TuplesToResourcesMapper.PREDICATE_VALUE)));
	    one(mrContext).write(with(new TextMatcher("http://www.example.org/staffid/85740")), with(new TextMatcher(TuplesToResourcesMapper.OBJECT_VALUE)));
	    one(mrContext).write(with(new TextMatcher("http://www.example.org/index.html")), with(new TextMatcher("<http://www.example.org/index.html> <http://purl.org/dc/elements/1.1/creator> <http://www.example.org/staffid/85740> .")));
	}});
	TuplesToResourcesMapper mapper = new TuplesToResourcesMapper();
	mapper.setIncludeContexts(false);
	
	mapper.map(new LongWritable(5l), new Text(
		"<http://www.example.org/index.html> <http://purl.org/dc/elements/1.1/creator> <http://www.example.org/staffid/85740> <http://context/> ."), mrContext);
    }
    
    /*
     * NxParser 1.2.2 fails with typed literals. The map method should remove the type and try again. 
     */
    @Test
    public void qualifiedIntTest() throws IOException, InterruptedException {
	context.checking(new Expectations(){{
	    one(mrContext).getCounter(TuplesToResourcesMapper.MapCounters.NX_PARSER_EXCEPTION);
	    will(returnValue(nxParserExceptionCounter));
	    one(nxParserExceptionCounter).increment(1l);
	    one(mrContext).write(with(new TextMatcher("http://www.example.org/terms/age")), with(new TextMatcher(TuplesToResourcesMapper.PREDICATE_VALUE)));
	    one(mrContext).write(with(new TextMatcher("http://www.example.org/staffid/85740")), with(new TextMatcher("<http://www.example.org/staffid/85740> <http://www.example.org/terms/age> \"27\" .")));
	}});
	TuplesToResourcesMapper mapper = new TuplesToResourcesMapper();
	mapper.map(new LongWritable(5l), new Text(
		"<http://www.example.org/staffid/85740>  <http://www.example.org/terms/age> \"27\"^^<http://www.w3.org/2001/XMLSchema#integer> ."), mrContext);
    }
}
