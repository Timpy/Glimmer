package com.yahoo.glimmer.indexing;

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

import it.unimi.di.big.mg4j.document.DocumentCollectionBuilder;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.junit.Before;
import org.junit.Test;

public class BySubjectCollectionBuilderTest {
    private static final String SUBJECT_RECORD_1 = "3\t-1\t" +
    		"http://subject1/\t" + 
    		"<http://predicate11> \"literal1\" .\t" + 
    		"<http://predicate12> <http://resource1> .\t";
    
    private static final String SUBJECT_RECORD_2 = "6\t3\t" +
	    "http://subject2/\t" + 
	    "<http://predicate21> <http://resource2> .\t";
    
    private Mockery context;
    private DocumentCollectionBuilder builder;
    private BySubjectCollectionBuilder.BuilderOutputWriter writer;
    
    @Before
    public void before() throws IllegalArgumentException, IOException {
	context = new Mockery();
	builder = context.mock(DocumentCollectionBuilder.class);
	writer = new BySubjectCollectionBuilder.BuilderOutputWriter(builder);
    }
    
    @Test
    public void writerTest() throws IOException, InterruptedException {
	final Sequence writeSequence = context.sequence("writeSequence");
	context.checking(new Expectations() {{
	    one(builder).startDocument("", "");
	    inSequence(writeSequence);
	    one(builder).endDocument();
	    inSequence(writeSequence);
	    one(builder).startDocument("", "");
	    inSequence(writeSequence);
	    one(builder).endDocument();
	    inSequence(writeSequence);
	    one(builder).startDocument("", "");
	    inSequence(writeSequence);
	    one(builder).endDocument();
	    inSequence(writeSequence);
	    
	    // doc 1
	    one(builder).startDocument("http://subject1/", "http://subject1/");
	    inSequence(writeSequence);
	    one(builder).startTextField();
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("3"), new MutableString("\t"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("-1"), new MutableString("\t"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("http://subject1/"), new MutableString("\t"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString(""), new MutableString("<"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("http"), new MutableString("://"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("predicate11"), new MutableString("> \""));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("literal1"), new MutableString("\" .\t<"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("http"), new MutableString("://"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("predicate12"), new MutableString("> <"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("http"), new MutableString("://"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("resource1"), new MutableString("> .\t"));
	    inSequence(writeSequence);
	    one(builder).endTextField();
	    inSequence(writeSequence);
	    // End doc 1.
	    one(builder).endDocument();
	    inSequence(writeSequence);
	    
	    one(builder).startDocument("", "");
	    inSequence(writeSequence);
	    one(builder).endDocument();
	    inSequence(writeSequence);
	    one(builder).startDocument("", "");
	    inSequence(writeSequence);
	    one(builder).endDocument();
	    inSequence(writeSequence);
	    
	    // doc 2
	    one(builder).startDocument("http://subject2/", "http://subject2/");
	    inSequence(writeSequence);
	    one(builder).startTextField();
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("6"), new MutableString("\t"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("3"), new MutableString("\t"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("http://subject2/"), new MutableString("\t"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString(""), new MutableString("<"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("http"), new MutableString("://"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("predicate21"), new MutableString("> <"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("http"), new MutableString("://"));
	    inSequence(writeSequence);
	    one(builder).add(new MutableString("resource2"), new MutableString("> .\t"));
	    inSequence(writeSequence);
	    one(builder).endTextField();
	    inSequence(writeSequence);
	    // End doc 2.
	    one(builder).endDocument();
	    inSequence(writeSequence);
	}});
	
	LongWritable key = new LongWritable(0);
	Text value = new Text(SUBJECT_RECORD_1);
	writer.write(key, value);
	key.set(1);
	value.set(SUBJECT_RECORD_2);
	writer.write(key, value);
	
	context.assertIsSatisfied();
    }
}
