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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.glimmer.indexing.preprocessor.ResourceRecordWriter.OUTPUT;
import com.yahoo.glimmer.indexing.preprocessor.ResourceRecordWriter.OutputCount;
import com.yahoo.glimmer.util.BySubjectRecord;

public class ResourceRecordWriterTest {
    private Mockery context;
    private Expectations e;
    private FileSystem fs;
    private FSDataOutputStream allOs;
    private FSDataOutputStream bySubjectOs;
    private FSDataOutputStream subjectOs;
    private FSDataOutputStream predicateOs;
    private FSDataOutputStream objectOs;
    private FSDataOutputStream contextOs;

    @Before
    public void before() throws IOException {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	fs = context.mock(FileSystem.class);

	allOs = context.mock(FSDataOutputStream.class, "allOs");
	bySubjectOs = context.mock(FSDataOutputStream.class, "bySubjectOs");
	subjectOs = context.mock(FSDataOutputStream.class, "subjectOs");
	predicateOs = context.mock(FSDataOutputStream.class, "predicateOs");
	objectOs = context.mock(FSDataOutputStream.class, "objectOs");
	contextOs = context.mock(FSDataOutputStream.class, "contextOs");

	e = new Expectations() {
	    {
		one(fs).exists(with(new Path("/somepath")));
		will(returnValue(false));
		one(fs).mkdirs(with(new Path("/somepath")));
		one(fs).create(with(new Path("/somepath/all")), with(false));
		will(returnValue(allOs));
		one(fs).create(with(new Path("/somepath/bySubject")), with(false));
		will(returnValue(bySubjectOs));
		one(fs).create(with(new Path("/somepath/subjects")), with(false));
		will(returnValue(subjectOs));
		one(fs).create(with(new Path("/somepath/predicates")), with(false));
		will(returnValue(predicateOs));
		one(fs).create(with(new Path("/somepath/objects")), with(false));
		will(returnValue(objectOs));
		one(fs).create(with(new Path("/somepath/contexts")), with(false));
		will(returnValue(contextOs));
		one(allOs).close();
		one(bySubjectOs).close();
		one(subjectOs).close();
		one(predicateOs).close();
		one(objectOs).close();
		one(contextOs).close();
	    }
	};
    }

    @Test
    public void writeSubjectAndObjectTest() throws IOException, InterruptedException {
	e.one(allOs).write(e.with(new ByteMatcher("http://a/key\n", true)), e.with(0), e.with(13));
	e.one(contextOs).write(e.with(new ByteMatcher("http://a/key\n", true)), e.with(0), e.with(13));
	e.one(objectOs).write(e.with(new ByteMatcher("http://a/key\nbNode123\n", true)), e.with(0), e.with(22));
	e.one(predicateOs).write(e.with(new ByteMatcher("3\thttp://a/key\n", true)), e.with(0), e.with(15));
	e.one(subjectOs).write(e.with(new ByteMatcher("http://a/key\n", true)), e.with(0), e.with(13));
	e.one(bySubjectOs).write(e.with(new ByteMatcher("66\thttp://a/key\t<http://predicate/> <http://Object> .\n", true)), e.with(0), e.with(54));
	
	context.checking(e);
	
	ResourceRecordWriter writer = new ResourceRecordWriter(fs, new Path("/somepath"), null);
	
	OutputCount outputCount = new OutputCount();
	outputCount.output = OUTPUT.PREDICATE;
	outputCount.count = 3;
	writer.write(new Text("http://a/key"), outputCount);
	outputCount.output = OUTPUT.OBJECT;
	outputCount.count = 0;
	writer.write(new Text("http://a/key"), outputCount);
	outputCount.output = OUTPUT.CONTEXT;
	outputCount.count = 0;
	writer.write(new Text("http://a/key"), outputCount);
	outputCount.output = OUTPUT.ALL;
	outputCount.count = 0;
	writer.write(new Text("http://a/key"), outputCount);
	BySubjectRecord record = new BySubjectRecord();
	record.setId(66);
	record.setSubject("http://a/key");
	record.addRelation("<http://predicate/> <http://Object> .");
	writer.write(new Text("http://a/key"), record);
	outputCount.output = OUTPUT.OBJECT;
	outputCount.count = 0;
	writer.write(new Text("bNode123"), outputCount);
	writer.close(null);
	
	context.assertIsSatisfied();
    }
    
    private static class ByteMatcher extends BaseMatcher<byte[]> {
	private byte[] bytes;
	private boolean ignoreTrailingBytes;
	
	public ByteMatcher(String string, boolean ignoreTrailingBytes) {
	    bytes = string.getBytes();
	    this.ignoreTrailingBytes = ignoreTrailingBytes;
	}

	@Override
	public boolean matches(Object object) {
	    assert object instanceof byte[];
	    byte[] other = (byte[]) object;
	    if (ignoreTrailingBytes) {
		other = Arrays.copyOf(other, bytes.length);
	    }
	    return Arrays.equals(bytes, other);
	}

	@Override
	public void describeTo(Description description) {
	    if (ignoreTrailingBytes) {
		description.appendText(new String(bytes) + "...");
	    } else {
		description.appendText(new String(bytes));
	    }
	}
    }
}
