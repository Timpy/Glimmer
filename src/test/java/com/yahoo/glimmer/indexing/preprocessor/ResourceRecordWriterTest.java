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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Random;

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import ucar.unidata.io.bzip2.CBZip2InputStream;

import com.yahoo.glimmer.indexing.preprocessor.ResourceRecordWriter.OUTPUT;
import com.yahoo.glimmer.indexing.preprocessor.ResourceRecordWriter.OutputCount;
import com.yahoo.glimmer.util.BySubjectRecord;
import com.yahoo.glimmer.util.Bz2BlockIndexedDocumentCollection;

public class ResourceRecordWriterTest {
    private Mockery context;
    private Expectations e;
    private FileSystem fs;
    private FSDataOutputStream allOs;
    private FSDataOutputStream subjectOs;
    private FSDataOutputStream predicateOs;
    private FSDataOutputStream objectOs;
    private FSDataOutputStream contextOs;
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private Path tempDirPath;

    @Before
    public void before() throws IOException {
	tempDirPath = new Path(tempFolder.getRoot().getCanonicalPath());
	
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	fs = context.mock(FileSystem.class);

	allOs = context.mock(FSDataOutputStream.class, "allOs");
	subjectOs = context.mock(FSDataOutputStream.class, "subjectOs");
	predicateOs = context.mock(FSDataOutputStream.class, "predicateOs");
	objectOs = context.mock(FSDataOutputStream.class, "objectOs");
	contextOs = context.mock(FSDataOutputStream.class, "contextOs");

	e = new Expectations() {
	    {
		one(fs).exists(with(tempDirPath));
		will(returnValue(false));
		one(fs).mkdirs(with(tempDirPath));
		one(fs).create(with(new Path(tempDirPath, "all")), with(false));
		will(returnValue(allOs));
		one(fs).create(with(new Path(tempDirPath, "subjects")), with(false));
		will(returnValue(subjectOs));
		one(fs).create(with(new Path(tempDirPath, "predicates")), with(false));
		will(returnValue(predicateOs));
		one(fs).create(with(new Path(tempDirPath, "objects")), with(false));
		will(returnValue(objectOs));
		one(fs).create(with(new Path(tempDirPath, "contexts")), with(false));
		will(returnValue(contextOs));
		one(allOs).close();
		one(subjectOs).close();
		one(predicateOs).close();
		one(objectOs).close();
		one(contextOs).close();
	    }
	};
    }

    @Test
    public void writeSubjectAndObjectTest() throws IOException, InterruptedException {
	ByteArrayOutputStream bySubjectBos = new ByteArrayOutputStream(1024);
	FSDataOutputStream bySubjectOs = new FSDataOutputStream(bySubjectBos, null);
	ByteArrayOutputStream bySubjectOffsetsBos = new ByteArrayOutputStream(1024);
	FSDataOutputStream bySubjectOffsetsOs = new FSDataOutputStream(bySubjectOffsetsBos, null);
	
	e.one(fs).create(e.with(new Path(tempDirPath, "bySubject.bz2")), e.with(false));
	e.will(Expectations.returnValue(bySubjectOs));
	e.one(fs).create(e.with(new Path(tempDirPath, "bySubject.blockOffsets")), e.with(false));
	e.will(Expectations.returnValue(bySubjectOffsetsOs));
	
	e.one(allOs).write(e.with(new ByteMatcher("http://a/key1\nhttp://a/key2\nhttp://a/key3\n", true)), e.with(0), e.with(42));
	e.one(contextOs).write(e.with(new ByteMatcher("http://a/key\n", true)), e.with(0), e.with(13));
	e.one(objectOs).write(e.with(new ByteMatcher("http://a/key\nbNode123\n", true)), e.with(0), e.with(22));
	e.one(predicateOs).write(e.with(new ByteMatcher("3\thttp://a/key\n", true)), e.with(0), e.with(15));
	e.one(subjectOs).write(e.with(new ByteMatcher("http://a/key\n", true)), e.with(0), e.with(13));

	context.checking(e);
	
	ResourceRecordWriter writer = new ResourceRecordWriter(fs, tempDirPath, null);
	
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
	writer.write(new Text("http://a/key1"), outputCount);
	writer.write(new Text("http://a/key2"), outputCount);
	writer.write(new Text("http://a/key3"), outputCount);
	BySubjectRecord record = new BySubjectRecord();
	record.setId(66);
	record.setPreviousId(55);
	record.setSubject("http://a/key");
	record.addRelation("<http://predicate/> <http://Object> .");
	writer.write(new Text("http://a/key"), record);
	outputCount.output = OUTPUT.OBJECT;
	outputCount.count = 0;
	writer.write(new Text("bNode123"), outputCount);
	writer.close(null);
	
	context.assertIsSatisfied();
	
	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bySubjectBos.toByteArray());
	assertEquals('B', byteArrayInputStream.read());
	assertEquals('Z', byteArrayInputStream.read());
	InputStream inputStream = new CBZip2InputStream(byteArrayInputStream);
	byte [] buffer = new byte[1024];
	int bytesRead = inputStream.read(buffer);
	assertEquals("66\t55\thttp://a/key\t<http://predicate/> <http://Object> .\t\n", new String(buffer, 0, bytesRead, "ASCII"));
	inputStream.close();
	
	inputStream = new ByteArrayInputStream(bySubjectOffsetsBos.toByteArray());
	DataInputStream dis = new DataInputStream(inputStream);
	assertEquals(66l, dis.readLong());
	assertEquals(4l, dis.readLong());
	
	// Size of collection. This is the same as the number of lines written to ALL.
	assertEquals(3l, dis.readLong());
	
	boolean eOFExceptionThrown = false;
	try {
	    dis.readByte();
	} catch (EOFException e) {
	    eOFExceptionThrown = true;
	}
	assertTrue(eOFExceptionThrown);
	
	inputStream.close();
	byteArrayInputStream.close();
	dis.close();
    }
    
    @Test
    public void bySubjectsTest() throws IOException, InterruptedException {
	FSDataOutputStream bySubjectOs = new FSDataOutputStream(new FileOutputStream(new File(tempDirPath.toUri().getPath(), "bySubject.bz2")), null);
	FSDataOutputStream bySubjectOffsetsOs = new FSDataOutputStream(new FileOutputStream(new File(tempDirPath.toUri().getPath(), "bySubject.blockOffsets")), null);
	
	e.one(fs).create(e.with(new Path(tempDirPath, "bySubject.bz2")), e.with(false));
	e.will(Expectations.returnValue(bySubjectOs));
	e.one(fs).create(e.with(new Path(tempDirPath, "bySubject.blockOffsets")), e.with(false));
	e.will(Expectations.returnValue(bySubjectOffsetsOs));

	e.allowing(subjectOs).write(e.with(new ByteMatcher()), e.with(0), e.with(Expectations.any(Integer.class)));
	context.checking(e);
	
	ResourceRecordWriter writer = new ResourceRecordWriter(fs, tempDirPath, null);
	
	BySubjectRecord record = new BySubjectRecord();
	Random random = new Random();
	for (long l = 100000 ; l < 200000 ; l += Math.abs(random.nextInt() % 20)) {
	    record.setId(l);
	    record.setSubject("Subject:" + Integer.toString(random.nextInt()));
	    for (int i = 0 ; i < random.nextInt() % 4 ; i++) {
		record.addRelation("a relation " + Long.toString(random.nextLong()));
	    }
	    
	    writer.write(null, record);
	    
	    record.setPreviousId(l);
	    record.clearRelations();
	}
	writer.close(null);
	
	long lastId = record.getId();
	
	Bz2BlockIndexedDocumentCollection collection = new Bz2BlockIndexedDocumentCollection("bySubject", null);
	String indexBaseName = new File(tempDirPath.toUri().getPath(), "bySubject").getCanonicalPath();
	collection.filename(indexBaseName);
	
	assertEquals(-1, collection.stream(99999).read());
	
	InputStream documentInputStream = collection.stream(100000);
	assertTrue(record.parse(new InputStreamReader(documentInputStream)));
	assertEquals(100000, record.getId());
	
	documentInputStream = collection.stream(lastId);
	assertTrue(record.parse(new InputStreamReader(documentInputStream)));
	assertEquals(lastId, record.getId());
	
	assertEquals(-1, collection.stream(lastId + 1).read());
	
	collection.close();
    }
    
    private static class ByteMatcher extends BaseMatcher<byte[]> {
	private byte[] bytes;
	private boolean ignoreTrailingBytes;
	
	public ByteMatcher() {
	}
	public ByteMatcher(String string, boolean ignoreTrailingBytes) {
	    bytes = string.getBytes();
	    this.ignoreTrailingBytes = ignoreTrailingBytes;
	}

	@Override
	public boolean matches(Object object) {
	    if (bytes == null) {
		return true;
	    }
	    assert object instanceof byte[];
	    byte[] other = (byte[]) object;
	    if (ignoreTrailingBytes) {
		other = Arrays.copyOf(other, bytes.length);
	    }
	    return Arrays.equals(bytes, other);
	}

	@Override
	public void describeTo(Description description) {
	    if (bytes == null) {
		description.appendText("any byte array");
	    } else if (ignoreTrailingBytes) {
		description.appendText(new String(bytes) + "...");
	    } else {
		description.appendText(new String(bytes));
	    }
	}
    }
}
