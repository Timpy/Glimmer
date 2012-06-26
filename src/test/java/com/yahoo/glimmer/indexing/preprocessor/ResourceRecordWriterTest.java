package com.yahoo.glimmer.indexing.preprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

import com.yahoo.glimmer.indexing.preprocessor.ResourceRecordWriter;

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
		one(fs).create(with(new Path("/somepath/bysubject")), with(false));
		will(returnValue(bySubjectOs));
		one(fs).create(with(new Path("/somepath/subject")), with(false));
		will(returnValue(subjectOs));
		one(fs).create(with(new Path("/somepath/predicate")), with(false));
		will(returnValue(predicateOs));
		one(fs).create(with(new Path("/somepath/object")), with(false));
		will(returnValue(objectOs));
		one(fs).create(with(new Path("/somepath/context")), with(false));
		will(returnValue(contextOs));
	    }
	};
    }

    @Test
    public void writeSubjectAndObjectTest() throws IOException, InterruptedException {
	e.one(allOs).write(e.with(new ByteMatcher("http://a/key", true)), e.with(0), e.with(12));
	e.one(allOs).write('\n');
	e.one(subjectOs).write(e.with(new ByteMatcher("http://a/key", true)), e.with(0), e.with(12));
	e.one(subjectOs).write('\n');
	e.one(objectOs).write(e.with(new ByteMatcher("http://a/key", true)), e.with(0), e.with(12));
	e.one(objectOs).write('\n');
	e.one(bySubjectOs).write(e.with(new ByteMatcher("http://a/key", true)), e.with(0), e.with(12));
	e.one(bySubjectOs).write('\t');
	e.one(bySubjectOs).write(e.with(new ByteMatcher("<http://a/key> <http://predicate/> <http://Object> .", true)), e.with(0), e.with(52));
	e.one(bySubjectOs).write('\n');
	
	context.checking(e);
	
	ResourceRecordWriter writer = new ResourceRecordWriter(fs, new Path("/somepath"), null);
	
	writer.write(new Text("http://a/key"), new Text("<http://a/key> <http://predicate/> <http://Object> .  OBJECT"));
    }
    
    @Test
    public void writeContextTest() throws IOException, InterruptedException {
	e.one(allOs).write(e.with(new ByteMatcher("http://a/key", true)), e.with(0), e.with(12));
	e.one(allOs).write('\n');
	e.one(contextOs).write(e.with(new ByteMatcher("http://a/key", true)), e.with(0), e.with(12));
	e.one(contextOs).write('\n');

	context.checking(e);

	ResourceRecordWriter writer = new ResourceRecordWriter(fs, new Path("/somepath"), null);

	writer.write(new Text("http://a/key"), new Text("CONTEXT"));
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
	    return Arrays.equals(bytes, Arrays.copyOf(other, bytes.length));
	}

	@Override
	public void describeTo(Description description) {
	    if (ignoreTrailingBytes) {
		description.appendText(Arrays.toString(bytes) + "...");
	    } else {
		description.appendText(Arrays.toString(bytes));
	    }
	}
    }

    @Test
    public void byteArrayRegionMatchesTest() {
	byte[] big = "Hello World!".getBytes();
	assertTrue(ResourceRecordWriter.byteArrayRegionMatches(big, 6, "World".getBytes(), 5));
	assertFalse(ResourceRecordWriter.byteArrayRegionMatches(big, 5, "World".getBytes(), 5));
	assertFalse(ResourceRecordWriter.byteArrayRegionMatches(big, 7, "World".getBytes(), 5));
	assertFalse(ResourceRecordWriter.byteArrayRegionMatches(big, 10, "World".getBytes(), 5));

	assertTrue(ResourceRecordWriter.byteArrayRegionMatches(big, 0, "Hell".getBytes(), 4));
	assertFalse(ResourceRecordWriter.byteArrayRegionMatches(big, -1, "Hell".getBytes(), 4));
	assertFalse(ResourceRecordWriter.byteArrayRegionMatches(big, 0, "ell".getBytes(), 4));
	assertTrue(ResourceRecordWriter.byteArrayRegionMatches(big, 1, "ell".getBytes(), 3));

	assertTrue(ResourceRecordWriter.byteArrayRegionMatches(big, 10, "!".getBytes(), 1));
	
	assertTrue(ResourceRecordWriter.byteArrayRegionMatches(big, 0, "Hello World!".getBytes(), 11));
	assertTrue(ResourceRecordWriter.byteArrayRegionMatches(big, 0, "Hello World!".getBytes(), 12));
    }
}
