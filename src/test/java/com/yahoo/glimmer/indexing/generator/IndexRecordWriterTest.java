package com.yahoo.glimmer.indexing.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.mg4j.index.FileIndex;
import it.unimi.dsi.mg4j.index.IndexIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.glimmer.indexing.generator.IndexRecordWriter.OutputFormat;

public class IndexRecordWriterTest {
    private static final Path INDEX_TMP_DIR = new Path("/tmp/IndexRecordWriterTest");
    private Mockery context;
    private TaskInputOutputContext<?, ?, ?, ?> taskContext;
    private Configuration conf;
    private FileSystem fs = new RawLocalFileSystem();
    private TaskAttemptID taskAttemptID = new TaskAttemptID("taskId", 8, false, 88, 888);

    @Before
    public void before() throws IOException, URISyntaxException {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	taskContext = context.mock(TaskInputOutputContext.class, "taskContext");
	conf = new Configuration();
	
	conf.set("mapred.output.dir", INDEX_TMP_DIR.toString());
	conf.setInt(TripleIndexGenerator.NUMBER_OF_DOCUMENTS, 7);

	fs.initialize(new URI("file:///"), new Configuration());
    }

    @After
    public void after() throws IOException {
	// fs.deleteOnExit(new Path(INDEX_TMP_DIR)); doesn't work..
	if (!INDEX_TMP_DIR.toString().startsWith("/tmp/")) {
	    throw new AssertionError("Not removing test indexes as they are not in /tmp as expected.");
	}
	fs.delete(INDEX_TMP_DIR, true);
    }

    @Test
    public void test() throws Exception {
	context.checking(new Expectations(){{
	    allowing(taskContext).getConfiguration();
	    will(returnValue(conf));
	    allowing(taskContext).getTaskAttemptID();
	    will(returnValue(taskAttemptID));
	}});
	OutputFormat outputFormat = new IndexRecordWriter.OutputFormat();
	
	conf.setStrings("RdfFieldNames", "index0", "index1");

	RecordWriter<TermOccurrencePair, TermOccurrences> recordWriter = outputFormat.getRecordWriter(taskContext);

	TermOccurrencePair key = new TermOccurrencePair("term1", 0, null);
	TermOccurrences value = new TermOccurrences(16);
	value.setTermFrequency(3);
	recordWriter.write(key, value);
	value.setDocument(3);
	value.addOccurrence(11);
	value.addOccurrence(15);
	recordWriter.write(key, value);
	value.setDocument(4);
	value.clearOccerrences();
	value.addOccurrence(12);
	recordWriter.write(key, value);
	value.setDocument(7);
	value.clearOccerrences();
	value.addOccurrence(14);
	value.addOccurrence(17);
	value.addOccurrence(18);
	recordWriter.write(key, value);
	
	key = new TermOccurrencePair("term2", 0, null);
	value.setTermFrequency(2);
	recordWriter.write(key, value);
	value.setDocument(1);
	value.clearOccerrences();
	value.addOccurrence(10);
	value.addOccurrence(19);
	recordWriter.write(key, value);
	value.setDocument(7);
	value.clearOccerrences();
	value.addOccurrence(13);
	value.addOccurrence(16);
	recordWriter.write(key, value);
	
	key = new TermOccurrencePair("term2", 1, null);
	value.setTermFrequency(1);
	recordWriter.write(key, value);
	value.setDocument(1);
	value.clearOccerrences();
	value.addOccurrence(14);
	recordWriter.write(key, value);
	
	key = new TermOccurrencePair("term3", 1, null);
	value.setTermFrequency(1);
	recordWriter.write(key, value);
	value.setDocument(3);
	value.clearOccerrences();
	value.addOccurrence(10);
	value.addOccurrence(11);
	recordWriter.write(key, value);
	
	recordWriter.close(taskContext);

	// Check the written indexes..
	
	Path workPath = outputFormat.getDefaultWorkFile(taskContext,"");
	System.out.println("Default work file is " + workPath.toString());
	
	FileIndex index0 = (FileIndex) FileIndex.getInstance(workPath.toString() + "/index0", true);
	assertEquals(7, index0.numberOfDocuments);
	assertEquals(2, index0.numberOfTerms);
	assertTrue(index0.hasPositions);
	// term1
	checkOccurrences(index0.documents(0), 3, "(3:11,15) (4:12) (7:14,17,18)");
	// term2
	checkOccurrences(index0.documents(1), 2, "(1:10,19) (7:13,16)");

	FileIndex index1 = (FileIndex) FileIndex.getInstance(workPath.toString() + "/index1", true);
	assertEquals(7, index1.numberOfDocuments);
	assertEquals(2, index1.numberOfTerms);
	assertTrue(index0.hasPositions);
	checkOccurrences(index1.documents(0), 1, "(1:14)");
	// term3
	checkOccurrences(index1.documents(1), 1, "(3:10,11)");
    }

    private void checkOccurrences(IndexIterator documents, int frequencey, String expected) throws IOException {
	assertEquals(frequencey, documents.frequency());
	StringBuilder actual = new StringBuilder();
	while (documents.hasNext()) {
	    if (actual.length() > 0) {
		actual.append(' ');
	    }
	    Integer next = documents.next();
	    actual.append('(');
	    actual.append(next);
	    actual.append(':');
	    int[] positions = new int[10];
	    int noPositions = documents.positions(positions);
	    for (int i = 0; i < noPositions; i++) {
		if (i != 0) {
		    actual.append(',');
		}
		actual.append(positions[i]);
	    }
	    actual.append(")");
	}
	assertEquals(expected, actual.toString());
    }

}
