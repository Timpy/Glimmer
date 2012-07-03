package com.yahoo.glimmer.indexing.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.mg4j.index.FileIndex;
import it.unimi.dsi.mg4j.index.IndexIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TermOccurrencePairReduceTest {
    private static final String INDEX_TMP_DIR = "/tmp/TermOccurrencePairReduceTest";
    private Mockery context;
    private Reducer<TermOccurrencePair, Occurrence, Text, Text>.Context reducerContext;
    private Map<Integer, Index> indices;
    private FileSystem fs = new RawLocalFileSystem();
    

    @SuppressWarnings("unchecked")
    @Before
    public void before() throws IOException, URISyntaxException {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);

	reducerContext = context.mock(Context.class, "reducerContext");

	fs.initialize(new URI("file:///"), new Configuration());
	indices = new HashMap<Integer, Index>();
	Index index = new Index(fs, INDEX_TMP_DIR, "index0", 7, true);
	index.open();
	indices.put(0, index);
	index = new Index(fs, INDEX_TMP_DIR, "index1", 7, true);
	index.open();
	indices.put(1, index);
    }
    
    @After
    public void after() throws IOException {
	// fs.deleteOnExit(new Path(INDEX_TMP_DIR)); doesn't work..
	if (!INDEX_TMP_DIR.startsWith("/tmp/")) {
	    throw new AssertionError("Not removing test indexes as they are not in /tmp as expected.");
	}
	fs.delete(new Path(INDEX_TMP_DIR), true);
    }

    @Test
    public void treeTermsTest() throws Exception {
	context.checking(new Expectations() {{
	    allowing(reducerContext).setStatus(with(any(String.class)));
	}});
	
	TermOccurrencePairReduce reducer = new TermOccurrencePairReduce();
	reducer.setIndices(indices);
	
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
	
	reducer.cleanup(reducerContext);
	
	context.assertIsSatisfied();
	
	// Check the written indexes..
	FileIndex index0 = (FileIndex) FileIndex.getInstance(INDEX_TMP_DIR + "/index0", true);
	assertEquals(7, index0.numberOfDocuments);
	assertEquals(2, index0.numberOfTerms);
	assertTrue(index0.hasPositions);
	// term1
	checkOccurrences(index0.documents(0), 3, "(3:11,15) (4:12) (7:14,17,18)");
	// term2
	checkOccurrences(index0.documents(1), 2, "(1:10,19) (7:13,16)");
	
	FileIndex index1 = (FileIndex) FileIndex.getInstance(INDEX_TMP_DIR + "/index1", true);
	assertEquals(7, index1.numberOfDocuments);
	assertEquals(1, index1.numberOfTerms);
	assertTrue(index0.hasPositions);
	// term3
	checkOccurrences(index1.documents(0), 1, "(2:10,11)");
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
	    for (int i = 0 ; i < noPositions ; i++) {
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
