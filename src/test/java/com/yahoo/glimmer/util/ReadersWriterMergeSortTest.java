package com.yahoo.glimmer.util;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.StringBuilderWriter;
import org.junit.Test;

import com.yahoo.glimmer.util.ReadersWriterMergeSort;

public class ReadersWriterMergeSortTest {

    @Test
    public void noReadersTest() throws IOException {
	List<BufferedReader> sourceReaders = new ArrayList<BufferedReader>();
	StringBuilderWriter writer = new StringBuilderWriter();

	ReadersWriterMergeSort.mergeSort(sourceReaders, writer);

	assertEquals("", writer.toString());
    }

    @Test
    public void emptyReadersTest() throws IOException {
	List<BufferedReader> sourceReaders = new ArrayList<BufferedReader>();
	sourceReaders.add(new BufferedReader(new StringReader("")));
	sourceReaders.add(new BufferedReader(new StringReader("")));
	sourceReaders.add(new BufferedReader(new StringReader("")));
	StringBuilderWriter writer = new StringBuilderWriter();

	ReadersWriterMergeSort.mergeSort(sourceReaders, writer);

	assertEquals("", writer.toString());
    }

    @Test(expected = IllegalStateException.class)
    public void duplicateLinesTest() throws IOException {
	List<BufferedReader> sourceReaders = new ArrayList<BufferedReader>();
	sourceReaders.add(new BufferedReader(new StringReader("A")));
	sourceReaders.add(new BufferedReader(new StringReader("A")));
	sourceReaders.add(new BufferedReader(new StringReader("")));
	StringBuilderWriter writer = new StringBuilderWriter();

	ReadersWriterMergeSort.mergeSort(sourceReaders, writer);
    }

    @Test
    public void simple1Test() throws IOException {
	List<BufferedReader> sourceReaders = new ArrayList<BufferedReader>();
	sourceReaders.add(new BufferedReader(new StringReader("Hello\nWorld!")));
	sourceReaders.add(new BufferedReader(new StringReader("Baa\nBaz\nFoo")));
	sourceReaders.add(new BufferedReader(new StringReader("2\n6\n8\nZ\n")));
	StringBuilderWriter writer = new StringBuilderWriter();
	
	ReadersWriterMergeSort.mergeSort(sourceReaders, writer);
	assertEquals("2\n" +
			"6\n" +
			"8\n" +
			"Baa\n" +
			"Baz\n" +
			"Foo\n" +
			"Hello\n" +
			"World!\n" +
			"Z\n"
	, writer.toString());
    }

    @Test
    public void noNewLinesTest() throws IOException {
	List<BufferedReader> sourceReaders = new ArrayList<BufferedReader>();
	sourceReaders.add(new BufferedReader(new StringReader("B")));
	sourceReaders.add(new BufferedReader(new StringReader("A")));
	sourceReaders.add(new BufferedReader(new StringReader("1")));
	sourceReaders.add(new BufferedReader(new StringReader("")));
	StringBuilderWriter writer = new StringBuilderWriter();

	ReadersWriterMergeSort.mergeSort(sourceReaders, writer);
	assertEquals("1\nA\nB\n", writer.toString());
    }
}
