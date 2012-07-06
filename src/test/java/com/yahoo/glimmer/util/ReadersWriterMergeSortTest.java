package com.yahoo.glimmer.util;

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

    @Test
    public void newLinesFirstTest() throws IOException {
	List<BufferedReader> sourceReaders = new ArrayList<BufferedReader>();
	sourceReaders.add(new BufferedReader(new StringReader("A%1")));
	sourceReaders.add(new BufferedReader(new StringReader("A1")));
	sourceReaders.add(new BufferedReader(new StringReader("A")));
	StringBuilderWriter writer = new StringBuilderWriter();

	ReadersWriterMergeSort.mergeSort(sourceReaders, writer);
	assertEquals("A\nA%1\nA1\n", writer.toString());
    }
}
