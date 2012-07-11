package com.yahoo.glimmer.indexing.generator;

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

import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.mg4j.index.IndexWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yahoo.glimmer.indexing.RDFDocumentFactory;
import com.yahoo.glimmer.indexing.RDFDocumentFactory.IndexType;
import com.yahoo.glimmer.util.Util;

public class IndexRecordWriter extends RecordWriter<TermOccurrencePair, TermOccurrences> {
    private Map<Integer, IndexWrapper> indices = new HashMap<Integer, IndexWrapper>();

    public IndexRecordWriter(FileSystem fs, Path taskWorkPath, int numberOfDocs, RDFDocumentFactory.IndexType indexType, String ... fieldNames) throws IOException {
	if (indexType == RDFDocumentFactory.IndexType.VERTICAL) {
	    // Open the alignment index
	    Index index = new Index(fs, taskWorkPath, TripleIndexGenerator.ALIGNMENT_INDEX_NAME, numberOfDocs, false);
	    index.open();
	    indices.put(HorizontalBySubjectMapper.ALIGNMENT_INDEX, new IndexWrapper(index));
	}

	
	// Open one index per field
	for (int i = 0; i < fieldNames.length; i++) {
	    String name = Util.encodeFieldName(fieldNames[i]);
	    if (!name.startsWith("NOINDEX")) {

		// Get current size of heap in bytes
		long heapSize = Runtime.getRuntime().totalMemory();
		// Get maximum size of heap in bytes. The heap cannot
		// grow beyond this size.
		// Any attempt will result in an OutOfMemoryException.
		long heapMaxSize = Runtime.getRuntime().maxMemory();
		// Get amount of free memory within the heap in bytes.
		// This size will increase
		// after garbage collection and decrease as new objects
		// are created.
		long heapFreeSize = Runtime.getRuntime().freeMemory();

		System.out.println("Opening index for field:" + name + " Heap size: current/max/free: " + heapSize + "/" + heapMaxSize + "/" + heapFreeSize);

		Index index = new Index(fs, taskWorkPath, name, numberOfDocs, true);
		index.open();

		indices.put(i, new IndexWrapper(index));
	    }
	}
    }

    @Override
    public void write(TermOccurrencePair key, TermOccurrences value) throws IOException, InterruptedException {
	IndexWrapper index = indices.get(key.getIndex());
	index.write(key.getTerm(), value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	for (IndexWrapper index : indices.values()) {
	    index.close();
	}
    }

    private static class IndexWrapper {
	private final Index index;
	private int writtenOccurrenceCount;

	public IndexWrapper(Index index) {
	    this.index = index;
	}

	public void write(String term, TermOccurrences value) throws IOException {
	    if (value.getTermFrequency() > 0) {
		index.getTermsWriter().println(term);
		index.getIndexWriter().newInvertedList();
		index.getIndexWriter().writeFrequency(value.getTermFrequency());
	    } else {
		IndexWriter indexWriter = index.getIndexWriter();
		OutputBitStream out = indexWriter.newDocumentRecord();
		indexWriter.writeDocumentPointer(out, value.getDocument());
		if (index.hasPositions() && value.hasOccurrence()) {
		    indexWriter.writePositionCount(out, value.getOccurrenceCount());
		    indexWriter.writeDocumentPositions(out, value.getOccurrences(), 0, value.getOccurrenceCount(), -1);
		}
		writtenOccurrenceCount += value.getOccurrenceCount();
	    }
	}

	public void close() throws IOException {
	    index.close(writtenOccurrenceCount);
	}
    }
    
    public static class OutputFormat extends FileOutputFormat<TermOccurrencePair, TermOccurrences> {
	@Override
	public RecordWriter<TermOccurrencePair, TermOccurrences> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
	    Configuration conf = job.getConfiguration();
	    FileSystem fs = FileSystem.get(conf);

	    Path taskWorkPath = getDefaultWorkFile(job, "");

	    int numberOfDocuments = conf.getInt(TripleIndexGenerator.NUMBER_OF_DOCUMENTS, -1);
	    if (numberOfDocuments < 0) {
		throw new IllegalArgumentException("Number of documents not set.");
	    }

	    IndexType indexType = RDFDocumentFactory.getIndexType(conf);
	    String[] fields = RDFDocumentFactory.getFieldsFromConf(conf);
	    return new IndexRecordWriter(fs, taskWorkPath, numberOfDocuments, indexType, fields);
	}
    }
}
