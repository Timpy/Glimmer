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
import it.unimi.dsi.mg4j.document.DocumentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.yahoo.glimmer.indexing.RDFDocumentFactory;
import com.yahoo.glimmer.indexing.RDFInputFormat;
import com.yahoo.glimmer.indexing.VerticalDocumentFactory;
import com.yahoo.glimmer.util.Util;

public class TermOccurrencePairReduce extends Reducer<TermOccurrencePair, Occurrence, Text, Text> {
    private Map<Integer, Index> indices = new HashMap<Integer, Index>();
    private long writtenOccurrences;

    private enum Counters {
	POSTINGLIST_SIZE_OVERFLOW, POSITIONLIST_SIZE_OVERFLOW
    }

    public void setIndices(Map<Integer, Index> indices) {
	this.indices = indices;
    }
    
    @Override
    public void setup(Context context) {
	Configuration job = context.getConfiguration();
	try {

	    // Create an instance of the factory that was used...we only
	    // need this to get the number of fields
	    Class<?> documentFactoryClass = job.getClass(RDFInputFormat.DOCUMENTFACTORY_CLASS, RDFDocumentFactory.class);
	    DocumentFactory factory = RDFDocumentFactory.buildFactory(documentFactoryClass, context);

	    // Creating the output dir
	    String outputDir = job.get(TripleIndexGenerator.OUTPUT_DIR);
	    if (!outputDir.endsWith("/")) {
		outputDir = outputDir + "/";
	    }
	    outputDir += "index/";
	    FileSystem fs = FileSystem.get(job);
	    // Adding a UUID to the name of the outputdir to make sure
	    // different mappers write to different directories
	    // TODO use hadoop working dir not UUID
	    String uuid = UUID.randomUUID().toString();
	    Path path = new Path(outputDir + uuid);
	    if (!fs.exists(path)) {
		fs.mkdirs(path);
	    }

	    if (factory instanceof VerticalDocumentFactory) {
		// Open the alignment index
		Index index = new Index(fs, outputDir + uuid, TripleIndexGenerator.ALIGNMENT_INDEX_NAME, job.getInt(TripleIndexGenerator.NUMBER_OF_DOCUMENTS,
			-1), false);
		index.open();
		indices.put(DocumentMapper.ALIGNMENT_INDEX, index);
	    }

	    // Open one index per field
	    for (int i = 0; i < factory.numberOfFields(); i++) {
		String name = Util.encodeFieldName(factory.fieldName(i));
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

		    Index index = new Index(fs, outputDir + uuid, name, job.getInt(TripleIndexGenerator.NUMBER_OF_DOCUMENTS, -1), true);
		    index.open();

		    indices.put(i, index);
		}
	    }

	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    @Override
    public void reduce(TermOccurrencePair key, Iterable<Occurrence> values, Context context) throws IOException {
	if (key == null || key.equals("")) {
	    return;
	}

	context.setStatus(key.getIndex() + ":" + key.getTerm());

	// Decide which index we are going to write to
	Index currentIndex = indices.get(key.getIndex());

	// For every term, the first values are fake Occurrences introduced
	// for document counting. 
	int numDocs = 0;
	Occurrence occ = null;
	Occurrence prevOcc = null;
	Iterator<Occurrence> valuesIt = values.iterator();
	while (valuesIt.hasNext()) {
	    occ = valuesIt.next();
	    //if (occ.getDocument() == -1) {
	    if (occ.isDocSet()) {
		break;
	    }
	    if (!occ.equals(prevOcc)) {
		numDocs++;
	    }
	    prevOcc = (Occurrence) occ.clone();
	}

	// Cut off the index type prefix from the key
	currentIndex.getTermsWriter().println(key.getTerm());
	currentIndex.getIndexWriter().newInvertedList();
	currentIndex.getIndexWriter().writeFrequency(Math.min(numDocs, TripleIndexGenerator.MAX_INVERTEDLIST_SIZE));

	int[] buf = new int[TripleIndexGenerator.MAX_POSITIONLIST_SIZE];
	int posIndex = 0;
	int writtenDocs = 0;
	int prevDocID = occ.getDocument();
	while (occ != null) {
	    int docID = occ.getDocument();
	    if (docID != prevDocID) {
		// New document, write out previous postings
		OutputBitStream out = currentIndex.getIndexWriter().newDocumentRecord();
		currentIndex.getIndexWriter().writeDocumentPointer(out, prevDocID);
		if (posIndex > 0 && currentIndex.hasPositions()) {
		    currentIndex.getIndexWriter().writePositionCount(out, posIndex);
		    currentIndex.getIndexWriter().writeDocumentPositions(out, buf, 0, posIndex, -1);
		}
		writtenDocs++;
		writtenOccurrences += posIndex;
		if (writtenDocs == TripleIndexGenerator.MAX_INVERTEDLIST_SIZE) {
		    context.getCounter(Counters.POSTINGLIST_SIZE_OVERFLOW).increment(1);
		    System.err.println("More than " + TripleIndexGenerator.MAX_INVERTEDLIST_SIZE + " documents for term " + key.getTerm());
		    break;
		}
		posIndex = 0;
		if (occ.isPositionSet()) {
		    buf[posIndex++] = occ.getPosition();
		}
	    } else {
		if (posIndex > TripleIndexGenerator.MAX_POSITIONLIST_SIZE - 1) {
		    context.getCounter(Counters.POSITIONLIST_SIZE_OVERFLOW).increment(1);
		    System.err.println("More than " + TripleIndexGenerator.MAX_POSITIONLIST_SIZE + " positions for term " + key.getTerm());
		} else if (occ.isPositionSet()){
		    buf[posIndex++] = occ.getPosition();
		}
	    }

	    prevDocID = docID;
	    prevOcc = (Occurrence) occ.clone();

	    boolean last = false;
	    if (valuesIt.hasNext()) {
		occ = valuesIt.next();
		// Skip equivalent occurrences
		while (occ.equals(prevOcc) && valuesIt.hasNext()) {
		    occ = valuesIt.next();
		}
		if (occ.equals(prevOcc) && !valuesIt.hasNext()) {
		    last = true;
		}
	    } else {
		last = true;
	    }
	    if (last) {
		// This is the last occurrence: write out the remaining
		// positions
		OutputBitStream out = currentIndex.getIndexWriter().newDocumentRecord();
		currentIndex.getIndexWriter().writeDocumentPointer(out, prevDocID);
		if (currentIndex.hasPositions()) {
		    currentIndex.getIndexWriter().writePositionCount(out, posIndex);
		    currentIndex.getIndexWriter().writeDocumentPositions(out, buf, 0, posIndex, -1);
		}
		writtenOccurrences += posIndex;
		occ = null;
	    }
	}
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
	try {
	    for (Index index : indices.values()) {
		index.close(writtenOccurrences);
	    }
	    super.cleanup(context);
	} catch (Throwable throwable) {
	    throwable.printStackTrace();
	}
    }
}