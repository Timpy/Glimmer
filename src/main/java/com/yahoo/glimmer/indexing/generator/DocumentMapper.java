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

import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.yahoo.glimmer.indexing.RDFDocument;
import com.yahoo.glimmer.indexing.RDFDocumentFactory;

public class DocumentMapper extends Mapper<LongWritable, RDFDocument, TermOccurrencePair, Occurrence> {
    static final int ALIGNMENT_INDEX = -1; // special index for alignments

    enum Counters {
	FAILED_PARSING, INDEXED_OCCURRENCES, NEGATIVE_PREDICATE_ID, NUMBER_OF_RECORDS
    }

    private String[] fields;

    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, RDFDocument, TermOccurrencePair, Occurrence>.Context context) throws IOException,
	    InterruptedException {
	Configuration conf = context.getConfiguration();
	fields = RDFDocumentFactory.getFieldsFromConf(conf);
    };

    @Override
    public void map(LongWritable key, RDFDocument doc, Context context) throws IOException, InterruptedException {
	if (doc == null || doc.getSubject() == null) {
	    // Failed parsing
	    context.getCounter(Counters.FAILED_PARSING).increment(1);
	    System.out.println("Document failed parsing");
	    return;
	}

	// used for counting # of docs per term
	Occurrence fakeDocOccurrrence = new Occurrence(null, doc.getId());

	// This is used to write the position of the last occurrence and testing
	// if the fakeDocOccurrrence for the term has already been written.
	Map<String, Integer> termToLastOccurrenceMap = new HashMap<String, Integer>();

	// Iterate over all indices
	for (int i = 0; i < fields.length; i++) {
	    Occurrence fakePredicateOccurrrence = new Occurrence(null, i);
	    Occurrence predicateOcc = new Occurrence(i, null);

	    String fieldName = fields[i];
	    if (fieldName.startsWith("NOINDEX")) {
		continue;
	    }

	    // Iterate in parallel over the words of the indices
	    MutableString term = new MutableString("");
	    MutableString nonWord = new MutableString("");
	    WordReader termReader = doc.content(i);
	    int position = 0;

	    while (termReader.next(term, nonWord)) {
		// Read next property as well
		if (term != null) {
		    String termString = term.toString();

		    // Report progress
		    context.setStatus(fields[i] + "=" + term.substring(0, Math.min(term.length(), 50)));

		    // Create an occurrence at the next position
		    Occurrence occ = new Occurrence(doc.getId(), position);
		    context.write(new TermOccurrencePair(termString, i, occ), occ);

		    Integer lastOccurrence = termToLastOccurrenceMap.get(termString);
		    if (lastOccurrence == null) {
			// Create fake occurrences the first time we encounter
			// each term (this will be
			// used for counting # of docs per term
			context.write(new TermOccurrencePair(termString, i, fakeDocOccurrrence), fakeDocOccurrrence);
			if (doc.getIndexType() == RDFDocumentFactory.IndexType.VERTICAL) {
			    context.write(new TermOccurrencePair(termString, ALIGNMENT_INDEX, fakePredicateOccurrrence), fakePredicateOccurrrence);
			}
		    }

		    // Update last occurrence
		    termToLastOccurrenceMap.put(termString, position);

		    if (doc.getIndexType() == RDFDocumentFactory.IndexType.VERTICAL) {
			// TODO Why not add to keySet? Is the number of writes
			// important?
			context.write(new TermOccurrencePair(termString, ALIGNMENT_INDEX, predicateOcc), predicateOcc);
		    }

		    position++;
		    context.getCounter(Counters.INDEXED_OCCURRENCES).increment(1);
		} else {
		    System.out.println("Nextterm is null");
		}
	    }
	    for (String termString : termToLastOccurrenceMap.keySet()) {
		Integer lastOccurrence = termToLastOccurrenceMap.get(termString);
		// Write the last occurrence as the negative positions.
		Occurrence occ = new Occurrence(doc.getId(), -1 - lastOccurrence);
		context.write(new TermOccurrencePair(termString, i, occ), occ);
	    }
	    termToLastOccurrenceMap.clear();
	}

	context.getCounter(Counters.NUMBER_OF_RECORDS).increment(1);
    }
}