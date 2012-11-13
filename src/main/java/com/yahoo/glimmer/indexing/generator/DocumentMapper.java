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
import com.yahoo.glimmer.indexing.generator.TermValue.Type;

public class DocumentMapper extends Mapper<LongWritable, RDFDocument, TermKey, TermValue> {
    public static final int ALIGNMENT_INDEX = -1; // special index for
						  // alignments

    enum Counters {
	FAILED_PARSING, INDEXED_OCCURRENCES, NUMBER_OF_RECORDS
    }

    private String[] fields;

    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, RDFDocument, TermKey, TermValue>.Context context) throws IOException,
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

	// This is used to write the position of the last occurrence and testing
	// if the fakeDocOccurrrence for the term has already been written.
	Map<String, DocStat> termToDocStatMap = new HashMap<String, DocStat>();

	// Iterate over all indices
	for (int i = 0; i < fields.length; i++) {
	    TermValue indexIdValue = new TermValue(Type.INDEX_ID, i);

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
		    TermValue occurrenceValue = new TermValue(Type.OCCURRENCE, doc.getId(), position);
		    context.write(new TermKey(termString, i, occurrenceValue), occurrenceValue);

		    DocStat docStat = termToDocStatMap.get(termString);
		    if (docStat == null) {
			if (doc.getIndexType() == RDFDocumentFactory.IndexType.VERTICAL) {
			    // For the Alignment Index, we write the predicate
			    // id(Which is equal to the index id for a VERTICAL
			    // index) the first time we encounter a term.
			    // The 'Alignment Index' is an index without counts
			    // or positions. It's used for query optimization in
			    // the query parse. The resulting 'alignment index'
			    // is basically used as a map from term to
			    // predicates that term occures in.
			    context.write(new TermKey(termString, ALIGNMENT_INDEX, indexIdValue), indexIdValue);
			}
			docStat = new DocStat();
			docStat.last = position;
			docStat.count = 1;
			termToDocStatMap.put(termString, docStat);
		    } else {
			docStat.last = position;
			docStat.count++;
		    }

		    position++;
		    context.getCounter(Counters.INDEXED_OCCURRENCES).increment(1);
		} else {
		    System.out.println("Nextterm is null");
		}
	    }

	    for (String termString : termToDocStatMap.keySet()) {
		DocStat docStat = termToDocStatMap.get(termString);
		TermValue occurrenceCountValue = new TermValue(Type.DOC_STATS, docStat.count, docStat.last);
		context.write(new TermKey(termString, i, occurrenceCountValue), occurrenceCountValue);
	    }
	    termToDocStatMap.clear();
	}

	context.getCounter(Counters.NUMBER_OF_RECORDS).increment(1);
    }

    private static class DocStat {
	int last;
	int count;
    }
}