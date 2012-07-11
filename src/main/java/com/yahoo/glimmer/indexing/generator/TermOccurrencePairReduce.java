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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

public class TermOccurrencePairReduce extends Reducer<TermOccurrencePair, Occurrence, TermOccurrencePair, TermOccurrences> {
    private static final int MAX_INVERTEDLIST_SIZE = 50000000;
    private static final int MAX_POSITIONLIST_SIZE = 1000000;
    
    private TermOccurrences termOccurrences;

    private enum Counters {
	POSTINGLIST_SIZE_OVERFLOW, POSITIONLIST_SIZE_OVERFLOW, POSITIONLIST_SIZE_OVERFLOW_TIMES
    }
    
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer<TermOccurrencePair,Occurrence,TermOccurrencePair,TermOccurrences>.Context context) throws IOException ,InterruptedException {
	// termOccurrences and the buffer are reused for every call to context.write()
	termOccurrences = new TermOccurrences(MAX_POSITIONLIST_SIZE);
    };
    
    @Override
    public void reduce(TermOccurrencePair key, Iterable<Occurrence> values, Context context) throws IOException, InterruptedException {
	if (key == null || key.equals("")) {
	    return;
	}

	context.setStatus(key.getIndex() + ":" + key.getTerm());

	int numDocs = 0;
	Occurrence occ = null;
	Occurrence prevOcc = new Occurrence();
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
	    prevOcc.set(occ);
	}
	
	// write the document frequency for this term.
	termOccurrences.setTermFrequency(Math.min(numDocs, MAX_INVERTEDLIST_SIZE));
	context.write(key, termOccurrences);
	termOccurrences.setTermFrequency(0);
	
	int writtenDocs = 0;
	prevOcc.set(occ);
	while (occ != null) {
	    int docID = occ.getDocument();
	    if (docID != prevOcc.getDocument()) {
		// New document, write out previous postings
		termOccurrences.setDocument(prevOcc.getDocument());
		context.write(key, termOccurrences);
		termOccurrences.clearOccerrences();
		writtenDocs++;
		
		if (writtenDocs >= MAX_INVERTEDLIST_SIZE) {
		    context.getCounter(Counters.POSTINGLIST_SIZE_OVERFLOW).increment(1);
		    System.err.println("More than " + MAX_INVERTEDLIST_SIZE + " documents for term " + key.getTerm());
		    break;
		}
		if (occ.isPositionSet()) {
		    termOccurrences.addOccurrence(occ.getPosition());
		}
	    } else if (occ.isPositionSet()) {
		if (!termOccurrences.addOccurrence(occ.getPosition())) {
		    context.getCounter(Counters.POSITIONLIST_SIZE_OVERFLOW).increment(1);
		    System.err.println("More than " + MAX_POSITIONLIST_SIZE + " positions for term " + key.getTerm());
		}
	    }

	    prevOcc.set(occ);

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
		termOccurrences.setDocument(prevOcc.getDocument());
		context.write(key, termOccurrences);
		
		termOccurrences.clearOccerrences();
		occ = null;
	    }
	}
    }
}