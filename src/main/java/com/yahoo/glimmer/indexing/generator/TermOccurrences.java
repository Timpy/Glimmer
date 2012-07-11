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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Writable;

/**
 * This is only used to pass the results from the Reducer to the RecordWriter.
 * So object of this type shouldn't be compared or serialized by Hadoop.
 * 
 * @author tep
 */
public class TermOccurrences implements Writable, Cloneable {
    // This is the number of times the term occurs in all docs not just this
    // doc.
    private int termFrequency;
    private int document;
    private int[] occurrences;
    private int occurrenceCount;

    public TermOccurrences(int occurrencesBufferSize) {
	occurrences = new int[occurrencesBufferSize];
    }
    
    public int getTermFrequency() {
	return termFrequency;
    }

    public void setTermFrequency(int termFrequency) {
	this.termFrequency = termFrequency;
	document = 0;
	clearOccerrences();
    }

    public int getDocument() {
	return document;
    }

    public void setDocument(int document) {
	this.document = document;
	termFrequency = 0;
    }


    public void clearOccerrences() {
	occurrenceCount = 0;
    }

    /**
     * @param occurrence
     *            to add at next to end of list
     * @return true if occurrence was added.
     */
    public boolean addOccurrence(int occurrence) {
	if (occurrenceCount >= occurrences.length) {
	    return false;
	}
	occurrences[occurrenceCount++] = occurrence;
	return true;
    }

    public boolean hasOccurrence() {
	return occurrenceCount > 0;
    }

    public int getOccurrenceCount() {
	return occurrenceCount;
    }

    public int[] getOccurrences() {
	return occurrences;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	throw new NotImplementedException();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	throw new NotImplementedException();
    }

    @Override
    public boolean equals(Object o) {
	if (o instanceof TermOccurrences) {
	    TermOccurrences that = (TermOccurrences) o;
	    if (termFrequency == that.termFrequency && document == that.document && occurrenceCount == that.occurrenceCount) {
		for (int i = 0; i < occurrenceCount; i++) {
		    if (occurrences[i] != that.occurrences[i]) {
			return false;
		    }
		}
		return true;
	    }
	}
	return false;
    }

    @Override
    public int hashCode() {
	int hash = 7;
	hash = 31 * hash + termFrequency;
	hash = 31 * hash + document;
	hash = 31 * hash + Arrays.hashCode(occurrences);
	return hash;
    }

    public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append(termFrequency);
	sb.append(':');
	sb.append(document);
	sb.append(" (");
	for (int i = 0 ; i < occurrenceCount ; i++) {
	    if (i > 0) {
		sb.append(',');
	    }
	    sb.append(occurrences[i]);
	}
	sb.append(')');
	return sb.toString();
    }
}