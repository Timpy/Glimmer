package com.yahoo.glimmer.indexing.generator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IndexRecordWriterTermValue implements IndexRecordWriterValue {
    private String term;
    private int termFrequency;
    private int occurrenceCount;
    private long sumOfMaxTermPositions;

    public String getTerm() {
	return term;
    }

    public void setTerm(String term) {
	this.term = term;
    }

    public int getTermFrequency() {
	return termFrequency;
    }

    public void setTermFrequency(int termFrequency) {
	this.termFrequency = termFrequency;
    }

    public int getOccurrenceCount() {
	return occurrenceCount;
    }

    public void setOccurrenceCount(int occurrenceCount) {
	this.occurrenceCount = occurrenceCount;
    }

    public long getSumOfMaxTermPositions() {
	return sumOfMaxTermPositions;
    }

    public void setSumOfMaxTermPositions(long sumOfMaxTermPositions) {
	this.sumOfMaxTermPositions = sumOfMaxTermPositions;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
	// TODO
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
	// TODO
    }

    @Override
    public int compareTo(IndexRecordWriterValue value) {
	if (value instanceof IndexRecordWriterTermValue) {
	    IndexRecordWriterTermValue that = (IndexRecordWriterTermValue) value;
	    long i = term.compareTo(that.term);
	    if (i != 0) {
		i = termFrequency - that.termFrequency;
		if (i != 0) {
		    i = occurrenceCount - that.occurrenceCount;
		    if (i != 0) {
			i = sumOfMaxTermPositions - that.sumOfMaxTermPositions;
		    }
		}
	    }
	    if (i > 0) {
		return 1;
	    } else if (i < 0) {
		return -1;
	    }
	    return 0;
	}
	return -1;
    }
    
    @Override
    public boolean equals(Object o) {
	if (o instanceof IndexRecordWriterTermValue) {
	    IndexRecordWriterTermValue that = (IndexRecordWriterTermValue) o;
	    if (termFrequency == that.termFrequency && occurrenceCount == that.occurrenceCount && sumOfMaxTermPositions == that.sumOfMaxTermPositions) {
		return term.equals(that.term);
	    }
	}
	return false;
    }
    
    @Override
    public int hashCode() {
	int hash = 11;
	hash = 31 * hash + term.hashCode();
	hash = 31 * hash + termFrequency;
	hash = 31 * hash + occurrenceCount;
	hash = 31 * hash + (int)(sumOfMaxTermPositions ^ (sumOfMaxTermPositions >>> 32));
	return hash;
    }
    
    @Override
    public String toString() {
        return term + ':' + termFrequency + ':' + occurrenceCount + ':' + sumOfMaxTermPositions;
    }
}
