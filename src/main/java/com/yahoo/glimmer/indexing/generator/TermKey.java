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
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 
 * @author pmika
 * 
 */
public class TermKey implements WritableComparable<TermKey> {
    private int index;
    private String term;
    // We want hadoop to sort our values too. So the initial sort includes the
    // value in the key. The FirstGroupingComparator is then used to group all
    // values for each index/term pair.
    private TermValue value = new TermValue();

    /*
     * Required for Hadoop
     */
    public TermKey() {
    }

    public TermKey(String term, int index, TermValue value) {
	this.index = index;
	this.term = term;
	this.value = value;
    }

    public String getTerm() {
	return term;
    }

    public int getIndex() {
	return index;
    }

    public TermValue getValue() {
	return value;
    }

    public void readFields(DataInput in) throws IOException {
	value.readFields(in);
	index = in.readInt();
	term = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
	value.write(out);
	out.writeInt(index);
	Text.writeString(out, term);
    }

    public int compareTo(TermKey top) {
	if (!term.equals(top.term)) {
	    return term.compareTo(top.term);
	} else if (index != top.index) {
	    return ((Integer) index).compareTo(top.index);
	} else {
	    return value.compareTo(top.value);
	}
    }

    @Override
    public int hashCode() {
	int hash = 31 * value.hashCode() + index;
	return 31 * hash + term.hashCode();
    }

    @Override
    public boolean equals(Object right) {
	if (right instanceof TermKey) {
	    TermKey r = (TermKey) right;
	    return term.equals(r.term) && index == r.index && (value == null ? r.value == null : value.equals(r.value));
	} else {
	    return false;
	}
    }

    public String toString() {
	return Integer.toString(index) + ":" + term + ":" + (value == null ? "null" : value.toString());
    }

    /** A Comparator that compares serialized TermKey objects. */
    public static class Comparator extends WritableComparator {
	public Comparator() {
	    super(TermKey.class, true);
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    // Compare the term
	    try {
		// first byte of string encodes the length of the size
		int length1 = WritableUtils.decodeVIntSize(b1[s1 + 3 * Integer.SIZE / 8]);
		int length2 = WritableUtils.decodeVIntSize(b2[s2 + 3 * Integer.SIZE / 8]);

		String term1 = Text.decode(b1, s1 + 3 * Integer.SIZE / 8 + length1, l1 - (3 * Integer.SIZE / 8 + length1));
		String term2 = Text.decode(b2, s2 + 3 * Integer.SIZE / 8 + length2, l2 - (3 * Integer.SIZE / 8 + length2));

		int result = term1.compareTo(term2);

		if (result != 0) {
		    return result;
		} else {
		    // Compare the index
		    int index1 = WritableComparator.readInt(b1, s1 + 2 * Integer.SIZE / 8);
		    int index2 = WritableComparator.readInt(b2, s2 + 2 * Integer.SIZE / 8);
		    if (index1 > index2) {
			return 1;
		    } else if (index1 < index2) {
			return -1;
		    } else {
			// Compare the doc
			int doc1 = WritableComparator.readInt(b1, s1);
			int doc2 = WritableComparator.readInt(b2, s2);
			if (doc1 > doc2) {
			    return 1;
			} else if (doc1 < doc2) {
			    return -1;
			} else {
			    // Compare the position
			    int pos1 = WritableComparator.readInt(b1, s1 + Integer.SIZE / 8);
			    int pos2 = WritableComparator.readInt(b2, s2 + Integer.SIZE / 8);
			    if (pos1 > pos2) {
				return 1;
			    } else if (pos1 < pos2) {
				return -1;
			    } else {
				return 0;
			    }
			}
		    }
		}
	    } catch (CharacterCodingException e) {
		e.printStackTrace();
	    }
	    return 0;
	}
    }

    /**
     * Compare only the term and index of the pair, so that reduce is called
     * once for each value of the first part.
     * 
     * NOTE: first part (i.e. index and term) are serialized first
     */
    public static class FirstGroupingComparator implements RawComparator<TermKey> {

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    // Skip the first two integers
	    int intsize = Integer.SIZE / 8;
	    return WritableComparator.compareBytes(b1, s1 + intsize * 2, l1 - intsize * 2, b2, s2 + intsize * 2, l2 - intsize * 2);
	}

	public int compare(TermKey o1, TermKey o2) {
	    if (!o1.getTerm().equals(o2.getTerm())) {
		return o1.getTerm().compareTo(o2.getTerm());
	    } else if (o1.getIndex() != o2.getIndex()) {
		return ((Integer) o1.getIndex()).compareTo(o2.getIndex());
	    }
	    return 0;
	}
    }

    /**
     * Partition based only on the term. All occurrences of a term are processed
     * by the same reducer instance.
     */
    public static class FirstPartitioner extends HashPartitioner<TermKey, TermValue> {
	@Override
	public int getPartition(TermKey key, TermValue value, int numPartitions) {
	    return Math.abs(key.getTerm().hashCode() * 127) % numPartitions;
	}
    }
}