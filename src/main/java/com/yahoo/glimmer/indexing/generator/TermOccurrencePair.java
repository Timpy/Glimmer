package com.yahoo.glimmer.indexing.generator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * 
 * @author pmika
 * 
 */
public class TermOccurrencePair implements WritableComparable<TermOccurrencePair> {
    String term;
    Occurrence occ = new Occurrence();
    int index;

    /*
     * Required for Hadoop
     */
    public TermOccurrencePair() {
    }

    public TermOccurrencePair(String term, int index, Occurrence occurrence) {
	this.index = index;
	this.term = term;
	this.occ = occurrence;
    }

    public String getTerm() {
	return term;
    }

    public int getIndex() {
	return index;
    }

    public void readFields(DataInput in) throws IOException {
	occ.readFields(in);
	index = in.readInt();
	term = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
	occ.write(out);
	out.writeInt(index);
	Text.writeString(out, term);
    }

    public int compareTo(TermOccurrencePair top) {
	if (!term.equals(top.term)) {
	    return term.compareTo(top.term);
	} else if (index != top.index) {
	    return ((Integer) index).compareTo(top.index);
	} else {
	    return occ.compareTo(top.occ);
	}
    }

    @Override
    public int hashCode() {
	int hash = 31 * occ.hashCode() + index;
	return 31 * hash + term.hashCode();
    }

    @Override
    public boolean equals(Object right) {
	if (right instanceof TermOccurrencePair) {
	    TermOccurrencePair r = (TermOccurrencePair) right;
	    return term.equals(r.term) && index == r.index && occ.equals(r.occ);
	} else {
	    return false;
	}
    }

    public String toString() {
	return Integer.toString(index) + ":" + term + ":" + occ.toString();
    }

    /** A Comparator that compares serialized TermOccurrencePair. */
    public static class Comparator extends WritableComparator {
	public Comparator() {
	    super(TermOccurrencePair.class, true);
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
}