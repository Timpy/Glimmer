package com.yahoo.glimmer.indexing.generator;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * Compare only the term and index of the pair, so that reduce is called once for
 * each value of the first part.
 * 
 * NOTE: first part (i.e. index and term) are serialized first
 */
public class FirstGroupingComparator implements RawComparator<TermOccurrencePair> {

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	// Skip the first two integers
	int intsize = Integer.SIZE / 8;
	return WritableComparator.compareBytes(b1, s1 + intsize * 2, l1 - intsize * 2, b2, s2 + intsize * 2, l2 - intsize * 2);
    }

    public int compare(TermOccurrencePair o1, TermOccurrencePair o2) {
	if (!o1.term.equals(o2.term)) {
	    return o1.term.compareTo(o2.term);
	} else if (o1.index != o2.index) {
	    return ((Integer) o1.index).compareTo(o2.index);
	}
	return 0;
    }
}