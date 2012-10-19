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

import org.apache.hadoop.io.WritableComparable;

/**
 * The value that is passed between the mapper and reducer.
 * 
 * Can represent different thing depending on how the document and position are
 * set.
 * 
 * If both document and position are set and are positive - The Occurrence is
 * the occurrence of keys term in the given document at the given position.
 * 
 * If both document and position are set but the position is negative - The
 * Occurrence represents the last position the term occurs at. Where the
 * position is the absolute value minus 1.
 * 
 * If document is not set, and the index is not the alignment index - The
 * position is the doc id. This is used to compute the term frequency(Document
 * containing this term).
 * 
 * If document is not set, and the index is the alignment index - The position
 * is index/predicate id.
 * 
 * 
 * 
 * @author tep
 * 
 */
public class Occurrence implements WritableComparable<Occurrence>, Cloneable {
    private Integer document;
    private Integer position;

    public Occurrence(Integer document, Integer position) {
	if (document != null && document < 0) {
	    throw new IllegalArgumentException("Document can not be less than 0.");
	}
	this.document = document;
	this.position = position;
    }

    public Occurrence() {
    }

    public Occurrence(Occurrence p) {
	this.document = p.document;
	this.position = p.position;
    }

    public boolean isDocSet() {
	return document != null;
    }

    public int getDocument() {
	if (isDocSet()) {
	    return document;
	}
	throw new IllegalStateException("getDocument() called on occurrence without a document.");
    }

    public boolean isPositionSet() {
	return position != null;
    }

    public int getPosition() {
	if (isPositionSet()) {
	    return position;
	}
	throw new IllegalStateException("getPosition() called on occurrence without a position.");
    }

    public void set(Occurrence occ) {
	document = occ.document;
	position = occ.position;
    }

    private static final int NOT_SET_SERIALIZATION_VALUE = Integer.MIN_VALUE;

    public void readFields(DataInput in) throws IOException {
	document = in.readInt();
	if (document == NOT_SET_SERIALIZATION_VALUE) {
	    document = null;
	}
	position = in.readInt();
	if (position == NOT_SET_SERIALIZATION_VALUE) {
	    position = null;
	}
    }

    public void write(DataOutput out) throws IOException {
	if (isDocSet()) {
	    out.writeInt(document);
	} else {
	    out.write(NOT_SET_SERIALIZATION_VALUE);
	}
	if (isPositionSet()) {
	    out.writeInt(position);
	} else {
	    out.write(NOT_SET_SERIALIZATION_VALUE);
	}
    }

    @Override
    public boolean equals(Object o) {
	if (o instanceof Occurrence) {
	    if (((Occurrence) o).document == document && ((Occurrence) o).position == position) {
		return true;
	    }
	}
	return false;
    }

    @Override
    public int hashCode() {
	int hash = 7;
	if (isDocSet()) {
	    hash = 31 * hash + document;
	}
	if (isPositionSet()) {
	    hash = 31 * hash + position;
	}
	return hash;
    }

    public String toString() {
	return "" + (isDocSet() ? document : "No Doc") + ":" + (isPositionSet() ? position : "No Pos");
    }

    public int compareTo(Occurrence that) {
	int i = nullSafeCompareTo(this.document, that.document, true);
	if (i != 0) {
	    i = nullSafeCompareTo(this.position, that.position, true);
	}
	return i;
    }

    private static <T extends Comparable<T>> int nullSafeCompareTo(T a, T b, boolean nullsFirst) {
	if (a == null) {
	    if (b == null) {
		return 0;
	    }
	    return nullsFirst ? -1 : 1;
	} else if (b == null) {
	    return nullsFirst ? 1 : -1;
	}
	return a.compareTo(b);
    }
}