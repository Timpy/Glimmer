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

public class Occurrence implements WritableComparable<Occurrence>, Cloneable {
    private static final int NO_DOC = -14;
    private static final int NO_POSITION = -13;
    private int document;
    private int position;

    // Hadoop needs this
    public Occurrence() {
	document = NO_DOC;
	position = NO_POSITION;
    }

    public Occurrence(Integer document, Integer position) {
	if (document == null) {
	    this.document = NO_DOC;
	} else if (document >= 0) {
	    this.document = document;
	} else {
	    throw new IllegalArgumentException("Document can not be less than 0.");
	}
	
	if (position == null) {
	    this.position = NO_POSITION;
	} else if (position >= 0) {
	    this.position = position;
	} else {
	    throw new IllegalArgumentException("Position can not be less than 0.");
	}
    }

    public Occurrence(Occurrence p) {
	this.document = p.document;
	this.position = p.position;
    }
    
    public boolean isDocSet() {
	return document != NO_DOC;
    }
    public int getDocument() {
	if (isDocSet()) {
	    return document;
	}
	throw new IllegalStateException("getDocument() called on occurrence without a document.");
    }

    public boolean isPositionSet() {
	return position != NO_POSITION;
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
    
    public void readFields(DataInput in) throws IOException {
	document = in.readInt();
	position = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
	out.writeInt(document);
	out.writeInt(position);
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
	hash = 31 * hash + document;
	hash = 31 * hash + position;
	return hash;
    }

    public String toString() {
	return "" + (isDocSet() ? document : "No Doc") + ":" + (isPositionSet() ? position : "No Pos");
    }

    public int compareTo(Occurrence that) {
	if (this.document < that.document) {
	    return -1;
	} else if (this.document > that.document) {
	    return +1;
	} else {
	    // this.document == that.document
	    if (this.position < that.position) {
		return -1;
	    } else if (this.position > that.position) {
		return +1;
	    }
	}
	return 0;
    }
}