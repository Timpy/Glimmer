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
public class TermValue implements WritableComparable<TermValue>, Cloneable {
    // The order of this enum is important as it determines the order in which the values
    // are give in the Reducers values Iterable.
    public enum Type {
	OCCURRENCE_COUNT, LAST_OCCURRENCE, PREDICATE_ID, OCCURRENCE;
    }
    private Type type;
    private int v1;
    private int v2;

    public TermValue(Type type, int v1) {
	this.type = type;
	this.v1 = v1;
    }
    public TermValue(Type type, int v1, int v2) {
	this.type = type;
	this.v1 = v1;
	this.v2 = v2;
    }

    public TermValue() {
    }

    public TermValue(TermValue that) {
	set(that);
    }
    
    public void set(TermValue that) {
	type = that.type;
	v1 = that.v1;
	v2 = that.v2;
    }
    
    public Type getType() {
	return type;
    }
    public int getV1() {
	return v1;
    }
    public int getV2() {
	return v2;
    }

    public void readFields(DataInput in) throws IOException {
	type = Type.values()[in.readInt()];
	v1 = in.readInt();
	v2 = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
	out.writeInt(type.ordinal());
	out.writeInt(v1);
	out.writeInt(v2);
    }

    @Override
    public boolean equals(Object o) {
	if (o instanceof TermValue) {
	    TermValue that = (TermValue) o;
	    return type == that.type && v1 == that.v1 && v2 == that.v2;
	}
	return false;
    }

    @Override
    public int hashCode() {
	int hash = 7;
	hash = 31 * hash + type.hashCode();
	hash = 31 * hash + v1;
	hash = 31 * hash + v2;
	return hash;
    }

    public String toString() {
	return type.name() + "(" + v1 + "," + v2 + ")";
    }

    public int compareTo(TermValue that) {
	int i = type.compareTo(that.type);
	if (i != 0) {
	    i = v1 - that.v1;
	    if (i != 0) {
		i = v2 - that.v2;
	    }
	}
	return i;
    }
}