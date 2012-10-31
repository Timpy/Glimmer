package com.yahoo.glimmer.indexing.preprocessor;

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

public class TupleElement {
    public static enum Type {
	BNODE, LITERAL, RESOURCE, UNBOUND, VARIABLE;
    }

    public Type type;
    public String text;
    public String n3;

    public boolean isOfType(Type... types) {
	for (Type type : types) {
	    if (this.type == type) {
		return true;
	    }
	}
	return false;
    }
}