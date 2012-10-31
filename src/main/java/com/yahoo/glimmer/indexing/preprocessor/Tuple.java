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

import com.yahoo.glimmer.indexing.preprocessor.TuplesToResourcesMapper.TupleElementName;

public class Tuple {
    public TupleElement subject = new TupleElement();
    public TupleElement predicate = new TupleElement();
    public TupleElement object = new TupleElement();
    public TupleElement context = new TupleElement();
    
    public TupleElement getElement(TupleElementName name) {
        switch (name) {
        case SUBJECT : return subject;
        case PREDICATE : return predicate;
        case OBJECT : return object;
        case CONTEXT : return context;
        default: return null;
        }
    }
}