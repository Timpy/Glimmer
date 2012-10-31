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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class SchemaDotOrgTupleFilterTest {
    private SchemaDotOrgTupleFilter filter;
    private Tuple tuple;
    
    @Before
    public void before() {
	filter = new SchemaDotOrgTupleFilter();
	tuple = new Tuple();
    }
    
    @Test
    public void test() {
	assertFalse(filter.filter(tuple));
	assertNull(tuple.predicate.type);
	assertNull(tuple.predicate.text);
	assertNull(tuple.predicate.n3);
	
	tuple.predicate.type = TupleElement.Type.RESOURCE;
	tuple.predicate.text = "http://not.schema.org/path";
	tuple.predicate.n3 = "<http://not.schema.org/path>";
	assertFalse(filter.filter(tuple));
	assertEquals(TupleElement.Type.RESOURCE, tuple.predicate.type);
	assertEquals("http://not.schema.org/path", tuple.predicate.text);
	assertEquals("<http://not.schema.org/path>", tuple.predicate.n3);
	
	tuple.predicate.text = "http://schema.org/path";
	tuple.predicate.n3 = "<http://schema.org/path>";
	assertTrue(filter.filter(tuple));
	assertEquals(TupleElement.Type.RESOURCE, tuple.predicate.type);
	assertEquals("http://schema.org/path", tuple.predicate.text);
	assertEquals("<http://schema.org/path>", tuple.predicate.n3);
	
	tuple.predicate.text = "http://schema.org/path/property";
	tuple.predicate.n3 = "<http://schema.org/path/property>";
	assertTrue(filter.filter(tuple));
	assertEquals("http://schema.org/property", tuple.predicate.text);
	assertEquals("<http://schema.org/property>", tuple.predicate.n3);
	
	tuple.predicate.text = "http://schema.org/a/longer/path/property";
	tuple.predicate.n3 = "<http://schema.org/a/longer/path/property>";
	assertTrue(filter.filter(tuple));
	assertEquals("http://schema.org/property", tuple.predicate.text);
	assertEquals("<http://schema.org/property>", tuple.predicate.n3);
	
	tuple.predicate.text = "http://schema.org";
	tuple.predicate.n3 = "<http://schema.org>";
	assertFalse(filter.filter(tuple));
	assertEquals("http://schema.org", tuple.predicate.text);
	assertEquals("<http://schema.org>", tuple.predicate.n3);
	
	tuple.predicate.text = "http://schema.org/";
	tuple.predicate.n3 = "<http://schema.org/>";
	assertTrue(filter.filter(tuple));
	assertEquals("http://schema.org/", tuple.predicate.text);
	assertEquals("<http://schema.org/>", tuple.predicate.n3);
	
	// Should try and do something sensible when given nonsense
	tuple.predicate.text = "http://schema.org/a/longer/path/nonsense/";
	tuple.predicate.n3 = "<http://schema.org/a/longer/path/nonsense/>";
	assertTrue(filter.filter(tuple));
	assertEquals("http://schema.org/nonsense", tuple.predicate.text);
	assertEquals("<http://schema.org/nonsense>", tuple.predicate.n3);
    }
}
