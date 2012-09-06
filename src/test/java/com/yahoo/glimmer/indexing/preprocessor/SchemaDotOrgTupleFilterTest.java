package com.yahoo.glimmer.indexing.preprocessor;

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
