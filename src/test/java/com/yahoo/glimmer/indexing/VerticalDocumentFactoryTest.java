package com.yahoo.glimmer.indexing;

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
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.jmock.Expectations;
import org.junit.Test;

public class VerticalDocumentFactoryTest extends AbstractDocumentFactoryTest {
    @Override
    protected Expectations defineExpectations() throws Exception {
        Expectations e = super.defineExpectations();
        
        // Returning null here means the factory won't try and load the predicates 
        e.allowing(conf).get(VerticalDocumentFactory.PREDICATES_FILENAME_KEY);
        e.will(Expectations.returnValue(null));
        
        return e; 
    }
    
    @Test
    public void test1() throws IOException {
	VerticalDocumentFactory factory = new VerticalDocumentFactory(metadata);
	factory.setTaskAttemptContext(taskContext);
	factory.init();
	factory.setResourcesHash(resourcesHash);
	List<String> indexedProperties = Arrays.asList(new String[]{
		"http://predicate/1",
		"http://predicate/2", 
		"http://predicate/3"});
	factory.setIndexedProperties(indexedProperties);
	assertEquals(3, factory.numberOfFields());
	
	VerticalDocument document = (VerticalDocument)factory.getDocument(rawContentInputStream, metadata);
	
	assertEquals("http://subject/", document.uri());
	assertEquals("The Title", document.title());
	
	MutableString word = new MutableString();
	MutableString nonWord = new MutableString();
	
	WordArrayReader reader = (WordArrayReader)document.content(0);
	assertTrue(reader.next(word, nonWord));
	assertEquals("45", word.toString());
	assertEquals("", nonWord.toString());
	assertFalse(reader.next(word, nonWord));
	
	reader = (WordArrayReader)document.content(1);
	assertTrue(reader.next(word, nonWord));
	assertEquals("46", word.toString());
	assertEquals("", nonWord.toString());
	assertFalse(reader.next(word, nonWord));
	
	reader = (WordArrayReader)document.content(2);
	assertTrue(reader.next(word, nonWord));
	assertEquals("object", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(reader.next(word, nonWord));
	assertEquals("3", word.toString());
	assertEquals("", nonWord.toString());
	assertFalse(reader.next(word, nonWord));
	
	context.assertIsSatisfied();
	
	assertEquals(3l, counters.findCounter(RDFDocumentFactory.Counters.INDEXED_TRIPLES).getValue());
    }
}
