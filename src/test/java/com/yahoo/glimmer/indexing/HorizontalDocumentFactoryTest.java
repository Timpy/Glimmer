package com.yahoo.glimmer.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;

import org.jmock.Expectations;
import org.junit.Test;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.MetadataKeys;

public class HorizontalDocumentFactoryTest extends AbstractDocumentFactoryTest {
    @Override
    protected Expectations defineExpectations() throws Exception {
        Expectations e = super.defineExpectations();
        
        e.allowing(resourcesHash).get("http://context/1");
        e.will(Expectations.returnValue(55l));
        
        return e; 
    }
    
    @Test
    public void withContextTest() throws IOException {
	metadata.put(MetadataKeys.WITH_CONTEXTS, true);
	HorizontalDocumentFactory factory = new HorizontalDocumentFactory(metadata);
	factory.setTaskAttemptContext(taskContext);
	factory.init();
	factory.setResourcesHash(resourcesHash);
	assertEquals(4, factory.numberOfFields());
	HorizontalDocument document = (HorizontalDocument)factory.getDocument(rawContentInputStream, metadata);
	
	assertEquals("http://subject/", document.uri());
	assertEquals("The Title", document.title());
	
	MutableString word = new MutableString();
	MutableString nonWord = new MutableString();
	
	// token, predicate & context are positional/parallel indexes.
	WordArrayReader tokenReader = (WordArrayReader)document.content(0);
	WordArrayReader predicateReader = (WordArrayReader)document.content(1);
	WordArrayReader contextReader = (WordArrayReader)document.content(2);
	
	assertTrue(tokenReader.next(word, nonWord));
	assertEquals("45", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(predicateReader.next(word, nonWord));
	assertEquals("http_predicate_1", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(contextReader.next(word, nonWord));
	assertEquals(RDFDocument.NULL_URL, word.toString());
	assertEquals("", nonWord.toString());
	
	assertTrue(tokenReader.next(word, nonWord));
	assertEquals("object", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(predicateReader.next(word, nonWord));
	assertEquals("http_predicate_3", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(contextReader.next(word, nonWord));
	assertEquals("55", word.toString());
	assertEquals("", nonWord.toString());
	
	assertTrue(tokenReader.next(word, nonWord));
	assertEquals("3", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(predicateReader.next(word, nonWord));
	assertEquals("http_predicate_3", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(contextReader.next(word, nonWord));
	assertEquals("55", word.toString());
	assertEquals("", nonWord.toString());
	
	assertTrue(tokenReader.next(word, nonWord));
	assertEquals("46", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(predicateReader.next(word, nonWord));
	assertEquals("http_predicate_2", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(contextReader.next(word, nonWord));
	assertEquals(RDFDocument.NULL_URL, word.toString());
	assertEquals("", nonWord.toString());
	
	assertFalse(tokenReader.next(word, nonWord));
	assertFalse(predicateReader.next(word, nonWord));
	assertFalse(contextReader.next(word, nonWord));
	
	WordArrayReader uriReader = (WordArrayReader)document.content(3);
	assertTrue(uriReader.next(word, nonWord));
	assertEquals("subject", word.toString());
	assertEquals("", nonWord.toString());
	assertFalse(uriReader.next(word, nonWord));
	
	context.assertIsSatisfied();
	
	assertEquals(3l, counters.findCounter(RDFDocumentFactory.Counters.INDEXED_TRIPLES).getValue());
    }
    
    @Test
    public void withoutContextTest() throws IOException {
	HorizontalDocumentFactory factory = new HorizontalDocumentFactory(metadata);
	factory.setTaskAttemptContext(taskContext);
	factory.init();
	factory.setResourcesHash(resourcesHash);
	assertEquals(4, factory.numberOfFields());
	HorizontalDocument document = (HorizontalDocument)factory.getDocument(rawContentInputStream, metadata);
	assertEquals("http://subject/", document.uri());
	assertEquals("The Title", document.title());
	
	MutableString word = new MutableString();
	MutableString nonWord = new MutableString();
	
	// token, predicate & context are positional/parallel indexes.
	WordArrayReader tokenReader = (WordArrayReader)document.content(0);
	WordArrayReader predicateReader = (WordArrayReader)document.content(1);
	WordArrayReader contextReader = (WordArrayReader)document.content(2);
	
	assertTrue(tokenReader.next(word, nonWord));
	assertEquals("45", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(predicateReader.next(word, nonWord));
	assertEquals("http_predicate_1", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(contextReader.next(word, nonWord));
	assertEquals(RDFDocument.NULL_URL, word.toString());
	assertEquals("", nonWord.toString());
	
	assertTrue(tokenReader.next(word, nonWord));
	assertEquals("object", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(predicateReader.next(word, nonWord));
	assertEquals("http_predicate_3", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(contextReader.next(word, nonWord));
	assertEquals(RDFDocument.NULL_URL, word.toString());
	assertEquals("", nonWord.toString());
	
	assertTrue(tokenReader.next(word, nonWord));
	assertEquals("3", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(predicateReader.next(word, nonWord));
	assertEquals("http_predicate_3", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(contextReader.next(word, nonWord));
	assertEquals(RDFDocument.NULL_URL, word.toString());
	assertEquals("", nonWord.toString());
	
	assertTrue(tokenReader.next(word, nonWord));
	assertEquals("46", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(predicateReader.next(word, nonWord));
	assertEquals("http_predicate_2", word.toString());
	assertEquals("", nonWord.toString());
	assertTrue(contextReader.next(word, nonWord));
	assertEquals(RDFDocument.NULL_URL, word.toString());
	assertEquals("", nonWord.toString());
	
	assertFalse(tokenReader.next(word, nonWord));
	assertFalse(predicateReader.next(word, nonWord));
	assertFalse(contextReader.next(word, nonWord));
	
	WordArrayReader uriReader = (WordArrayReader)document.content(3);
	assertTrue(uriReader.next(word, nonWord));
	assertEquals("subject", word.toString());
	assertEquals("", nonWord.toString());
	assertFalse(uriReader.next(word, nonWord));
	
	context.assertIsSatisfied();
	
	assertEquals(3l, counters.findCounter(RDFDocumentFactory.Counters.INDEXED_TRIPLES).getValue());
    }
}
