package com.yahoo.glimmer.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.IOException;

import org.jmock.Expectations;
import org.junit.Test;

import com.yahoo.glimmer.indexing.HorizontalDocumentFactory.HorizontalDataRSSDocument;
import com.yahoo.glimmer.indexing.HorizontalDocumentFactory.MetadataKeys;

public class HorizontalDocumentFactoryTest extends AbstractDocumentFactoryTest {
    
    @Override
    protected Expectations defineExpectations() {
        Expectations expectations = super.defineExpectations();
        
        @SuppressWarnings("unchecked")
        LcpMonotoneMinimalPerfectHashFunction<CharSequence> contextHash = context.mock(LcpMonotoneMinimalPerfectHashFunction.class);
        expectations.allowing(contextHash).get("http://context/1");
        expectations.will(Expectations.returnValue(55l));
        expectations.allowing(contextHash).get("literal context");
        expectations.will(Expectations.returnValue(56l));
        
        metadata.put(MetadataKeys.CONTEXT_MPH, contextHash);
        
        return expectations; 
    }
    
    @Test
    public void test1() throws IOException {
	HorizontalDocumentFactory factory = new HorizontalDocumentFactory(metadata);
	assertEquals(4, factory.numberOfFields());
	HorizontalDataRSSDocument document = (HorizontalDataRSSDocument)factory.getDocument(rawContentInputStream, metadata);
	
	assertEquals("<http://subject/>", document.uri());
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
	assertEquals(HorizontalDocumentFactory.NO_CONTEXT, word.toString());
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
	assertEquals(HorizontalDocumentFactory.NO_CONTEXT, word.toString());
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
	
	assertEquals(3l, counters.findCounter(TripleIndexGenerator.Counters.INDEXED_TRIPLES).getValue());
    }
}
