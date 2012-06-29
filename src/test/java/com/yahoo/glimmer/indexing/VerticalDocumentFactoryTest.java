package com.yahoo.glimmer.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;

import org.junit.Test;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.MetadataKeys;
import com.yahoo.glimmer.indexing.VerticalDocumentFactory.VerticalDataRSSDocument;

public class VerticalDocumentFactoryTest extends AbstractDocumentFactoryTest {
    @Test
    public void test1() throws IOException {
	metadata.put(MetadataKeys.INDEXED_PROPERTIES_FILENAME, "VerticalDocumentFactoryTest.PropertiesToIndex");
	VerticalDocumentFactory factory = new VerticalDocumentFactory(metadata);
	assertEquals(3, factory.numberOfFields());
	
	VerticalDataRSSDocument document = (VerticalDataRSSDocument)factory.getDocument(rawContentInputStream, metadata);
	
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
