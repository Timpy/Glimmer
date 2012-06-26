package com.yahoo.glimmer.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.MetadataKeys;
import com.yahoo.glimmer.indexing.VerticalDocumentFactory.VerticalDataRSSDocument;

public class VerticalDocumentFactoryTest {
    private static final Charset RAW_CHARSET = Charset.forName("UTF-8");
    private Mockery context;
    private LcpMonotoneMinimalPerfectHashFunction<CharSequence> subjectsHash;
    private Mapper<?, ?, ?, ?>.Context mapContext;
    private Counter indexedTriplesCounter = new Counter() {};
    
    @SuppressWarnings("unchecked")
    @Before
    public void before() {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	subjectsHash = context.mock(LcpMonotoneMinimalPerfectHashFunction.class, "subjectsHash");
	mapContext = context.mock(Mapper.Context.class);
    }
    
    @Test
    public void test1() throws IOException {
	context.checking(new Expectations(){{
	    allowing(subjectsHash).get("http://object/1");
	    will(returnValue(0l));
	    allowing(subjectsHash).get("http://object/2");
	    will(returnValue(1l));
	    allowing(mapContext).getCounter(TripleIndexGenerator.Counters.INDEXED_TRIPLES);
	    will(returnValue(indexedTriplesCounter));
	}});
	
	Reference2ObjectMap<Enum<?>, Object> metadata = new Reference2ObjectOpenHashMap<Enum<?>, Object>();
	metadata.put(MetadataKeys.INDEXED_PROPERTIES_FILENAME, "VerticalDocumentFactoryTest.PropertiesToIndex");
	metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "UTF-8");
	metadata.put(PropertyBasedDocumentFactory.MetadataKeys.TITLE, "The Title");
	metadata.put(MetadataKeys.SUBJECTS_MPH, subjectsHash);
	metadata.put(MetadataKeys.MAPPER_CONTEXT, mapContext);
	
	VerticalDocumentFactory factory = new VerticalDocumentFactory(metadata);
	
	String docString = "<http://subject/>\t" +
			"<http://subject/> <http://predicate/1> <http://object/1> .  " +
			"<http://subject/> <http://predicate/2> <http://object/2> .  " +
			"<http://subject/> <http://predicate/3> \"object 3\"@en .  ";
	ByteArrayInputStream rawContentInputStream = new ByteArrayInputStream(docString.getBytes(RAW_CHARSET));
	
	VerticalDataRSSDocument document = (VerticalDataRSSDocument)factory.getDocument(rawContentInputStream, metadata);
	
	assertEquals("<http://subject/>", document.uri());
	assertEquals("The Title", document.title());
	
	MutableString word = new MutableString();
	MutableString nonWord = new MutableString();
	
	WordArrayReader reader = (WordArrayReader)document.content(0);
	assertTrue(reader.next(word, nonWord));
	assertEquals("0", word.toString());
	assertEquals("", nonWord.toString());
	assertFalse(reader.next(word, nonWord));
	
	reader = (WordArrayReader)document.content(1);
	assertTrue(reader.next(word, nonWord));
	assertEquals("1", word.toString());
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
	
	assertEquals(3l, indexedTriplesCounter.getValue());
    }
}
