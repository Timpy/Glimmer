package com.yahoo.glimmer.indexing;

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.MetadataKeys;

public class AbstractDocumentFactoryTest {
    protected static final Charset RAW_CHARSET = Charset.forName("UTF-8");
    protected static final String RAW_CONTENT_STRING = "http://subject/\t" +
    		"<http://subject/> <http://predicate/1> <http://object/1> \"literal context\" .  " +
    		"<http://subject/> <http://predicate/2> <http://object/2> .  " +
    		"<http://subject/> <http://predicate/3> \"object 3\"@en <http://context/1> .  ";
    
    protected Mockery context;
    protected LcpMonotoneMinimalPerfectHashFunction<CharSequence> resourcesHash;
    protected Mapper<?, ?, ?, ?>.Context mapContext;
    protected Counters counters = new Counters();
    protected Reference2ObjectMap<Enum<?>, Object> metadata = new Reference2ObjectOpenHashMap<Enum<?>, Object>();
    protected ByteArrayInputStream rawContentInputStream;
    
    
    protected Expectations defineExpectations() {
	return new Expectations(){{
	    allowing(resourcesHash).get("http://object/1");
	    will(returnValue(45l));
	    allowing(resourcesHash).get("http://object/2");
	    will(returnValue(46l));
	    allowing(mapContext).getCounter(RDFDocumentFactory.Counters.INDEXED_TRIPLES);
	    will(returnValue(counters.findCounter(RDFDocumentFactory.Counters.INDEXED_TRIPLES)));
	}};
    }
    
    @SuppressWarnings("unchecked")
    @Before
    public void before() {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	resourcesHash = context.mock(LcpMonotoneMinimalPerfectHashFunction.class, "resourcesHash");
	mapContext = context.mock(Mapper.Context.class);
	
	context.checking(defineExpectations());
	
	metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "UTF-8");
	metadata.put(PropertyBasedDocumentFactory.MetadataKeys.TITLE, "The Title");
	metadata.put(MetadataKeys.RESOURCES_HASH, resourcesHash);
	metadata.put(MetadataKeys.MAPPER_CONTEXT, mapContext);
	
	rawContentInputStream = new ByteArrayInputStream(RAW_CONTENT_STRING.getBytes(RAW_CHARSET));
    }
}
