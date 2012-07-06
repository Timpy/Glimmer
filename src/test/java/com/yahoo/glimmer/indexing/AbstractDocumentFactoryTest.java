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

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;

public class AbstractDocumentFactoryTest {
    protected static final Charset RAW_CHARSET = Charset.forName("UTF-8");
    protected static final String RAW_CONTENT_STRING = "http://subject/\t" +
    		"<http://subject/> <http://predicate/1> <http://object/1> \"literal context\" .  " +
    		"<http://subject/> <http://predicate/2> <http://object/2> .  " +
    		"<http://subject/> <http://predicate/3> \"object 3\"@en <http://context/1> .  ";
    
    protected Mockery context;
    protected LcpMonotoneMinimalPerfectHashFunction<CharSequence> resourcesHash;
    protected TaskInputOutputContext<?, ?, ?, ?> taskContext;
    protected Configuration conf;
    protected Counters counters = new Counters();
    protected Reference2ObjectMap<Enum<?>, Object> metadata = new Reference2ObjectOpenHashMap<Enum<?>, Object>();
    protected ByteArrayInputStream rawContentInputStream;
    
    
    protected void defineMocks(Mockery context) {
    }
    
    protected Expectations defineExpectations() throws Exception {
	return new Expectations(){{
	    allowing(resourcesHash).get("http://object/1");
	    will(returnValue(45l));
	    allowing(resourcesHash).get("http://object/2");
	    will(returnValue(46l));
	    
	    allowing(taskContext).getConfiguration();
	    will(returnValue(conf));
	    
	    // Returning null here means the factory won't try and load the file from the FileSystem.
	    allowing(conf).get(RDFDocumentFactory.RESOURCES_FILENAME_KEY);
	    will(returnValue(null));
	        
	    allowing(taskContext).getCounter(RDFDocumentFactory.Counters.INDEXED_TRIPLES);
	    will(returnValue(counters.findCounter(RDFDocumentFactory.Counters.INDEXED_TRIPLES)));
	}};
    }
    
    @SuppressWarnings("unchecked")
    @Before
    public void before() throws Exception {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	resourcesHash = context.mock(LcpMonotoneMinimalPerfectHashFunction.class, "resourcesHash");
	taskContext = context.mock(TaskInputOutputContext.class, "taskContext");
	conf = context.mock(Configuration.class, "conf");
	
	defineMocks(context);
	
	context.checking(defineExpectations());
	
	metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "UTF-8");
	metadata.put(PropertyBasedDocumentFactory.MetadataKeys.TITLE, "The Title");
	
	rawContentInputStream = new ByteArrayInputStream(RAW_CONTENT_STRING.getBytes(RAW_CHARSET));
    }
}
