package com.yahoo.glimmer.indexing;

import it.unimi.dsi.lang.MutableString;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.glimmer.indexing.BySubjectCollectionBuilder.MapClass;

public class BySubjectCollectionBuilderTest {
    private static final String SUBJECT_RECORD_1 = "3\t" +
    		"http://subject1/\t" + 
    		"<http://predicate11> \"literal1\" .  " + 
    		"<http://predicate12> <http://resource1> .";
    
    private static final String SUBJECT_RECORD_2 = "7\t" +
	    "http://subject2/\t" + 
	    "<http://predicate21> <http://resource2> .";
    
    private Mockery context;
    private Mapper<LongWritable, Text, MutableString, MutableString>.Context mapperContext;

    @SuppressWarnings("unchecked")
    @Before
    public void before() {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	mapperContext = context.mock(Mapper.Context.class, "mapperContext");
    }
    
    @Test
    public void mapTest() throws IOException, InterruptedException {
	context.checking(new Expectations() {{
	    // doc 1
	    one(mapperContext).write(with(new MutableString("http://subject1/")), with(new MutableString("http://subject1/")));
	    one(mapperContext).write(with(new MutableString("")), with(new MutableString("<")));
	    one(mapperContext).write(with(new MutableString("http")), with(new MutableString("://")));
	    one(mapperContext).write(with(new MutableString("predicate11")), with(new MutableString("> \"")));
	    one(mapperContext).write(with(new MutableString("literal1")), with(new MutableString("\" .  <")));
	    one(mapperContext).write(with(new MutableString("http")), with(new MutableString("://")));
	    one(mapperContext).write(with(new MutableString("predicate12")), with(new MutableString("> <")));
	    one(mapperContext).write(with(new MutableString("http")), with(new MutableString("://")));
	    one(mapperContext).write(with(new MutableString("resource1")), with(new MutableString("> .")));
	    // End doc 1.
	    one(mapperContext).write(with(new MutableString(BySubjectCollectionBuilder.COMMAND_KEY)), with(BySubjectCollectionBuilder.END_DOC_VALUE));
	    
	    // Empty docs.
	    exactly(7 - 3 - 1).of(mapperContext).write(with(BySubjectCollectionBuilder.COMMAND_KEY), with(BySubjectCollectionBuilder.EMPTY_DOC_VALUE));
	    
	    // doc 2
	    one(mapperContext).write(with(new MutableString("http://subject2/")), with(new MutableString("http://subject2/")));
	    one(mapperContext).write(with(new MutableString("")), with(new MutableString("<")));
	    one(mapperContext).write(with(new MutableString("http")), with(new MutableString("://")));
	    one(mapperContext).write(with(new MutableString("predicate21")), with(new MutableString("> <")));
	    one(mapperContext).write(with(new MutableString("http")), with(new MutableString("://")));
	    one(mapperContext).write(with(new MutableString("resource2")), with(new MutableString("> .")));
	    // End doc 2.
	    one(mapperContext).write(with(new MutableString(BySubjectCollectionBuilder.COMMAND_KEY)), with(BySubjectCollectionBuilder.END_DOC_VALUE));
	}});
	
	MapClass mapper = new MapClass();
	LongWritable key = new LongWritable(7);
	Text value = new Text(SUBJECT_RECORD_1);
	mapper.map(key, value, mapperContext);
	key.set(8);
	value.set(SUBJECT_RECORD_2);
	mapper.map(key, value, mapperContext);
	
	context.assertIsSatisfied();
    }
}
