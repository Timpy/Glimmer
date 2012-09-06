package com.yahoo.glimmer.indexing.preprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

public class TupleFilterSerializerTest {

    @Test(expected = IOException.class)
    public void emptyInputTest() throws IOException {
	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("".getBytes());
	TupleFilterSerializer.deserialize(byteArrayInputStream);
    }

    @Test
    public void emptyFilterTest() throws IOException {
	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("<com.yahoo.glimmer.indexing.preprocessor.RegexTupleFilter/>".getBytes());
	TupleFilter filter = TupleFilterSerializer.deserialize(byteArrayInputStream);
	assertTrue(filter instanceof RegexTupleFilter);
    }

    @Test
    public void regexFilterTest() throws IOException {
	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(("<com.yahoo.glimmer.indexing.preprocessor.RegexTupleFilter>"
		+ "<subjectPattern><pattern>subjectA</pattern><flags>0</flags></subjectPattern>"
		+ "<objectPattern><pattern>http(s)+://(host|localhost)/objectB</pattern><flags>0</flags></objectPattern>"
		+ "<andNotOrConjunction>true</andNotOrConjunction>"
		+ "</com.yahoo.glimmer.indexing.preprocessor.RegexTupleFilter>").getBytes());
	TupleFilter filter = TupleFilterSerializer.deserialize(byteArrayInputStream);
	assertTrue(filter instanceof RegexTupleFilter);
	RegexTupleFilter regexTupleFilter = (RegexTupleFilter) filter;
	assertEquals("subjectA", regexTupleFilter.getSubjectPattern().toString());
	assertEquals("http(s)+://(host|localhost)/objectB", regexTupleFilter.getObjectPattern().toString());
	assertTrue(regexTupleFilter.isAndNotOrConjunction());
    }
}
