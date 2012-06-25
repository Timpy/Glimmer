package com.yahoo.glimmer.indexing.preprocessor;

import static org.junit.Assert.assertEquals;
import org.apache.hadoop.io.Text;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

public class TextMatcher extends BaseMatcher<Text> {
    private final String expected;
    
    public TextMatcher(String expected) {
	this.expected = expected;
    }
    
    @Override
    public boolean matches(Object object) {
	if (object instanceof Text) {
	    assertEquals(expected, object.toString());
	    return true;
	}
	return false;
    }

    @Override
    public void describeTo(Description description) {
	description.appendText("Text object matching >" + expected + "<");
    }
}
