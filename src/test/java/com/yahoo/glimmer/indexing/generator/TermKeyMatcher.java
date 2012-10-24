package com.yahoo.glimmer.indexing.generator;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import com.yahoo.glimmer.indexing.generator.TermValue.Type;

class TermKeyMatcher extends BaseMatcher<TermKey> {
    private TermKey pair;
    
    public TermKeyMatcher(int index, String term) {
        pair = new TermKey(term, index, null);
    }
    public TermKeyMatcher(int index, String term, Type type, int v1) {
	pair = new TermKey(term, index, new TermValue(type, v1));
    }
    public TermKeyMatcher(int index, String term, Type type, int v1, int v2) {
	pair = new TermKey(term, index, new TermValue(type, v1, v2));
    }
    
    @Override
    public boolean matches(Object object) {
        return pair.equals(object);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(pair.toString());
    }
}