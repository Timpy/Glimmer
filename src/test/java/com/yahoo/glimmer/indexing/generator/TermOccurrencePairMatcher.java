package com.yahoo.glimmer.indexing.generator;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

class TermOccurrencePairMatcher extends BaseMatcher<TermOccurrencePair> {
    private TermOccurrencePair pair;
    
    public TermOccurrencePairMatcher(int index, String term) {
        pair = new TermOccurrencePair(term, index, null);
    }
    public TermOccurrencePairMatcher(int index, String term, Integer document, Integer position) {
	pair = new TermOccurrencePair(term, index, new Occurrence(document, position));
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