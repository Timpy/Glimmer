package com.yahoo.glimmer.indexing.preprocessor;

import org.semanticweb.yars.nx.Resource;

public class SchemaDotOrgTupleFilter implements TupleFilter {
    private static final String SCHEMA_DOT_ORG_URL_PREFIX = "http://schema.org/";
    private StringBuilder sb;
    
    public SchemaDotOrgTupleFilter() {
	readResolve();
    }
    
    // On instantiation by XStream Java's field initialization or default constructor aren't used used.
    // XStream uses the same mechanism as the JDK serialization.
    private Object readResolve() {
	sb = new StringBuilder(SCHEMA_DOT_ORG_URL_PREFIX);
	return this;
    }
    
    @Override
    public boolean filter(Tuple tuple) {
	if (tuple.predicate.type == TupleElement.Type.RESOURCE && tuple.predicate.text.startsWith(SCHEMA_DOT_ORG_URL_PREFIX)) {
	    String text = tuple.predicate.text;
	    int end = text.length();
	    while (text.charAt(--end) == '/') {}
	    
	    int start = text.lastIndexOf('/', end);
	    if (start > SCHEMA_DOT_ORG_URL_PREFIX.length()) {
		// remove path
		sb.setLength(SCHEMA_DOT_ORG_URL_PREFIX.length() - 1);
		sb.append(tuple.predicate.text.substring(start, end + 1));
		tuple.predicate.text = sb.toString();
		tuple.predicate.n3 = new Resource(tuple.predicate.text).toN3();
	    }
	    return true;
	}
	return false;
    }
}
