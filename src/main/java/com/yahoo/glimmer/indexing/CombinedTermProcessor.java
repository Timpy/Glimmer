package com.yahoo.glimmer.indexing;

import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.DowncaseTermProcessor;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.index.snowball.PorterStemmer;

/**
 * A term processor that combines other term processors.
 * 
 */

public class CombinedTermProcessor implements TermProcessor {
    private static final long serialVersionUID = 1L;

    private static CombinedTermProcessor INSTANCE = new CombinedTermProcessor();

    public final static TermProcessor[] TERM_PROCESSORS = { NonWordTermProcessor.getInstance(), DowncaseTermProcessor.getInstance(),
	    StopwordTermProcessor.getInstance(), new PorterStemmer() };

    private CombinedTermProcessor() {
    }

    public final static TermProcessor getInstance() {
	return INSTANCE;
    }

    public boolean processTerm(final MutableString term) {
	boolean process = true;
	for (TermProcessor tp : TERM_PROCESSORS) {
	    process = tp.processTerm(term);
	    if (!process)
		break;
	}
	return process;
    }

    public boolean processPrefix(final MutableString prefix) {
	return processTerm(prefix);
    }

    private Object readResolve() {
	return this;
    }

    public String toString() {
	return this.getClass().getName();
    }

    public String toSpec() {
	return toString();
    }

    public CombinedTermProcessor copy() {
	return this;
    }
}
