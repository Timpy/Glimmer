package com.yahoo.glimmer.indexing;

import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.TermProcessor;

/** A term processor that excludes words with non-alphanumeric characters. */

public class NonWordTermProcessor implements TermProcessor {
	private static final long serialVersionUID = 1L;

	private final static NonWordTermProcessor INSTANCE = new NonWordTermProcessor();

	public final static TermProcessor getInstance() {
		return INSTANCE;
	}

	private NonWordTermProcessor() {
	}

	public boolean processTerm(final MutableString term) {
		if (term == null)
			return false;
		if (!term.toString().matches("[a-zA-Z0-9]+"))
			return false;
		return true;
	}

	public boolean processPrefix(final MutableString prefix) {
		return processTerm(prefix);
	}

	private Object readResolve() {
		return INSTANCE;
	}

	public String toString() {
		return this.getClass().getName();
	}

	public NonWordTermProcessor copy() {
		return this;
	}
}
