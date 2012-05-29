package com.yahoo.glimmer.indexing;

import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/** A WordReader that reads words from a List of tokens
 * 
 * @author pmika
 *
 */
 @SuppressWarnings("serial")
 public class WordArrayReader implements WordReader {

		private static final MutableString EMPTY_NONWORD = new MutableString("");
		
		private List<String> tokens;
		int pos = 0;

		public WordArrayReader(List<String> tokens) {
			this.tokens = tokens;
			
		}
		public WordReader copy() {
			return new WordArrayReader(tokens);
		}

		public boolean next(MutableString word, MutableString nonWord)
				throws IOException {
			if (pos < tokens.size()) {
				word.replace(tokens.get(pos++));
				nonWord.replace(EMPTY_NONWORD);
				return true;
			} else {
				return false;
			} 
		}

		public WordReader setReader(Reader reader) {
			throw new RuntimeException("Not supported: setReader()");
		}
		
	}

