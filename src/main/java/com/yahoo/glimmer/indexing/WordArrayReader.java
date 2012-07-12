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

import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * A WordReader that reads words from a List of tokens
 * 
 * @author pmika
 * 
 */
public class WordArrayReader implements WordReader {
    private static final long serialVersionUID = -5084427095759689812L;

    private static final MutableString EMPTY_NONWORD = new MutableString("");

    private List<String> tokens;
    int pos = 0;

    public WordArrayReader(List<String> tokens) {
	this.tokens = tokens;

    }

    public WordReader copy() {
	return new WordArrayReader(tokens);
    }

    public boolean next(MutableString word, MutableString nonWord) throws IOException {
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
