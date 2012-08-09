package com.yahoo.glimmer.query;

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
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.index.BitStreamIndex;
import it.unimi.dsi.mg4j.index.IndexIterator;
import it.unimi.dsi.util.StringMap;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

public class Util {

    public static String getText(Document d) throws IOException {

	// should depend on the factory and the factory fields, but the
	// collection we have has only one field - > spit it all
	Reader r = (Reader) d.content(0);
	WordReader rr = d.wordReader(0).setReader(r);
	MutableString text = new MutableString();
	MutableString word = new MutableString(), nonWord = new MutableString();
	try {
	    while (rr.next(word, nonWord)) {
		text.append(word);
		text.append(nonWord);
	    }
	} catch (IOException e) {
	    // throw new RuntimeException( e );
	    e.printStackTrace();
	}
	return text.toString();
    }

    public static String decodeEntities(String query) {
	if (query == null || query.equals("")) {
	    return null;
	}

	String result = query.replaceAll("&quot;", "\"");
	result = result.replaceAll("&#39;", "'");
	result = result.replaceAll("&#92;", "\\\\");
	return result;
    }

    public static Map<String, Integer> getTermDistribution(BitStreamIndex index) throws IOException {
	Map<String, Integer> histogram = new HashMap<String, Integer>();

	StringMap<? extends CharSequence> termMap = index.termMap;

	for (CharSequence term : termMap.list()) {
	    long id = termMap.get(term);
	    IndexIterator it = index.documents(((int) id));
	    histogram.put(term.toString(), it.frequency());
	    it.dispose();
	}
	return histogram;

    }

}
