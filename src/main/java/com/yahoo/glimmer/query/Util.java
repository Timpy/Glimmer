package com.yahoo.glimmer.query;

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
