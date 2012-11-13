package com.yahoo.glimmer.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class UtilTest {

    @Test
    public void nullTest() {
	List<String> shortNames = Util.generateShortNames(null, null);
	assertEquals(0, shortNames.size());
    }
    
    @Test
    public void emptyTest() {
	List<String> names = new ArrayList<String>();
	List<String> shortNames = Util.generateShortNames(names, null);
	assertEquals(0, shortNames.size());
    }
    
    @Test
    public void identTest() {
	List<String> names = new ArrayList<String>();
	names.add("b");
	names.add("a");
	names.add("c");
	List<String> shortNames = Util.generateShortNames(names, null);
	assertEquals(names.size(), shortNames.size());
	assertEquals("b", shortNames.get(0));
	assertEquals("a", shortNames.get(1));
	assertEquals("c", shortNames.get(2));
    }
    
    @Test
    public void simpleTest() {
	List<String> names = new ArrayList<String>();
	names.add("http://schema.org/name");
	names.add("http://schema.org/url");
	names.add("http://schema.org/tracks");
	names.add("http://schema.org/duration");
	List<String> shortNames = Util.generateShortNames(names, null);
	assertEquals(names.size(), shortNames.size());
	assertEquals("name", shortNames.get(0));
	assertEquals("url", shortNames.get(1));
	assertEquals("tracks", shortNames.get(2));
	assertEquals("duration", shortNames.get(3));
    }
    
    @Test
    public void test() {
	List<String> names = new ArrayList<String>();
	names.add("http://schema.org/name");
	names.add("http://schema.org/url");
	names.add("http://domain/topicA");
	names.add("http://domain/topicB");
	names.add("http://domain/topic/A");
	names.add("http://domain/topic/B");
	names.add("http://domain/event/A");
	names.add("http://domain/event/B");
	names.add("http://schema.org/excludeMe");
	names.add("http://domain/excludeMe");
	names.add("http://domain/name");
	names.add("http://domain/url");
	List<String> shortNames = Util.generateShortNames(names, Collections.singleton("excludeMe"));
	assertEquals(names.size(), shortNames.size());
	assertEquals("name", shortNames.get(0));
	assertEquals("url", shortNames.get(1));
	assertEquals("topicA", shortNames.get(2));
	assertEquals("topicB", shortNames.get(3));
	assertEquals("A", shortNames.get(4));
	assertEquals("B", shortNames.get(5));
	assertEquals("event_A", shortNames.get(6));
	assertEquals("event_B", shortNames.get(7));
	assertEquals("org_excludeMe", shortNames.get(8));
	assertEquals("domain_excludeMe", shortNames.get(9));
	assertEquals("domain_name", shortNames.get(10));
	assertEquals("domain_url", shortNames.get(11));
    }
    
    @Test
    public void caseTest() {
	List<String> names = new ArrayList<String>();
	names.add("http://schema.org/articleBody");
	names.add("http://schema.org/ArticleBody");
	List<String> shortNames = Util.generateShortNames(names, null);
	assertEquals(names.size(), shortNames.size());
	assertEquals("articleBody", shortNames.get(0));
	assertEquals("ArticleBody", shortNames.get(1));
    }
}
