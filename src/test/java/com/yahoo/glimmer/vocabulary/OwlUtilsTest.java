package com.yahoo.glimmer.vocabulary;

import static org.junit.Assert.*;

import org.junit.Test;
import org.semanticweb.owlapi.model.IRI;

public class OwlUtilsTest {

    @Test(expected=IllegalArgumentException.class)
    public void nullGetLocalNameTest() {
	OwlUtils.getLocalName(null);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void emptyGetLocalNameTest() {
	OwlUtils.getLocalName(IRI.create(""));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void nonUrlGetLocalNameTest() {
	OwlUtils.getLocalName(IRI.create("non-URL"));
    }
    
    @Test
    public void hostGetLocalNameTest() {
	assertEquals("host", OwlUtils.getLocalName(IRI.create("http://host")));
	assertEquals("host", OwlUtils.getLocalName(IRI.create("http://host/")));
	assertEquals("host", OwlUtils.getLocalName(IRI.create("http://host//")));
	assertEquals("host", OwlUtils.getLocalName(IRI.create("http://host#")));
	assertEquals("host", OwlUtils.getLocalName(IRI.create("http://host/#")));
	// This maybe wrong
	assertEquals("?", OwlUtils.getLocalName(IRI.create("http://host/#?")));
    }
    
    @Test
    public void pathGetLocalNameTest() {
	assertEquals("host", OwlUtils.getLocalName(IRI.create("http://host/")));
	assertEquals("p", OwlUtils.getLocalName(IRI.create("http://host/p")));
	assertEquals("path", OwlUtils.getLocalName(IRI.create("http://host/path")));
	assertEquals("path", OwlUtils.getLocalName(IRI.create("http://host/path/")));
	
	// This maybe wrong
	assertEquals("path?p=v", OwlUtils.getLocalName(IRI.create("http://host/path?p=v")));
	assertEquals("subpath", OwlUtils.getLocalName(IRI.create("http://host/path/subpath/#")));
    }
    
    @Test
    public void fragmentGetLocalNameTest() {
	assertEquals("path", OwlUtils.getLocalName(IRI.create("http://host/path#")));
	assertEquals("f", OwlUtils.getLocalName(IRI.create("http://host/path#f")));
	assertEquals("fragment", OwlUtils.getLocalName(IRI.create("http://host/path#fragment")));
	assertEquals("fragment#", OwlUtils.getLocalName(IRI.create("http://host/path#fragment#")));
    }
}
