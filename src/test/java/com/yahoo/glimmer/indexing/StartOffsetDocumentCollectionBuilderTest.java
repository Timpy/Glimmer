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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.di.mg4j.document.Document;
import it.unimi.di.mg4j.document.DocumentFactory;
import it.unimi.di.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.di.mg4j.io.IOFactory;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.lang.MutableString;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.nio.CharBuffer;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

public class StartOffsetDocumentCollectionBuilderTest {
    private Mockery context;
    private DocumentFactory documentFactory;
    private IOFactory ioFactory;
    private StartOffsetDocumentCollectionBuilder builder;
    private ByteArrayOutputStream documentsOutputStream;
    private InputStream offsetsInStream;
    private OutputStream offsetsOutputStream;
    
    @Before
    public void before() throws IOException {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);
	
	
	Reference2ObjectMap<Enum<?>, Object> properties = new Reference2ObjectOpenHashMap<Enum<?>, Object>();
	properties.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "UTF-8");
	documentFactory = new IdentityDocumentFactory(properties);
	
	ioFactory = context.mock(IOFactory.class);
	
	builder = new StartOffsetDocumentCollectionBuilder("BaseName", documentFactory, ioFactory);
	
	documentsOutputStream = new ByteArrayOutputStream(4096);
	offsetsInStream = new PipedInputStream(4096);
	offsetsOutputStream = new PipedOutputStream((PipedInputStream)offsetsInStream);
    }
    
    @Test
    public void test() throws IOException, ClassNotFoundException {
	context.checking(new Expectations() {{
	    one(ioFactory).getOutputStream("BaseNameSuffix.documents");
	    will(returnValue(documentsOutputStream));
	    one(ioFactory).getOutputStream("BaseNameSuffix.sos");
	    will(returnValue(offsetsOutputStream));
	}});
	
	builder.open("Suffix");
	MutableString nonWord = new MutableString(" ");
	
	builder.startDocument(null, null);
	builder.add(new MutableString("A"), nonWord);
	builder.add(new MutableString("Document"), nonWord);
	builder.endDocument();
	builder.startDocument(null, null);
	builder.add(new MutableString("Another"), nonWord);
	builder.add(new MutableString("document"), nonWord);
	builder.endDocument();
	builder.startDocument(null, null);
	builder.add(new MutableString("The"), nonWord);
	builder.add(new MutableString("third"), nonWord);
	builder.add(new MutableString("document"), nonWord);
	builder.endDocument();
	builder.startDocument(null, null);
	builder.add(new MutableString("Something completely different."), nonWord);
	builder.endDocument();
	
	builder.close();
	
	context.assertIsSatisfied();
	
	assertEquals("A Document Another document The third document Something completely different. ", new String(documentsOutputStream.toByteArray(), 0, documentsOutputStream.size(), "UTF-8"));
	
	// Write the document collection to a temp file for loading into the collection once it's been de-serialized.
	File tempDocumentsFile = File.createTempFile(StartOffsetDocumentCollectionBuilderTest.class.getSimpleName(), Long.toString(System.currentTimeMillis()));
	tempDocumentsFile.deleteOnExit();
	FileOutputStream tempOutputStream = new FileOutputStream(tempDocumentsFile);
	tempOutputStream.write(documentsOutputStream.toByteArray(), 0, documentsOutputStream.size());
	
	Object object = BinIO.loadObject(offsetsInStream);
	assertTrue(object instanceof StartOffsetDocumentCollection);
	StartOffsetDocumentCollection collection = (StartOffsetDocumentCollection)object;
	collection.filename(tempDocumentsFile.getAbsolutePath());
	
	CharBuffer contentBuffer = CharBuffer.allocate(4096);
	Document document = collection.document(0);
	Object content = document.content(0);
	assertTrue(content instanceof Reader);
	Reader contentReader = (Reader) content;
	assertEquals(11, contentReader.read(contentBuffer));
	contentBuffer.flip();
	assertEquals("A Document ", contentBuffer.toString());
	
	contentBuffer.clear();
	document = collection.document(1);
	content = document.content(0);
	assertTrue(content instanceof Reader);
	contentReader = (Reader) content;
	assertEquals(17, contentReader.read(contentBuffer));
	contentBuffer.flip();
	assertEquals("Another document ", contentBuffer.toString());
	
	contentBuffer.clear();
	document = collection.document(2);
	content = document.content(0);
	assertTrue(content instanceof Reader);
	contentReader = (Reader) content;
	assertEquals(19, contentReader.read(contentBuffer));
	contentBuffer.flip();
	assertEquals("The third document ", contentBuffer.toString());
	
	contentBuffer.clear();
	document = collection.document(3);
	content = document.content(0);
	assertTrue(content instanceof Reader);
	contentReader = (Reader) content;
	assertEquals(32, contentReader.read(contentBuffer));
	contentBuffer.flip();
	assertEquals("Something completely different. ", contentBuffer.toString());
	
	assertEquals(4, collection.size());
    }
}
