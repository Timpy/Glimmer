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
import static org.junit.Assert.assertNull;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
    private String basename;
    private File sysTmp;

    @Before
    public void before() throws IOException {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);

	Reference2ObjectMap<Enum<?>, Object> properties = new Reference2ObjectOpenHashMap<Enum<?>, Object>();
	properties.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "UTF-8");
	documentFactory = new IdentityDocumentFactory(properties);

	ioFactory = context.mock(IOFactory.class);

	sysTmp = new File(System.getProperty("java.io.tmpdir"));
	basename = new File(sysTmp, StartOffsetDocumentCollectionBuilderTest.class.getSimpleName() + (System.currentTimeMillis() % 100000)).toString();
	builder = new StartOffsetDocumentCollectionBuilder(basename, documentFactory, ioFactory);
    }

    @Test
    public void test() throws IOException, ClassNotFoundException {
	File tempDocumentsFile = new File(basename + "-suffix" + StartOffsetDocumentCollection.DOCUMENTS_EXTENSION);
	tempDocumentsFile.deleteOnExit();
	final FileOutputStream documentsOutputStream = new FileOutputStream(tempDocumentsFile);

	File tempOffsetsFile = new File(basename + "-suffix" + StartOffsetDocumentCollection.START_OFFSETS_EXTENSION);
	tempOffsetsFile.deleteOnExit();
	final FileOutputStream offsetsOutputStream = new FileOutputStream(tempOffsetsFile);

	context.checking(new Expectations() {
	    {
		one(ioFactory).getOutputStream(basename + "-suffix.documents");
		will(returnValue(documentsOutputStream));
		one(ioFactory).getOutputStream(basename + "-suffix.sos");
		will(returnValue(offsetsOutputStream));
	    }
	});

	builder.open("-suffix");
	MutableString nonWord = new MutableString(" ");

	// Title and URI are ignored.
	builder.startDocument("Title", "URI");
	builder.startTextField();
	builder.add(new MutableString("A"), nonWord);
	builder.add(new MutableString("Document"), nonWord);
	builder.endTextField();
	builder.endDocument();

	// null doc should write nothing.
	builder.startDocument("", "");
	builder.endDocument();

	builder.startDocument("", "");
	builder.startTextField();
	builder.add(new MutableString("Another"), nonWord);
	builder.add(new MutableString("document"), nonWord);
	builder.endTextField();
	builder.endDocument();

	// empty doc should write a newline.
	builder.startDocument("", "");
	builder.endDocument();
	builder.endTextField();
	builder.endDocument();

	builder.startDocument("Doc3", "URI3");
	builder.startTextField();
	builder.add(new MutableString("The"), nonWord);
	builder.add(new MutableString("third"), nonWord);
	builder.add(new MutableString("document"), nonWord);
	builder.endTextField();
	builder.endDocument();

	builder.startDocument("Doc4", "URI4");
	builder.startTextField();
	builder.add(new MutableString("Something completely different."), nonWord);
	builder.endTextField();
	builder.endDocument();

	builder.close();

	context.assertIsSatisfied();

	documentsOutputStream.flush();
	offsetsOutputStream.flush();

	// Check contents of .documents file.
	FileInputStream documentsInputStream = new FileInputStream(tempDocumentsFile);
	byte[] buffer = new byte[4096];
	int byteCount = documentsInputStream.read(buffer);
	assertEquals("A Document \n" + "Another document \n" + "\n" + "The third document \n" + "Something completely different. \n", new String(buffer, 0,
		byteCount, "UTF-8"));

	FileInputStream offsetsInputStream = new FileInputStream(tempOffsetsFile);

	Object object = BinIO.loadObject(offsetsInputStream);
	assertTrue(object instanceof StartOffsetDocumentCollection);
	StartOffsetDocumentCollection collection = (StartOffsetDocumentCollection) object;
	collection.filename(tempOffsetsFile.getAbsolutePath());

	CharBuffer contentBuffer = CharBuffer.allocate(4096);
	Document document = collection.document(0);
	assertNull(document.title());
	Object content = document.content(0);
	assertTrue(content instanceof Reader);
	Reader contentReader = (Reader) content;
	assertEquals(11, contentReader.read(contentBuffer));
	contentBuffer.flip();
	assertEquals("A Document ", contentBuffer.toString());

	// null doc
	contentBuffer.clear();
	document = collection.document(1);
	assertNull(document.title());
	content = document.content(0);
	assertTrue(content instanceof Reader);
	assertEquals(-1, contentReader.read(contentBuffer));

	contentBuffer.clear();
	document = collection.document(2);
	assertNull(document.title());
	content = document.content(0);
	assertTrue(content instanceof Reader);
	contentReader = (Reader) content;
	assertEquals(17, contentReader.read(contentBuffer));
	contentBuffer.flip();
	assertEquals("Another document ", contentBuffer.toString());

	// empty doc
	contentBuffer.clear();
	document = collection.document(3);
	assertNull(document.title());
	content = document.content(0);
	assertTrue(content instanceof Reader);
	assertEquals(-1, contentReader.read(contentBuffer));

	contentBuffer.clear();
	document = collection.document(4);
	assertNull(document.title());
	content = document.content(0);
	assertTrue(content instanceof Reader);
	contentReader = (Reader) content;
	assertEquals(19, contentReader.read(contentBuffer));
	contentBuffer.flip();
	assertEquals("The third document ", contentBuffer.toString());

	contentBuffer.clear();
	document = collection.document(5);
	assertNull(document.title());
	content = document.content(0);
	assertTrue(content instanceof Reader);
	contentReader = (Reader) content;
	assertEquals(32, contentReader.read(contentBuffer));
	contentBuffer.flip();
	assertEquals("Something completely different. ", contentBuffer.toString());

	assertEquals(6, collection.size());
    }
}
