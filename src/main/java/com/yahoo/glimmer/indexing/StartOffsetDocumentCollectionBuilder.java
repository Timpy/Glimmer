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

import it.unimi.di.mg4j.document.DocumentCollectionBuilder;
import it.unimi.di.mg4j.document.DocumentFactory;
import it.unimi.di.mg4j.io.IOFactory;
import it.unimi.di.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.OutputStream;

public class StartOffsetDocumentCollectionBuilder implements DocumentCollectionBuilder {
    private final String basename;
    private final DocumentFactory factory;
    private final IOFactory ioFactory;
    
    private StartOffsetDocumentCollection collection;
    private ByteCountOutputStream documentsOutputStream;
    private OutputStream offsetsOutputStream;
    
    public StartOffsetDocumentCollectionBuilder(String basename, DocumentFactory factory, IOFactory ioFactory) {
	this.basename = basename;
	this.factory = factory;
	this.ioFactory = ioFactory;
    }
    
    @Override
    public String basename() {
	return basename;
    }

    @Override
    public void open(CharSequence suffix) throws IOException {
	collection = new StartOffsetDocumentCollection(factory);
	
	String basenameSuffixed = basename + suffix;
	String documentsFilename = basenameSuffixed + StartOffsetDocumentCollection.DOCUMENTS_EXTENSION;
	documentsOutputStream = new ByteCountOutputStream(ioFactory.getOutputStream(documentsFilename));
	
	String offsetsFilename = basenameSuffixed + StartOffsetDocumentCollection.START_OFFSETS_EXTENSION;
	offsetsOutputStream = ioFactory.getOutputStream(offsetsFilename);
    }

    @Override
    public void startDocument(CharSequence title, CharSequence uri) throws IOException {
	collection.addOffset(documentsOutputStream.getByteCount());
    }

    @Override
    public void startTextField() {
    }
    
    @Override
    public void add(MutableString word, MutableString nonWord) throws IOException {
	word.writeUTF8(documentsOutputStream);
	nonWord.writeUTF8(documentsOutputStream);
    }
    
    @Override
    public void endTextField() throws IOException {
    }
    @Override
    public void endDocument() throws IOException {
    }

    @Override
    public void nonTextField(Object o) throws IOException {
	throw new UnsupportedOperationException();
    }

    @Override
    public void virtualField(ObjectList<VirtualDocumentFragment> fragments) throws IOException {
	throw new UnsupportedOperationException();
    }
    
    @Override
    public void close() throws IOException {
	documentsOutputStream.flush();
	documentsOutputStream.close();
	collection.close();
	BinIO.storeObject(collection, offsetsOutputStream);
    }
    
    private static class ByteCountOutputStream extends OutputStream {
	private final OutputStream wrappedOutputStream;
	private int byteCount;
	
	public ByteCountOutputStream(OutputStream outputStream) {
	    wrappedOutputStream = outputStream;
	}
	
	public int getByteCount() {
	    return byteCount;
	}
	
	@Override
	public void write(int b) throws IOException {
	    wrappedOutputStream.write(b);
	    byteCount++;
	}
	
	@Override
	public void flush() throws IOException {
	    wrappedOutputStream.flush();
	}

	@Override
	public void close() throws IOException {
	    wrappedOutputStream.close();
	}
    }
}
