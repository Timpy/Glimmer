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

import it.unimi.di.mg4j.document.AbstractDocumentCollection;
import it.unimi.di.mg4j.document.Document;
import it.unimi.di.mg4j.document.DocumentCollection;
import it.unimi.di.mg4j.document.DocumentFactory;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMaps;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class StartOffsetDocumentCollection extends AbstractDocumentCollection implements Serializable {
    private static final long serialVersionUID = 7453027897721346888L;
    
    public static final String DOCUMENTS_EXTENSION = ".documents";
    public static final String START_OFFSETS_EXTENSION = ".sos";
    public static final Charset DOCUMENTS_CHARSET = Charset.forName("UTF-8");
    
    private static final int DEFAULT_SUBLIST_SIZE = 2 ^ 16;
    private int subListSize = DEFAULT_SUBLIST_SIZE;

    private final ArrayList<OffsetsList> offsetsLists = new ArrayList<OffsetsList>();
    private DocumentFactory factory;
    private int size;
    private transient FileChannel channel;

    public StartOffsetDocumentCollection(DocumentFactory factory) {
	this.factory = factory;
    }

    @Override
    public void filename(CharSequence filename) throws IOException {
	initFiles(new File(filename.toString()));
    }
    
    private void initFiles(File documentsFile) throws FileNotFoundException {
	FileInputStream documentsInputStream = new FileInputStream(documentsFile);
	channel = documentsInputStream.getChannel();
    }

    @Override
    public int size() {
	return size;
    }

    @Override
    public Document document(int index) throws IOException {
	return factory.getDocument(stream(index), metadata(index));
    }

    @Override
    public InputStream stream(int index) throws IOException {
	int startOffset = getOffset(index);
	int length;
	index++;
	if (index < size()) {
	    length = getOffset(index) - startOffset;
	} else {
	    length = (int) channel.size() - startOffset;
	}

	ByteBuffer byteBuffer = ByteBuffer.allocate(length);
	int read = channel.read(byteBuffer, startOffset);
	if (read != length) {
	    throw new IOException("Failed to read full document");
	}
	return new ByteArrayInputStream(byteBuffer.array());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Reference2ObjectMap<Enum<?>, Object> metadata(int index) throws IOException {
	return Reference2ObjectMaps.EMPTY_MAP;
    }

    @Override
    public DocumentCollection copy() {
	return null;
    }

    @Override
    public DocumentFactory factory() {
	return factory;
    }

    private int getOffset(int index) {
	if (index < 0 || index >= size()) {
	    throw new IndexOutOfBoundsException("Given index " + index + " out of range 0 to " + (size() - 1));
	}
	int offsetsIndex = index / subListSize;
	int subOffsetIndex = index % subListSize;

	OffsetsList offsetsList = offsetsLists.get(offsetsIndex);
	return offsetsList.offsets[subOffsetIndex];
    }
    
    protected void addOffset(int offset) {
	int offsetsIndex = size / subListSize;
	int subOffsetIndex = size % subListSize;
	
	OffsetsList subList;
	if (subOffsetIndex == 0) {
	    subList = new OffsetsList(subListSize);
	    offsetsLists.add(subList);
	} else {
	    subList = offsetsLists.get(offsetsIndex);
	}
	subList.offsets[subOffsetIndex] = offset;
	size++;
    }

    private static class OffsetsList implements Serializable {
	private static final long serialVersionUID = 5596313596485415506L;
	
	final int[] offsets;

	public OffsetsList(int length) {
	    offsets = new int[length];
	}
    }
}
