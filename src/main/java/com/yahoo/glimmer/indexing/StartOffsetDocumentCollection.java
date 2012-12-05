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
import it.unimi.di.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.fastutil.longs.AbstractLongBigList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.sux4j.util.EliasFanoLongBigList;

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
    private static final byte[] ZERO_BYTE_BUFFER = new byte[0];

    private static final long serialVersionUID = 7453027897721346888L;

    public static final String DOCUMENTS_EXTENSION = ".documents";
    public static final String START_OFFSETS_EXTENSION = ".sos";
    public static final Charset DOCUMENTS_CHARSET = Charset.forName("UTF-8");

    private static final int SUBLIST_SIZE = 10000;

    private final String name;
    private final DocumentFactory documentFactory;
    private final ArrayList<OffsetsList> offsetsLists;
    private int size;
    private transient OffsetsList currentSubList;
    private transient FileChannel channel;

    public StartOffsetDocumentCollection(String name, DocumentFactory documentFactory) {
	this.name = new File(name).getName();
	this.documentFactory = documentFactory;
	offsetsLists = new ArrayList<OffsetsList>();
	currentSubList = new OffsetsList();
    }

    @Override
    public void filename(CharSequence absolutePathToAFileInTheCollection) throws IOException {
	initFiles(new File(absolutePathToAFileInTheCollection.toString()).getParentFile());
    }

    private void initFiles(File absolutePathToCollection) throws FileNotFoundException {
	File documentsFile = new File(absolutePathToCollection, name + DOCUMENTS_EXTENSION);
	FileInputStream documentsInputStream = new FileInputStream(documentsFile);
	channel = documentsInputStream.getChannel();
    }

    @Override
    public int size() {
	return size;
    }

    @Override
    public Document document(int index) throws IOException {
	InputStream stream = stream(index);
	Reference2ObjectMap<Enum<?>, Object> metadata = getMetadata(stream);
	return documentFactory.getDocument(stream, metadata);
    }

    @Override
    public InputStream stream(int index) throws IOException {
	long startOffset = getOffset(index);
	long length;
	index++;
	if (index < size()) {
	    length = getOffset(index) - startOffset - 1;
	} else {
	    length = (int) channel.size() - startOffset - 1;
	}
	if (length > 0) {
	    ByteBuffer byteBuffer = ByteBuffer.allocate((int) length);
	    int read = channel.read(byteBuffer, startOffset);
	    if (read != length) {
		throw new IOException("Failed to read full document");
	    }
	    return new ByteArrayInputStream(byteBuffer.array());
	}
	return new ByteArrayInputStream(ZERO_BYTE_BUFFER);
    }

    @Override
    public Reference2ObjectMap<Enum<?>, Object> metadata(int index) throws IOException {
	return getMetadata(stream(index));
    }

    private Reference2ObjectMap<Enum<?>, Object> getMetadata(InputStream stream) throws IOException {
	Reference2ObjectOpenHashMap<Enum<?>, Object> metadata = new Reference2ObjectOpenHashMap<Enum<?>, Object>();

	// TODO Why is this not picked up from the factories metadata?
	metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "UTF-8");

	return metadata;
    }

    @Override
    public DocumentCollection copy() {
	return null;
    }

    @Override
    public DocumentFactory factory() {
	return documentFactory;
    }

    protected void addOffset(long offset) {
	if (currentSubList.addOffset(offset)) {
	    currentSubList.lastOffsetAdded();
	    offsetsLists.add(currentSubList);
	    currentSubList = new OffsetsList();
	}
	size++;
    }

    private long getOffset(int index) {
	if (index < 0 || index >= size()) {
	    throw new IndexOutOfBoundsException("Given index " + index + " out of range 0 to " + (size() - 1));
	}
	int offsetsIndex = index / SUBLIST_SIZE;
	int subOffsetIndex = index % SUBLIST_SIZE;

	OffsetsList offsetsList = offsetsLists.get(offsetsIndex);
	return offsetsList.getOffset(subOffsetIndex);
    }

    @Override
    public void close() throws IOException {
	super.close();
	currentSubList.lastOffsetAdded();
	offsetsLists.add(currentSubList);
	currentSubList = null;
	if (channel != null) {
	    channel.close();
	}
    }

    static class OffsetsList implements Serializable {
	private static final long serialVersionUID = 5596313596485415506L;

	private transient long[] tmpOffsets;
	private int offsetsSize = 0;
	private AbstractLongBigList offsets;

	public OffsetsList() {
	    tmpOffsets = new long[SUBLIST_SIZE];
	}

	public boolean addOffset(long offset) {
	    tmpOffsets[offsetsSize++] = offset;
	    return offsetsSize == tmpOffsets.length;
	}

	public void lastOffsetAdded() {
	    // Convert tmpOffsets to AbstractLongBigList
	    offsets = new EliasFanoLongBigList(new LongIterator() {
		int index;

		@Override
		public boolean hasNext() {
		    return index < offsetsSize;
		}

		@Override
		public Long next() {
		    return tmpOffsets[index++];
		}

		@Override
		public void remove() {
		    throw new UnsupportedOperationException();
		}

		@Override
		public long nextLong() {
		    return tmpOffsets[index++];
		}

		@Override
		public int skip(int n) {
		    int newIndex = index + n;
		    if (newIndex < offsetsSize) {
			index = newIndex;
			return n;
		    }
		    newIndex = offsetsSize - 1;
		    int skipped = newIndex - index;
		    index = newIndex;
		    return skipped;
		}
	    });
	    tmpOffsets = null;
	}

	public long getOffset(int offsetIndex) {
	    if (offsetIndex >= offsetsSize) {
		throw new IllegalArgumentException("Requested offset index " + offsetIndex + " is >= the offset list size " + offsetsSize);
	    }
	    if (offsets == null) {
		throw new IllegalStateException("lastOffsetAdded() has to be called before getOffset(i).");
	    }
	    return offsets.getLong(offsetIndex);
	}
    }
}
