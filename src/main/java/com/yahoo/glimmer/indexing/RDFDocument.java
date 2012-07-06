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

import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.mg4j.document.AbstractDocument;

import java.io.IOException;
import java.io.InputStream;

public abstract class RDFDocument extends AbstractDocument {
    public static final String NULL_URL = "NULL_URL";

    protected final RDFDocumentFactory factory;
    private Reference2ObjectMap<Enum<?>, Object> docMetadata;
    /** Whether we already parsed the document. */
    protected boolean parsed;
    /** The cached raw content. */
    protected InputStream rawContent;
    protected String url = NULL_URL;

    protected abstract void ensureParsed() throws IOException;

    public RDFDocument(RDFDocumentFactory factory, Reference2ObjectMap<Enum<?>, Object> docMetadata) {
	this.factory = factory;
	if (docMetadata == null) {
	    this.docMetadata = new Reference2ObjectArrayMap<Enum<?>, Object>();
	} else {
	    this.docMetadata = docMetadata;
	}
    }

    public void setContent(InputStream rawContent) {
	this.rawContent = rawContent;
	parsed = false;
    }

    public Object resolveNotNull(Enum<?> key) {
	return factory.resolveNotNull(key, docMetadata);
    }

    public Object resolve(Enum<?> key) {
	return factory.resolve(key, docMetadata);
    }

    public CharSequence uri() {
	try {
	    ensureParsed();
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
	return url;
    }

    // All fields use the same reader
    @Override
    public WordReader wordReader(final int field) {
	factory.ensureFieldIndex(field);
	return factory.wordReader;
    }
}