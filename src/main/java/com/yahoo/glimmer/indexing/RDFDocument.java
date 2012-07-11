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

import it.unimi.dsi.io.WordReader;

import java.io.IOException;
import java.io.InputStream;

import com.yahoo.glimmer.indexing.RDFDocumentFactory.IndexType;

public abstract class RDFDocument {
    // TODO RDFDocumnet should implement Hadoop's Writable but as we have a ref to an InputStream for lazy parsing this isn't possible.
    
    public static final String NULL_URL = "NULL_URL";
    
    protected final RDFDocumentFactory factory;
    /** Whether we already parsed the document. */
    protected boolean parsed;
    /** The cached raw content. */
    protected InputStream rawContent;
    protected String url = NULL_URL;

    public abstract WordReader content(final int field) throws IOException;
    public abstract IndexType getIndexType();
    protected abstract void ensureParsed() throws IOException;
	

    public RDFDocument(RDFDocumentFactory factory) {
	this.factory = factory;
    }

    public void setContent(InputStream rawContent) {
	this.rawContent = rawContent;
	parsed = false;
    }

    public CharSequence uri() {
	try {
	    ensureParsed();
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
	return url;
    }
}