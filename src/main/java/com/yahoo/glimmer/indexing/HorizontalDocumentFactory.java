package com.yahoo.glimmer.indexing;

import org.apache.hadoop.conf.Configuration;

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


public class HorizontalDocumentFactory extends RDFDocumentFactory {

    /**
     * Returns a copy of this document factory. A new parser is allocated for
     * the copy.
     */
//    public HorizontalDocumentFactory copy() {
//	return new HorizontalDocumentFactory(defaultMetadata);
//    }
//
//    public HorizontalDocumentFactory(final Properties properties) throws ConfigurationException {
//	super(properties);
//    }
//
//    public HorizontalDocumentFactory(final Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
//	super(defaultMetadata);
//    }
//
//    public HorizontalDocumentFactory(final String[] property) throws ConfigurationException {
//	super(property);
//    }
//
//    public HorizontalDocumentFactory() {
//	super();
//    }
    
    public static void setupConf(Configuration conf, boolean withContext) {
	setupConf(conf, IndexType.HORIZONTAL, withContext, "token", "property", "context", "uri");
    }

    @Override
    public RDFDocument getDocument() {
	return new HorizontalDocument(this);
    }
}
