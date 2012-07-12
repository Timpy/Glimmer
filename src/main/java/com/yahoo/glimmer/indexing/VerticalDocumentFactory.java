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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * The fields to be indexed are read from the file INDEXED_PROPERTIES_FILENAME,
 * which may contain either short names of the form prefix_localname, e.g.
 * fn_vcard or full URIs. In case of short names, the URI of the predicate
 * should be possible to convert to a shortname using the namespaces table. In
 * case of URIs, the URI in the file should be convertible using the namespaces
 * table.
 * 
 * @author pmika@yahoo-inc.com
 * 
 */
public class VerticalDocumentFactory extends RDFDocumentFactory {
    /**
     * Returns a copy of this document factory. A new parser is allocated for
     * the copy.
     */
//    public VerticalDocumentFactory copy() {
//	VerticalDocumentFactory factory = new VerticalDocumentFactory(defaultMetadata);
//	return factory;
//    }
//
//    public VerticalDocumentFactory(final Properties properties) throws ConfigurationException {
//	super(properties);
//    }
//
//    public VerticalDocumentFactory(final Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
//	super(defaultMetadata);
//    }
//
//    public VerticalDocumentFactory(final String[] property) throws ConfigurationException {
//	super(property);
//    }

    public static void setupConf(Configuration conf, boolean withContexts, String resourcesHash, String predicates) throws IOException {
	FileSystem fs = FileSystem.get(conf);
	FSDataInputStream predicatesInputStream = fs.open(new Path(predicates));
	
	ArrayList<String> arrayList = new ArrayList<String>();

	try {
	    BufferedReader reader = new BufferedReader(new InputStreamReader(predicatesInputStream));
	    String nextLine = "";

	    while ((nextLine = reader.readLine()) != null) {
		nextLine = nextLine.trim();
		if (!nextLine.isEmpty()) {
		    // Take the first column
		    String predicate = nextLine.split("\\s+")[0];
		    // if no match, returns the whole string

		    // Only include if it's in the namespaces table and not
		    // blacklisted
		    if (predicate != null && !isOnPredicateBlacklist(predicate)) {
			System.out.println("Going to index predicate:" + predicate);
			arrayList.add(predicate);
		    }
		}
	    }
	    reader.close();

	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
	System.out.println("Loaded " + arrayList.size() + " fields.");
	setupConf(conf, IndexType.VERTICAL, withContexts, resourcesHash, arrayList.toArray(new String[0]));
    }

    private void readObject(final ObjectInputStream s) throws IOException, ClassNotFoundException {
	s.defaultReadObject();
    }

    @Override
    public RDFDocument getDocument() {
	return new VerticalDocument(this);
    }
}
