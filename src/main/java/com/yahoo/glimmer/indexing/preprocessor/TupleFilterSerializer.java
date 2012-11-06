package com.yahoo.glimmer.indexing.preprocessor;

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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.StreamException;

public class TupleFilterSerializer {
    public static final String FILTER_XML_SYMBOLIC_NAME = "FilterXml";
    
    public static TupleFilter deserialize(Configuration conf) throws IOException {
	FileSystem fs = FileSystem.get(conf);
	@SuppressWarnings("deprecation") // TODO How to do this in hadoop 0.23??
	URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
	if (cacheFiles != null) {
	    for (URI cacheFile : cacheFiles) {
		if (FILTER_XML_SYMBOLIC_NAME.equals(cacheFile.getFragment())) {
		    Path filterPath = new Path(cacheFile);

		    FSDataInputStream filterIs = fs.open(filterPath);
		    return deserialize(filterIs);
		}
	    }
	}
	return null;
    }

    public static TupleFilter deserialize(InputStream is) throws IOException {
	XStream xStream = new XStream();
	try {
	    Object object = xStream.fromXML(is);
	    if (object instanceof TupleFilter) {
		return (TupleFilter) object;
	    }
	} catch (StreamException e) {
	    if (e.getCause() instanceof IOException) {
		throw (IOException) e.getCause();
	    }
	    e.printStackTrace();
	}
	throw new IllegalArgumentException("Input did not de-serialize to an instance of a TupleFilter.");
    }
}
