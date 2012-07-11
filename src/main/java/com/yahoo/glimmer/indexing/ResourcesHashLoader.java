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

import java.util.Map;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.AbstractObject2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This exists to stop multiply copies of the resources hash being created for the InputFormat and the Mapper.
 * If all the hash lookups were done in one or the other we wouldn't need this.
 *  
 * @author tep
 *
 */
public class ResourcesHashLoader {
    private static final String RESOURCES_FILENAME_KEY = "ResourcesFilename";

    private static AbstractObject2LongFunction<CharSequence> hash;

    public static void setCacheFilenameInConf(Configuration conf, String resourceHashFilename) {
	conf.set(RESOURCES_FILENAME_KEY, resourceHashFilename);
    }
    
    @SuppressWarnings("unchecked")
    public synchronized static void load(Configuration conf) {
	if (hash == null) {
	    String resourcesCacheFilename = conf.get(RESOURCES_FILENAME_KEY);
	    if (resourcesCacheFilename == null) {
		throw new RuntimeException("Configuration does not contain the resources filename");
	    }
	    try {
		Path resourcesCachePath = new Path(resourcesCacheFilename);
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream resourcesCacheInputStream = fs.open(resourcesCachePath);
		hash = (AbstractObject2LongFunction<CharSequence>) BinIO.loadObject(resourcesCacheInputStream);
		System.out.println("Loaded resource hash from " + resourcesCacheFilename + " with " + hash.size() + " entires.");
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	}
    }
    
    // For testing..
    public static void setHash(Map<CharSequence, Long> map) {
	ResourcesHashLoader.hash = new Object2LongOpenHashMap<CharSequence>(map);
    }

    public static Long lookup(String name) {
	if (hash == null) {
	    throw new RuntimeException("Hash has not been loaded.");
	}
	return hash.get(name);
    }
}
