package com.yahoo.glimmer.util;

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
import java.util.Enumeration;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * For use with {@link java.io.SequenceInputStream} to read hadoop
 * dir/part-?-?????.. files as a single {@link java.io.InputStream}.
 * 
 * @author tep
 * 
 */
public class MapReducePartInputStreamEnumeration implements Enumeration<InputStream> {
    private final FileSystem fileSystem;
    private final FileStatus[] partFileStatuses;
    private int partFileStatusesIndex;
    private final CompressionCodec codecIfAny;

    public MapReducePartInputStreamEnumeration(FileSystem fileSystem, Path srcPath) throws IOException {
	this.fileSystem = fileSystem;

	CompressionCodecFactory factory = new CompressionCodecFactory(fileSystem.getConf());
	codecIfAny = factory.getCodec(srcPath);

	FileStatus srcFileStatus = fileSystem.getFileStatus(srcPath);
	if (srcFileStatus.isDir()) {
	    // returns FileStatus objects sorted by filename.
	    String partFilenamePattern = "part-?-?????";
	    if (codecIfAny != null) {
		partFilenamePattern += codecIfAny.getDefaultExtension();
	    }
	    Path partPathGlob = new Path(srcPath, partFilenamePattern);
	    partFileStatuses = fileSystem.globStatus(partPathGlob);
	} else {
	    partFileStatuses = new FileStatus[] { srcFileStatus };
	}

    }

    @Override
    public boolean hasMoreElements() {
	return partFileStatusesIndex < partFileStatuses.length;
    }

    @Override
    public InputStream nextElement() {
	FileStatus partStatus = partFileStatuses[partFileStatusesIndex++];
	try {
	    // SequenceInputStream calls InputStream.close() for us..
	    InputStream is = fileSystem.open(partStatus.getPath());
	    if (codecIfAny != null) {
		is = codecIfAny.createInputStream(is);
	    }
	    return is;
	} catch (IOException e) {
	    throw new RuntimeException("Failed to open part file " + partStatus.getPath(), e);
	}
    }

    public void reset() {
	partFileStatusesIndex = 0;
    }

    public String removeCompressionSuffixIfAny(String filename) {
	if (codecIfAny != null) {
	    return CompressionCodecFactory.removeSuffix(filename, codecIfAny.getDefaultExtension());
	} else {
	    return filename;
	}
    }
}