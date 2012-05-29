package com.yahoo.glimmer.util;

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