package com.yahoo.glimmer.util;

import it.unimi.dsi.fastutil.longs.LongBigList;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.codec.digest.DigestUtils;

public class BlockOffsets implements Serializable {
    private static final long serialVersionUID = 6997859749849192991L;

    private final LongBigList firstDocIds;
    private final LongBigList blockOffsets;
    private final long recordCount;
    private final long fileSize;
    private final byte[] md5Digest;

    public BlockOffsets(LongBigList firstDocIds, LongBigList blockOffsets, long recordCount, long fileSize, byte[] md5Digest) {
	this.firstDocIds = firstDocIds;
	this.blockOffsets = blockOffsets;
	this.recordCount = recordCount;
	this.fileSize = fileSize;
	this.md5Digest = Arrays.copyOf(md5Digest, md5Digest.length);
    }

    public long getBlockOffset(long index) throws IOException {
	if (index < blockOffsets.size()) {
	    return blockOffsets.getLong(index);
	} else if (index == blockOffsets.size()) {
	    return fileSize - BlockCompressedDocumentCollection.FOOTER_LENGTH;
	}
	return -1;
    }

    public long getBlockOffsetIndex(long docId) {
	long index = longBigListBinarySearch(firstDocIds, docId);
	if (index < 0) {
	    index = -index - 2;
	}
	if (index > 0) {
	    // If a record spans multiple blocks the value at index in
	    // firstDocIds will be repeated.
	    // Find the first element equal to the value at firstDocIds[index].
	    long value = firstDocIds.getLong(index);
	    index--;
	    while (index >= 0 && firstDocIds.getLong(index) == value) {
		index--;
	    }
	    index++;
	}
	return index;
    }

    public long getRecordCount() {
	return recordCount;
    }

    public long getFileSize() {
	return fileSize;
    }

    public long getBlockCount() {
	return blockOffsets.size64();
    }
    
    public byte[] getMd5Digest() {
	return md5Digest;
    }

    // Surprisingly there doesn't seem to be a binary search method on
    // LongBigLists in fastutil..
    private static long longBigListBinarySearch(final LongBigList list, final long key) {
	long midVal;
	long from = 0;
	long to = list.size64() - 1;
	while (from <= to) {
	    final long mid = (from + to) >>> 1;
	    midVal = list.getLong(mid);
	    if (midVal < key) {
		from = mid + 1;
	    } else if (midVal > key) {
		to = mid - 1;
	    } else {
		return mid;
	    }
	}
	return -(from + 1);
    }
    
    public void printTo(PrintStream ps) {
	ps.println("Doc count:" + recordCount);
	ps.println("Bz2 file size:" + fileSize);
	if (md5Digest != null) {
	    ps.println("Bz2 MD5 hash:" + DigestUtils.md5Hex(md5Digest));
	}
	ps.println("FirstDoc\tBlockStart");
	for (long i = 0 ; i < firstDocIds.size64() ; i++) {
	    ps.printf("%d\t%d\n", firstDocIds.get(i), blockOffsets.get(i));
	}
    }
}