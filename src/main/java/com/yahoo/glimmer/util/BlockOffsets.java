package com.yahoo.glimmer.util;

import it.unimi.dsi.fastutil.longs.LongBigList;

import java.io.IOException;
import java.io.Serializable;

public class BlockOffsets implements Serializable {
    private static final long serialVersionUID = 6997859749849192991L;

    private final LongBigList firstDocIds;
    private final LongBigList blockOffsets;
    private final long recordCount;
    private final long fileSize;

    public BlockOffsets(LongBigList firstDocIds, LongBigList blockOffsets, long recordCount, long fileSize) {
        this.firstDocIds = firstDocIds;
        this.blockOffsets = blockOffsets;
        this.recordCount = recordCount;
        this.fileSize = fileSize;
    }

    public long getBlockOffset(int index) throws IOException {
        if (index < blockOffsets.size()) {
    	return blockOffsets.getLong(index);
        } else if (index == blockOffsets.size()) {
    	return (int)fileSize - Bz2BlockIndexedDocumentCollection.FOOTER_LENGTH;
        }
        return -1;
    }
    
    public int getBlockOffsetIndex(long docId) {
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
	return (int) index;
    }
    
    public long getRecordCount() {
	return recordCount;
    }
    public long getFileSize() {
	return fileSize;
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
}