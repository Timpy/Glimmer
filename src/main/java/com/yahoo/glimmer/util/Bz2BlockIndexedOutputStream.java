package com.yahoo.glimmer.util;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;


public class Bz2BlockIndexedOutputStream extends CBZip2OutputStream {
    private final static long BLOCK_DELIMITER = 0x314159265359l;
    private final static byte BLOCK_DELIMITER_BIT_LENGTH = 6 * 8;
    
    public static interface BlockCallback {
	public void blockStart(int blockIndex, long startOffset) throws IOException;
	public void blockEnd(int blockIndex, long startOffset, long endOffset) throws IOException;
    }
    
    private BlockCallback callback;

    public static Bz2BlockIndexedOutputStream newInstance(OutputStream out, int blockSize100k) throws IOException {
	BitSequenceMonitorOutputStream monitorOutputStream = new BitSequenceMonitorOutputStream();
	monitorOutputStream.setOutputStream(out);
	monitorOutputStream.setBitSequence(BLOCK_DELIMITER, BLOCK_DELIMITER_BIT_LENGTH);
	// YES. we have to write the first 2 bytes!?
	monitorOutputStream.write('B');
	monitorOutputStream.write('Z');
	return new Bz2BlockIndexedOutputStream(monitorOutputStream, blockSize100k);
    }
    
    private Bz2BlockIndexedOutputStream(BitSequenceMonitorOutputStream monitorOutputStream, int blockSize100k) throws IOException {
	super(monitorOutputStream, blockSize100k);
	monitorOutputStream.setCallback(new MonitorCallback());
    }
    
    public void setCallback(BlockCallback callback) {
	this.callback = callback;
    }
    
    private class MonitorCallback implements BitSequenceMonitorOutputStream.Callback {
	private int blockIndex = 0;
	private long lastByteOffset;
	@Override
	public void sequenceStart(long byteOffset, int bitInByte) throws IOException {
	    if (blockIndex > 0) {
		Bz2BlockIndexedOutputStream.this.callback.blockEnd(blockIndex - 1, lastByteOffset, byteOffset);
	    }
	    Bz2BlockIndexedOutputStream.this.callback.blockStart(blockIndex, byteOffset);
	    lastByteOffset = byteOffset;
	    blockIndex++;
	}

	@Override
	public void close(long byteOffset) throws IOException {
	    if (blockIndex > -1) {
		Bz2BlockIndexedOutputStream.this.callback.blockEnd(blockIndex, lastByteOffset, byteOffset);
	    }
	}
    }
}
