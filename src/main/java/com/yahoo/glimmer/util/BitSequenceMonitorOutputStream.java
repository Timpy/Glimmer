package com.yahoo.glimmer.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Detects a given bit sequence in the output stream and invokes
 * the given callback with the byte and bit offsets at which the
 * but sequence started.
 * 
 * @author tep
 */
public class BitSequenceMonitorOutputStream extends OutputStream {
    private OutputStream wrappedOutputStream;
    private Callback callback;
    private long bitSequence;
    private int bitSequenceLengthInBytes;
    private long bitSequenceMask;
    
    private long byteCount;
    private long bitPipe;

    public BitSequenceMonitorOutputStream() {
    }
    public void setOutputStream(OutputStream outputStream) {
	wrappedOutputStream = outputStream;
    }
    public void setCallback(Callback callback) {
	this.callback = callback;
    }
    public Callback getCallback() {
	return callback;
    }
    public void setBitSequence(long bitSequence, byte lengthInBits) {
	this.bitSequence = bitSequence;
	
	bitSequenceMask = (1l << lengthInBits) - 1;
	
	bitSequenceLengthInBytes = lengthInBits / 8;
	if (lengthInBits % 8 > 0) {
	    bitSequenceLengthInBytes++;
	}
    }

    @Override
    public void write(int b) throws IOException {
	wrappedOutputStream.write(b);
	byteCount++;

	bitPipe <<= 8;
	bitPipe |= (long) b & 0xFF;

	if ((bitPipe & bitSequenceMask) == bitSequence) {
	    // the block delimiter is byte aligned.
	    callback.sequenceStart(byteCount - bitSequenceLengthInBytes, 0);
	} else {
	    long tmp = bitPipe;
	    for (int i = 1; i < 8; i++) {
		tmp >>>= 1;
		if ((tmp & bitSequenceMask) == bitSequence) {
		    callback.sequenceStart(byteCount - bitSequenceLengthInBytes - 1, 8 - i);
		    break;
		}
	    }
	}
    }

    @Override
    public void flush() throws IOException {
	wrappedOutputStream.flush();
    }

    @Override
    public void close() throws IOException {
	wrappedOutputStream.close();
	// TODO Do something with trailing CRC bytes..
	callback.close(byteCount);
    }

    public interface Callback {
	public void sequenceStart(long byteOffset, int bitInByte) throws IOException;
	public void close(long byteOffset) throws IOException;
    }
}