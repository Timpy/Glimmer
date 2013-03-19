package com.yahoo.glimmer.util;

import it.unimi.di.big.mg4j.document.AbstractDocumentCollection;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.io.ByteBufferInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.FileChannel.MapMode;

import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.log4j.Logger;

public class Bz2BlockIndexedDocumentCollection extends AbstractDocumentCollection implements Serializable {
    private final static Logger LOGGER = Logger.getLogger(Bz2BlockIndexedDocumentCollection.class);
    private static final long serialVersionUID = -7943857364950329249L;

    private static final char RECORD_DELIMITER = '\n';
    private static final char FIELD_DELIMITER = '\t';

    private static final byte[] ZERO_BYTE_BUFFER = new byte[0];

    // We use the smallest block size during compression to improve retrieval
    // time(100KB).
    private static final int BZ2_BLOCK_SIZE = 100000;
    public static final String BZ2_EXTENSION = ".bz2";
    public static final String BLOCK_OFFSETS_EXTENSION = ".blockOffsets";

    // TODO.  What should this be?  Getting end of stream errors!?
    static final int FOOTER_LENGTH = 8;

    private final String name;
    private final DocumentFactory documentFactory;

    private transient BlockOffsets blockOffsets;
    private transient ThreadLocal<ByteBufferInputStream> threadLocalByteBufferInputStream;

    public Bz2BlockIndexedDocumentCollection(String name, DocumentFactory documentFactory) {
	this.name = new File(name).getName();
	this.documentFactory = documentFactory;
    }

    @Override
    public void filename(CharSequence absolutePathToAFileInTheCollection) throws IOException {
	initFiles(new File(absolutePathToAFileInTheCollection.toString()).getParentFile());
    }

    private void initFiles(File absolutePathToCollection) throws IOException {
	File bz2File = new File(absolutePathToCollection, name + BZ2_EXTENSION);
	FileInputStream bz2InputStream = new FileInputStream(bz2File);
	ByteBufferInputStream byteBufferInputStream = ByteBufferInputStream.map(bz2InputStream.getChannel(), MapMode.READ_ONLY);
	bz2InputStream.close();

	File blockOffsetsFile = new File(absolutePathToCollection, name + BLOCK_OFFSETS_EXTENSION);
	FileInputStream blockOffsetsDataInput = new FileInputStream(blockOffsetsFile);
	init(byteBufferInputStream, blockOffsetsDataInput);
	blockOffsetsDataInput.close();
    }

    public void init(final ByteBufferInputStream byteBufferInputStream, InputStream blockOffsetsInputStream) throws IOException {
	threadLocalByteBufferInputStream = new ThreadLocal<ByteBufferInputStream>() {
	    @Override
	    protected ByteBufferInputStream initialValue() {
		return byteBufferInputStream.copy();
	    }
	};

	DataInputStream blockOffsetsDataInput = new DataInputStream(blockOffsetsInputStream);
	try {
	    blockOffsets = (BlockOffsets) BinIO.loadObject(blockOffsetsDataInput);
	} catch (ClassNotFoundException e) {
	    throw new RuntimeException("BinIO.loadObject() threw:" + e);
	}
    }

    @Override
    public long size() {
	return blockOffsets.getRecordCount();
    }

    @Override
    public Document document(long index) throws IOException {
	InputStream stream = stream(index);
	Reference2ObjectMap<Enum<?>, Object> metadata = getMetadata(stream);
	return documentFactory.getDocument(stream, metadata);
    }

    @Override
    public InputStream stream(long index) throws IOException {
	InputStream inputStream = getInputStreamStartingAtDocStart(index);
	if (inputStream == null) {
	    inputStream = new ByteArrayInputStream(ZERO_BYTE_BUFFER);
	}
	return inputStream;
    }

    @Override
    public Reference2ObjectMap<Enum<?>, Object> metadata(long index) throws IOException {
	return getMetadata(stream(index));
    }

    private Reference2ObjectMap<Enum<?>, Object> getMetadata(InputStream stream) throws IOException {
	Reference2ObjectOpenHashMap<Enum<?>, Object> metadata = new Reference2ObjectOpenHashMap<Enum<?>, Object>();

	// TODO Why is this not picked up from the factories metadata?
	metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "UTF-8");

	return metadata;
    }

    @Override
    public DocumentCollection copy() {
	throw new UnsupportedOperationException();
    }

    @Override
    public DocumentFactory factory() {
	return documentFactory;
    }

    private InputStream getInputStreamStartingAtDocStart(long docId) throws IOException {
	// When/If the BZip2 block start marker occurs naturally in the
	// compressed data there will be blockOffsets entries that aren't
	// actually start of block. They are very rare.
	long probableBlockOffsetIndex = blockOffsets.getBlockOffsetIndex(docId);

	if (probableBlockOffsetIndex < 0) {
	    // docId is smaller that the first doc in the collection.
	    return null;
	}

	int retries = 3;
	while (retries > 0) {
	    final BufferedInputStream bis = getInputStreamStartingAt(probableBlockOffsetIndex);
	    PositionResult result = positionInputStreamAtDocStart(bis, docId, probableBlockOffsetIndex == 0);
	    switch (result) {
	    case FOUND:
		return new InputStream() {
		    private int b;

		    // Only read up to record delimiter.
		    @Override
		    public int read() throws IOException {
			if (b != -1) {
			    b = bis.read();
			    if (b == RECORD_DELIMITER) {
				b = -1;
			    }
			}
			return b;
		    }
		};
	    case NOT_FOUND:
		return null;
	    case TRY_PREVIOUS_BLOCK:
		LOGGER.info("got TRY_PREVIOUS_BLOCK for doc id:" + docId);
		retries--;
		probableBlockOffsetIndex--;
		if (probableBlockOffsetIndex < 0) {
		    return null;
		}
		break;
	    default:
		throw new IllegalStateException("Don't know what to do with a " + result + " got while looking for docId" + docId);
	    }
	}
	return null;
    }

    /**
     * @param blockOffsetIndex
     * @return an InputStream that reads the contents on one compressed block
     * @throws IOException
     */
    private InputStream getCompressedBlockInputStream(final long blockOffsetIndex) throws IOException {
	final long startOffset = blockOffsets.getBlockOffset(blockOffsetIndex);
	final long endOffset = blockOffsets.getBlockOffset(blockOffsetIndex + 1) + FOOTER_LENGTH;

	final ByteBufferInputStream byteBufferInputStream = threadLocalByteBufferInputStream.get();
	byteBufferInputStream.position(startOffset);

	return new InputStream() {
	    private long readOffset = startOffset;

	    @Override
	    public int read() throws IOException {
		if (readOffset >= endOffset) {
		    return -1;
		}
		readOffset++;
		return byteBufferInputStream.read() & 0xFF;
	    }
	};
    }

    private BufferedInputStream getInputStreamStartingAt(final long startBlockOffsetIndex) throws IOException {
	// As documents will span blocks, we need to create a new de-compressor
	// when there are no more bytes available from the current
	// de-compressor.
	InputStream uncompressedInputStream = new InputStream() {
	    private long blockOffsetIndex = startBlockOffsetIndex;
	    private CBZip2InputStream uncompressedInputStream = new CBZip2InputStream(getCompressedBlockInputStream(blockOffsetIndex), READ_MODE.BYBLOCK);

	    @Override
	    public int read() throws IOException {
		int b = uncompressedInputStream.read();
		return b;
	    }

	    @Override
	    public int read(byte[] b, int off, int len) throws IOException {
		int bytesRead = uncompressedInputStream.read(b, off, len);
		if (bytesRead < 0) {
		    // Next block
		    blockOffsetIndex++;
		    if (blockOffsetIndex < blockOffsets.getFileSize()) {
			InputStream compressedInputStream = getCompressedBlockInputStream(blockOffsetIndex);
			uncompressedInputStream = new CBZip2InputStream(compressedInputStream, READ_MODE.BYBLOCK);
			bytesRead = uncompressedInputStream.read(b, off, len);
		    } else {
			return -1;
		    }
		}
		return bytesRead;
	    }
	};

	return new BufferedInputStream(uncompressedInputStream);
    }

    private static final int BYTE_BUFFER_LENGTH = 32;

    private enum PositionResult {
	FOUND, NOT_FOUND, TRY_PREVIOUS_BLOCK, TRY_NEXT_BLOCK
    };

    private PositionResult positionInputStreamAtDocStart(BufferedInputStream is, long docId, boolean atRecordStart) throws IOException {
	byte[] byteBuffer = new byte[BYTE_BUFFER_LENGTH];

	int recordCount = 0;
	int totalBytesRead = 0;

	if (!atRecordStart) {
	    totalBytesRead = readTillNextRecordStart(is);
	    if (totalBytesRead < 0) {
		return PositionResult.NOT_FOUND;
	    }
	}
	
	PositionResult result = null;
	do {
	    is.mark(BYTE_BUFFER_LENGTH);

	    // Expect to read a long as a string terminated with a tab.
	    int b;
	    int bytesRead = 0;
	    while (true) {
		b = is.read();
		totalBytesRead++;
		if (b < 0) {
		    return PositionResult.NOT_FOUND;
		}
		if (b >= '0' && b <= '9') {
		    byteBuffer[bytesRead++] = (byte) b;
		    if (bytesRead == byteBuffer.length) {
			throw new IllegalStateException("Doc ID too long. Record started with " + new String(byteBuffer, 0, bytesRead));
		    }
		} else if (b == FIELD_DELIMITER) {
		    break;
		} else {
		    throw new IllegalStateException("Unexpected byte in doc ID >" + b + "<. Record started with " + new String(byteBuffer, 0, bytesRead));
		}
	    }

	    String readDocIdString = new String(byteBuffer, 0, bytesRead);
	    long readDocId = Long.parseLong(readDocIdString);

	    recordCount++;

	    if (readDocId > docId) {
		is.reset();
		if (recordCount == 1) {
		    // The first record has an Doc ID greater that the one we
		    // are looking for.
		    result = PositionResult.TRY_PREVIOUS_BLOCK;
		} else {
		    result = PositionResult.NOT_FOUND;
		}
	    } else if (readDocId == docId) {
		is.reset();
		result = PositionResult.FOUND;
	    } else if (totalBytesRead > BZ2_BLOCK_SIZE * 10) {
		result = PositionResult.NOT_FOUND;
	    } else {
		bytesRead = readTillNextRecordStart(is);
		if (bytesRead < 0) {
		    result = PositionResult.NOT_FOUND;
		} else {
		    totalBytesRead += bytesRead;
		    if (totalBytesRead > BZ2_BLOCK_SIZE * 10) {
			result = PositionResult.TRY_NEXT_BLOCK;
		    }
		}
	    }
    	} while (result == null);
	
	if (totalBytesRead > BZ2_BLOCK_SIZE * 2) {
	    float blocksRead = (float)totalBytesRead / BZ2_BLOCK_SIZE;
	    LOGGER.info("Had to read " + blocksRead + " blocks before PositionResult was set to " + result);
	}
	
	return result;
    }

    private int readTillNextRecordStart(InputStream is) throws IOException {
	int b;
	int byteCount = 0;
	while ((b = is.read()) != -1) {
	    byteCount++;
	    if (b == RECORD_DELIMITER) {
		return byteCount;
	    }
	}
	return -byteCount;
    }
}
