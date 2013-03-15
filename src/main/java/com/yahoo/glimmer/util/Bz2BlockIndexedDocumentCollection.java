package com.yahoo.glimmer.util;

import it.unimi.di.big.mg4j.document.AbstractDocumentCollection;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;

public class Bz2BlockIndexedDocumentCollection extends AbstractDocumentCollection implements Serializable {
    private static final long serialVersionUID = -7943857364950329249L;

    private static final char RECORD_DELIMITER = '\n';
    private static final char FIELD_DELIMITER = '\t';

    private static final byte[] ZERO_BYTE_BUFFER = new byte[0];

    // We use the smallest block size during compression to improve retrieve
    // time(100KB). Use twice this when scanning for the record's doc id.
    private static final int N_BYTES_TO_SEARCH_FOR_DOC_ID_ = 200000;
    public static final String BZ2_EXTENSION = ".bz2";
    public static final String BLOCK_OFFSETS_EXTENSION = ".blockOffsets";

    // 6 Seems to work as it's the size of the block header I guess.
    static final int FOOTER_LENGTH = 6;

    private final String name;
    private final DocumentFactory documentFactory;

    private transient BlockOffsets blockOffsets;
    private transient ByteBuffer bz2ByteBuffer;

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
	ByteBuffer bz2ByteBuffer = bz2InputStream.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, bz2File.length());
	bz2InputStream.close();

	File blockOffsetsFile = new File(absolutePathToCollection, name + BLOCK_OFFSETS_EXTENSION);
	FileInputStream blockOffsetsDataInput = new FileInputStream(blockOffsetsFile);
	init(bz2ByteBuffer, blockOffsetsDataInput);
	blockOffsetsDataInput.close();
    }

    public void init(ByteBuffer bz2ByteBuffer, InputStream blockOffsetsInputStream) throws IOException {
	this.bz2ByteBuffer = bz2ByteBuffer;

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
	int probableBlockOffsetIndex = blockOffsets.getBlockOffsetIndex(docId);

	if (probableBlockOffsetIndex < 0) {
	    // docId is smaller that the first doc in the collection.
	    return null;
	}

	int retries = 2;
	while (retries > 0) {
	    final InputStream is = getInputStreamStartingAt(probableBlockOffsetIndex);
	    PositionResult result = positionInputStreamAtDocStart(is, docId, N_BYTES_TO_SEARCH_FOR_DOC_ID_, probableBlockOffsetIndex == 0);
	    switch (result) {
	    case FOUND:
		return new InputStream() {
		    private int b;

		    // Only read up to record delimiter.
		    @Override
		    public int read() throws IOException {
			if (b != -1) {
			    b = is.read();
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
		retries--;
		probableBlockOffsetIndex--;
		if (probableBlockOffsetIndex < 0) {
		    return null;
		}
		break;
	    default:
		throw new IllegalStateException("Don't know what to do with a " + result);
	    }
	}
	return null;
    }

    /**
     * @param blockOffsetIndex
     * @return an InputStream that reads the contents on one compressed block
     * @throws IOException
     */
    private InputStream getCompressedBlockInputStream(final int blockOffsetIndex) throws IOException {
	final long startOffset = blockOffsets.getBlockOffset(blockOffsetIndex);
	final long endOffset = blockOffsets.getBlockOffset(blockOffsetIndex + 1) + FOOTER_LENGTH;

	return new InputStream() {
	    private long offset = startOffset;

	    @Override
	    public int read() throws IOException {
		if (offset >= endOffset) {
		    return -1;
		}
		if (offset > Long.MAX_VALUE) {
		    throw new IOException("Trying to read passed the 2^31 limit of mapped files! Byte offset was:" + offset);
		}
		int b = bz2ByteBuffer.get((int)offset) & 0xFF;
		offset++;
		return b;
	    }
	};
    }

    private InputStream getInputStreamStartingAt(final int startBlockOffsetIndex) throws IOException {
	// As documents will span blocks, we need to create a new de-compressor
	// when there are no more bytes available from the current
	// de-compressor.
	InputStream uncompressedInputStream = new InputStream() {
	    private int blockOffsetIndex = startBlockOffsetIndex;
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

    private PositionResult positionInputStreamAtDocStart(InputStream is, long docId, int maxBytesToRead, boolean atRecordStart) throws IOException {
	byte[] byteBuffer = new byte[BYTE_BUFFER_LENGTH];

	int recordCount = 0;
	int totalBytesRead = 0;

	if (!atRecordStart) {
	    totalBytesRead = readTillNextRecordStart(is);
	    if (totalBytesRead < 0) {
		return PositionResult.NOT_FOUND;
	    }
	}
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
		    return PositionResult.TRY_PREVIOUS_BLOCK;
		} else {
		    return PositionResult.NOT_FOUND;
		}
	    } else if (readDocId == docId) {
		is.reset();
		return PositionResult.FOUND;
	    }

	    if (totalBytesRead > maxBytesToRead) {
		return PositionResult.NOT_FOUND;
	    }
	    bytesRead = readTillNextRecordStart(is);
	    if (bytesRead < 0) {
		return PositionResult.NOT_FOUND;
	    }
	    totalBytesRead += bytesRead;
	} while (totalBytesRead < maxBytesToRead);
	return PositionResult.TRY_NEXT_BLOCK;
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
