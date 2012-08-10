package com.yahoo.glimmer.indexing;

/*		 
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentCollection;
import it.unimi.dsi.mg4j.document.DocumentCollectionBuilder;
import it.unimi.dsi.mg4j.document.DocumentFactory;
import it.unimi.dsi.mg4j.document.DocumentFactory.FieldType;
import it.unimi.dsi.mg4j.document.DocumentIterator;
import it.unimi.dsi.mg4j.document.DocumentSequence;
import it.unimi.dsi.mg4j.document.SimpleCompressedDocumentCollection;
import it.unimi.dsi.mg4j.document.ZipDocumentCollection;
import it.unimi.dsi.mg4j.tool.Scan;
import it.unimi.dsi.mg4j.tool.Scan.VirtualDocumentFragment;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A builder for {@linkplain InstantiatableSimpleCompressedDocumentCollection
 * simple compressed document collections}.
 * 
 * @author Sebastiano Vigna
 */

public class HdfsSimpleCompressedDocumentCollectionBuilder implements DocumentCollectionBuilder {
    private static final FsPermission ALL_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    /** The factory of the base document sequence. */
    private final DocumentFactory factory;
    /**
     * Whether will are building an exact collection (i.e., whether it stores
     * nonwords).
     */
    private final boolean exact;
    /** A frequency keeper used to compress document terms. */
    private final FrequencyCodec termsFrequencyKeeper;
    /**
     * A frequency keeper used to compress document nonterms, or
     * <code>null</code> if {@link #exact} is false.
     */
    private final FrequencyCodec nonTermsFrequencyKeeper;

    private final FileSystem fs;

    private final Path hdfsPath;

    /** The basename of the builder. */
    private String basename;
    /** The basename of current collection. */
    private String basenameSuffix;
    /** The output bit stream for documents. */
    private OutputBitStream documentsOutputBitStream;
    /** The output stream for terms. */
    private CountingOutputStream termsOutputStream;
    /**
     * The output stream for nonterms, or <code>null</code> if {@link #exact} is
     * false.
     */
    private CountingOutputStream nonTermsOutputStream;
    /** The output bit stream for document offsets. */
    private OutputBitStream documentOffsetsObs;
    /** The output bit stream for term offsets. */
    private OutputBitStream termOffsetsObs;
    /**
     * The output bit stream for nonterms offsets, or <code>null</code> if
     * {@link #exact} is false.
     */
    private OutputBitStream nonTermOffsetsObs;
    /**
     * A temporary cache for the content of a field as a list of global term
     * numbers. If the collection is exact, it alternates terms and nonterms.
     */
    private IntArrayList fieldContent;
    /** The map from term to global term numbers, in order of appearance. */
    private Object2IntOpenHashMap<MutableString> terms;
    /**
     * The map from term to global nonterm numbers, in order of appearance, or
     * <code>null</code> if {@link #exact} is false.
     */
    private Object2IntOpenHashMap<MutableString> nonTerms;
    /** The number of documents indexed so far. */
    private int documents;
    /** The number of words indexed so far. */
    private long words;
    /** The number of fields indexed so far. */
    private long fields;
    /** The number of bits used to code words. */
    private long bitsForWords;
    /** The number of bits used to code nonwords. */
    private long bitsForNonWords;
    /**
     * The number of bits used to code field lengths (the number of
     * words/nonwords pairs).
     */
    private long bitsForFieldLengths;
    /** The number of bits used to code URIs. */
    private long bitsForUris;
    /** The number of bits used to code document titles. */
    private long bitsForTitles;
    /** Whether we are compressing non-text or virtual fields. */
    private boolean hasNonText;
    /**
     * The zip output stream used to store non-text and virtual fields if
     * {@link #hasNonText} is true, or <code>null</code> otherwise.
     */
    private ZipOutputStream nonTextZipOutputStream;
    /** {@link #nonTextZipOutputStream} wrapped in a {@link DataOutputStream}. */
    private DataOutputStream nonTextZipDataOutputStream;

    /**
     * A simple codec for integers that remaps frequent numbers to smaller
     * numbers.
     */
    protected static class FrequencyCodec {
	/** The size of the symbol queue. */
	private final static int MAX_QUEUE_SIZE = 2048;
	/** The symbol queue. */
	private final int[] queue;
	/** An array parallel to {@link #queue} containing frequencies. */
	private final int[] freq;
	/** A map from input symbols to positions in {@link #queue}. */
	private final Int2IntOpenHashMap code2Pos;
	/** The current size of {{@link #queue}. */
	private int queueSize;

	public FrequencyCodec() {
	    code2Pos = new Int2IntOpenHashMap();
	    code2Pos.defaultReturnValue(-1);
	    queue = new int[MAX_QUEUE_SIZE];
	    freq = new int[MAX_QUEUE_SIZE];
	}

	/** Empties the queue and the symbol-to-position map. */
	public void reset() {
	    queueSize = 0;
	    code2Pos.clear();
	}

	private final void newSymbol(final int symbol) {
	    if (queueSize == MAX_QUEUE_SIZE) {
		// Queue filled up. First, we guarantee that there are elements
		// with frequency one.
		if (freq[MAX_QUEUE_SIZE - 1] != 1)
		    for (int j = MAX_QUEUE_SIZE; j-- != 0;)
			freq[j] /= freq[MAX_QUEUE_SIZE - 1];
		// Then, we remove half of them.
		int j = MAX_QUEUE_SIZE;
		while (j-- != 0)
		    if (freq[j] > 1)
			break;
		for (int k = j + (MAX_QUEUE_SIZE - j) / 2; k < MAX_QUEUE_SIZE; k++) {
		    code2Pos.remove(queue[k]);
		}
		queueSize = j + (MAX_QUEUE_SIZE - j) / 2;
	    }

	    // Now we know that we have space.
	    code2Pos.put(symbol, queueSize);
	    queue[queueSize] = symbol;
	    freq[queueSize] = 1;
	    queueSize++;
	}

	private final void oldSymbol(final int pos) {
	    // Term already in list
	    // Find term to exchange for change of frequency
	    int ex = pos;
	    while (ex >= 0 && freq[ex] == freq[pos])
		ex--;
	    ++ex;
	    freq[pos]++;
	    // Exchange
	    int t = queue[pos];
	    queue[pos] = queue[ex];
	    queue[ex] = t;
	    t = freq[pos];
	    freq[pos] = freq[ex];
	    freq[ex] = t;
	    code2Pos.put(queue[ex], ex);
	    code2Pos.put(queue[pos], pos);
	}

	/**
	 * Encodes a symbol, returning a (hopefully smaller) symbol.
	 * 
	 * @param symbol
	 *            the input symbol.
	 * @return the output symbol.
	 */
	public int encode(final int symbol) {
	    final int pos = code2Pos.get(symbol);
	    if (pos == -1) {
		final int result = queueSize + symbol;
		newSymbol(symbol);
		return result;
	    } else {
		oldSymbol(pos);
		return pos;
	    }
	}

	/**
	 * Decodes a symbol, returning the original symbol.
	 * 
	 * @param symbol
	 *            a symbol an encoded file.
	 * @return the corresponding original input symbol.
	 */
	public int decode(final int symbol) {

	    if (symbol < queueSize) {
		final int result = queue[symbol];
		oldSymbol(symbol);
		return result;
	    } else {
		int term = symbol - queueSize;
		newSymbol(term);
		return term;
	    }
	}
    }

    public HdfsSimpleCompressedDocumentCollectionBuilder(final String basename, final DocumentFactory factory, final boolean exact, FileSystem fs, Path hdfsPath) {
	this.basename = basename;
	this.factory = factory;
	this.exact = exact;
	this.fs = fs;
	this.hdfsPath = hdfsPath;
	this.termsFrequencyKeeper = new FrequencyCodec();
	this.nonTermsFrequencyKeeper = exact ? new FrequencyCodec() : null;

	boolean hasNonText = false;
	for (int i = factory.numberOfFields(); i-- != 0;)
	    hasNonText |= factory.fieldType(i) != FieldType.TEXT;
	this.hasNonText = hasNonText;

	terms = new Object2IntOpenHashMap<MutableString>(Scan.INITIAL_TERM_MAP_SIZE);
	terms.defaultReturnValue(-1);
	if (exact) {
	    nonTerms = new Object2IntOpenHashMap<MutableString>(Scan.INITIAL_TERM_MAP_SIZE);
	    nonTerms.defaultReturnValue(-1);
	} else
	    nonTerms = null;
    }

    public String basename() {
	return basename;
    }

    public void open(final CharSequence suffix) throws IOException {
	// Set the basename + suffix.
	basenameSuffix = basename + suffix;

	Path documentsOutputBitStreamPath = new Path(hdfsPath, basenameSuffix + SimpleCompressedDocumentCollection.DOCUMENTS_EXTENSION);
	OutputStream os = fs.create(documentsOutputBitStreamPath, true);
	documentsOutputBitStream = new OutputBitStream(os);
	fs.setPermission(documentsOutputBitStreamPath, ALL_PERMISSIONS);

	Path termsOutputStreamPath = new Path(hdfsPath, basenameSuffix + SimpleCompressedDocumentCollection.TERMS_EXTENSION);
	termsOutputStream = new CountingOutputStream(new FastBufferedOutputStream(fs.create(termsOutputStreamPath, true)));
	fs.setPermission(termsOutputStreamPath, ALL_PERMISSIONS);

	if (exact) {
	    Path nonTermsOutputStreamPath = new Path(hdfsPath, basenameSuffix + SimpleCompressedDocumentCollection.NONTERMS_EXTENSION);
	    nonTermsOutputStream = new CountingOutputStream(new FastBufferedOutputStream(fs.create(nonTermsOutputStreamPath, true)));
	    fs.setPermission(nonTermsOutputStreamPath, ALL_PERMISSIONS);
	}

	Path documentOffsetsObsPath = new Path(hdfsPath, basenameSuffix + SimpleCompressedDocumentCollection.DOCUMENT_OFFSETS_EXTENSION);
	documentOffsetsObs = new OutputBitStream(fs.create(documentOffsetsObsPath, true));
	fs.setPermission(documentOffsetsObsPath, ALL_PERMISSIONS);

	Path termOffsetsObsPath = new Path(hdfsPath, basenameSuffix + SimpleCompressedDocumentCollection.TERM_OFFSETS_EXTENSION);
	termOffsetsObs = new OutputBitStream(fs.create(termOffsetsObsPath, true));
	fs.setPermission(termOffsetsObsPath, ALL_PERMISSIONS);

	if (exact) {
	    Path nonTermOffsetsObsPath = new Path(hdfsPath, basenameSuffix + SimpleCompressedDocumentCollection.NONTERM_OFFSETS_EXTENSION);
	    nonTermOffsetsObs = new OutputBitStream(fs.create(nonTermOffsetsObsPath, true));
	    fs.setPermission(nonTermOffsetsObsPath, ALL_PERMISSIONS);
	}

	fieldContent = new IntArrayList();

	if (hasNonText) {
	    Path nonTextZipOutputStreamPath = new Path(hdfsPath, basenameSuffix + ZipDocumentCollection.ZIP_EXTENSION);
	    nonTextZipOutputStream = new ZipOutputStream(new FastBufferedOutputStream(fs.create(nonTextZipOutputStreamPath, true)));
	    nonTextZipDataOutputStream = new DataOutputStream(nonTextZipOutputStream);
	    fs.setPermission(nonTextZipOutputStreamPath, ALL_PERMISSIONS);
	}

	terms.clear();
	terms.trim(Scan.INITIAL_TERM_MAP_SIZE);
	if (exact) {
	    nonTerms.clear();
	    nonTerms.trim(Scan.INITIAL_TERM_MAP_SIZE);
	}
	words = fields = bitsForWords = bitsForNonWords = bitsForFieldLengths = bitsForUris = bitsForTitles = documents = 0;

	// First offset
	documentOffsetsObs.writeDelta(0);
	termOffsetsObs.writeDelta(0);
	if (exact)
	    nonTermOffsetsObs.writeDelta(0);

    }

    public void add(MutableString word, MutableString nonWord) throws IOException {
	int t = terms.getInt(word);
	if (t == -1) {
	    terms.put(word.copy(), t = terms.size());
	    termsOutputStream.resetByteCount();
	    word.writeSelfDelimUTF8(termsOutputStream);
	    termOffsetsObs.writeLongDelta(termsOutputStream.getByteCount());
	}
	fieldContent.add(t);
	if (exact) {
	    t = nonTerms.getInt(nonWord);
	    if (t == -1) {
		nonTerms.put(nonWord.copy(), t = nonTerms.size());
		nonTermsOutputStream.resetByteCount();
		nonWord.writeSelfDelimUTF8(nonTermsOutputStream);
		nonTermOffsetsObs.writeLongDelta(nonTermsOutputStream.getByteCount());
	    }
	    fieldContent.add(t);
	}
    }

    public static class InstantiatableSimpleCompressedDocumentCollection extends SimpleCompressedDocumentCollection {
	private static final long serialVersionUID = 7405536728183221997L;

	// Publicly expose the constructor.
	public InstantiatableSimpleCompressedDocumentCollection(String basename, long documents, long terms, long nonTerms, boolean exact, DocumentFactory factory) {
	    super(basename, documents, terms, nonTerms, exact, factory);
	}
    }

    public void close() throws IOException {
	documentsOutputBitStream.close();
	termsOutputStream.close();
	IOUtils.closeQuietly(nonTermsOutputStream);
	documentOffsetsObs.close();
	termOffsetsObs.close();
	if (nonTermOffsetsObs != null)
	    nonTermOffsetsObs.close();
	if (hasNonText) {
	    if (documents == 0)
		nonTextZipOutputStream.putNextEntry(new ZipEntry("dummy"));
	    nonTextZipDataOutputStream.close();
	}

	Path path = new Path(hdfsPath, basenameSuffix);

	final SimpleCompressedDocumentCollection simpleCompressedDocumentCollection = new InstantiatableSimpleCompressedDocumentCollection(path.toString(),
		documents, terms.size(), nonTerms != null ? nonTerms.size() : -1, exact, factory);
	Path objectPath = new Path(hdfsPath, basenameSuffix + DocumentCollection.DEFAULT_EXTENSION);
	// TODO. This is not good.  We are storing a InstantiatableSimpleCompressedDocumentCollection not a SimpleCompressedDocumentCollection
	// So to load it again the load must have access to the InstantiatableSimpleCompressedDocumentCollection class.
	FSDataOutputStream collectionOutputStream = fs.create(objectPath, true);
	BinIO.storeObject(simpleCompressedDocumentCollection, collectionOutputStream);
	collectionOutputStream.flush();
	collectionOutputStream.close();
	FsPermission allPermissions = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
	fs.setPermission(objectPath, allPermissions);
	simpleCompressedDocumentCollection.close();

	Path statsPath = new Path(hdfsPath, basenameSuffix + SimpleCompressedDocumentCollection.STATS_EXTENSION);
	final PrintStream stats = new PrintStream(fs.create(statsPath, true));
	fs.setPermission(statsPath, allPermissions);

	final long overallBits = bitsForTitles + bitsForUris + bitsForFieldLengths + bitsForWords + bitsForNonWords;
	stats.println("Documents: " + Util.format(documents) + " (" + Util.format(overallBits) + ", " + Util.format(overallBits / (double) documents)
		+ " bits per document)");
	stats.println("Terms: " + Util.format(terms.size()) + " (" + Util.format(words) + " words, " + Util.format(bitsForWords) + " bits, "
		+ Util.format(bitsForWords / (double) words) + " bits per word)");
	if (exact)
	    stats.println("Nonterms: " + Util.format(nonTerms.size()) + " (" + Util.format(words) + " nonwords, " + Util.format(bitsForNonWords) + " bits, "
		    + Util.format(bitsForNonWords / (double) words) + " bits per nonword)");
	stats.println("Bits for field lengths: " + Util.format(bitsForFieldLengths) + " (" + Util.format(bitsForFieldLengths / (double) fields)
		+ " bits per field)");
	stats.println("Bits for URIs: " + Util.format(bitsForUris) + " (" + Util.format(bitsForUris / (double) documents) + " bits per URI)");
	stats.println("Bits for titles: " + Util.format(bitsForTitles) + " (" + Util.format(bitsForTitles / (double) documents) + " bits per title)");
	stats.close();
    }

    public void endDocument() throws IOException {
	documentOffsetsObs.writeLongDelta(documentsOutputBitStream.writtenBits());
	if (hasNonText)
	    nonTextZipOutputStream.closeEntry();
    }

    public void endTextField() throws IOException {
	final int size = fieldContent.size();
	words += size / (exact ? 2 : 1);
	bitsForFieldLengths += documentsOutputBitStream.writeDelta(size / (exact ? 2 : 1));
	termsFrequencyKeeper.reset();
	if (exact) {
	    nonTermsFrequencyKeeper.reset();
	    for (int i = 0; i < size; i += 2) {
		bitsForWords += documentsOutputBitStream.writeDelta(termsFrequencyKeeper.encode(fieldContent.getInt(i)));
		bitsForNonWords += documentsOutputBitStream.writeDelta(nonTermsFrequencyKeeper.encode(fieldContent.getInt(i + 1)));
	    }
	} else
	    for (int i = 0; i < size; i++)
		bitsForWords += documentsOutputBitStream.writeDelta(termsFrequencyKeeper.encode(fieldContent.getInt(i)));
    }

    public void nonTextField(Object o) throws IOException {
	final ObjectOutputStream oos = new ObjectOutputStream(nonTextZipDataOutputStream);
	oos.writeObject(o);
	oos.flush();
    }

    public static int writeSelfDelimitedUtf8String(final OutputBitStream obs, final CharSequence s) throws IOException {
	final int len = s.length();
	int bits = 0;
	bits += obs.writeDelta(len);
	for (int i = 0; i < len; i++)
	    bits += obs.writeZeta(s.charAt(i), 7);
	return bits;
    }

    public void startDocument(CharSequence title, CharSequence uri) throws IOException {
	documentsOutputBitStream.writtenBits(0);
	bitsForUris += writeSelfDelimitedUtf8String(documentsOutputBitStream, uri == null ? "" : uri);
	bitsForTitles += writeSelfDelimitedUtf8String(documentsOutputBitStream, title);
	if (hasNonText) {
	    final ZipEntry currEntry = new ZipEntry(Integer.toString(documents));
	    nonTextZipOutputStream.putNextEntry(currEntry);

	}
	documents++;
    }

    public void startTextField() {
	fieldContent.size(0);
	fields++;
    }

    public void virtualField(final ObjectList<VirtualDocumentFragment> fragments) throws IOException {
	nonTextZipDataOutputStream.writeInt(fragments.size());
	for (VirtualDocumentFragment fragment : fragments) {
	    fragment.documentSpecifier().writeSelfDelimUTF8(nonTextZipOutputStream);
	    fragment.text().writeSelfDelimUTF8(nonTextZipOutputStream);
	}
    }

    @SuppressWarnings("unchecked")
    public void build(final DocumentSequence inputSequence) throws IOException {
	final DocumentIterator docIt = inputSequence.iterator();
	if (factory != inputSequence.factory())
	    throw new IllegalStateException("The factory provided by the constructor does not correspond to the factory of the input sequence");
	final int numberOfFields = factory.numberOfFields();
	WordReader wordReader;
	MutableString word = new MutableString();
	MutableString nonWord = new MutableString();

	open("");
	for (;;) {
	    Document document = docIt.nextDocument();
	    if (document == null)
		break;
	    startDocument(document.title(), document.uri());

	    for (int field = 0; field < numberOfFields; field++) {
		Object content = document.content(field);
		if (factory.fieldType(field) == FieldType.TEXT) {
		    startTextField();
		    wordReader = document.wordReader(field);
		    wordReader.setReader((Reader) content);
		    while (wordReader.next(word, nonWord))
			add(word, nonWord);
		    endTextField();
		} else if (factory.fieldType(field) == FieldType.VIRTUAL)
		    virtualField((ObjectList<VirtualDocumentFragment>) content);
		else
		    nonTextField(content);
	    }
	    document.close();
	    endDocument();
	}
	docIt.close();
	close();
    }
}
