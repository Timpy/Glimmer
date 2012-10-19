package com.yahoo.glimmer.indexing;

import it.unimi.di.mg4j.document.DocumentCollectionBuilder;
import it.unimi.di.mg4j.document.DocumentFactory;
import it.unimi.di.mg4j.io.IOFactory;
import it.unimi.di.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.OutputStream;

public class StartOffsetDocumentCollectionBuilder implements DocumentCollectionBuilder {
    private final String basename;
    private final DocumentFactory factory;
    private final IOFactory ioFactory;
    
    private StartOffsetDocumentCollection collection;
    private ByteCountOutputStream documentsOutputStream;
    private OutputStream offsetsOutputStream;
    
    public StartOffsetDocumentCollectionBuilder(String basename, DocumentFactory factory, IOFactory ioFactory) {
	this.basename = basename;
	this.factory = factory;
	this.ioFactory = ioFactory;
    }
    
    @Override
    public String basename() {
	return basename;
    }

    @Override
    public void open(CharSequence suffix) throws IOException {
	collection = new StartOffsetDocumentCollection(factory);
	
	String basenameSuffixed = basename + suffix;
	String documentsFilename = basenameSuffixed + StartOffsetDocumentCollection.DOCUMENTS_EXTENSION;
	documentsOutputStream = new ByteCountOutputStream(ioFactory.getOutputStream(documentsFilename));
	
	String offsetsFilename = basenameSuffixed + StartOffsetDocumentCollection.START_OFFSETS_EXTENSION;
	offsetsOutputStream = ioFactory.getOutputStream(offsetsFilename);
    }

    @Override
    public void startDocument(CharSequence title, CharSequence uri) throws IOException {
	collection.addOffset(documentsOutputStream.getByteCount());
    }

    @Override
    public void startTextField() {
    }
    
    @Override
    public void add(MutableString word, MutableString nonWord) throws IOException {
	word.writeUTF8(documentsOutputStream);
	nonWord.writeUTF8(documentsOutputStream);
    }
    
    @Override
    public void endTextField() throws IOException {
    }
    @Override
    public void endDocument() throws IOException {
    }

    @Override
    public void nonTextField(Object o) throws IOException {
	throw new UnsupportedOperationException();
    }

    @Override
    public void virtualField(ObjectList<VirtualDocumentFragment> fragments) throws IOException {
	throw new UnsupportedOperationException();
    }
    
    @Override
    public void close() throws IOException {
	documentsOutputStream.flush();
	documentsOutputStream.close();
	collection.close();
	BinIO.storeObject(collection, offsetsOutputStream);
    }
    
    private static class ByteCountOutputStream extends OutputStream {
	private final OutputStream wrappedOutputStream;
	private int byteCount;
	
	public ByteCountOutputStream(OutputStream outputStream) {
	    wrappedOutputStream = outputStream;
	}
	
	public int getByteCount() {
	    return byteCount;
	}
	
	@Override
	public void write(int b) throws IOException {
	    wrappedOutputStream.write(b);
	    byteCount++;
	}
	
	@Override
	public void flush() throws IOException {
	    wrappedOutputStream.flush();
	}

	@Override
	public void close() throws IOException {
	    wrappedOutputStream.close();
	}
    }
}
