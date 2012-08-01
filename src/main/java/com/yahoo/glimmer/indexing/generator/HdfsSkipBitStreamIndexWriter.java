package com.yahoo.glimmer.indexing.generator;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import it.unimi.di.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.mg4j.index.CompressionFlags.Component;
import it.unimi.di.mg4j.index.SkipBitStreamIndexWriter;
import it.unimi.dsi.io.OutputBitStream;

public class HdfsSkipBitStreamIndexWriter extends SkipBitStreamIndexWriter {
    protected static class HdfsSkipStreamFactory extends SkipStreamFactory {
	private final FileSystem fileSystem;
	
	public HdfsSkipStreamFactory(FileSystem fileSystem) {
	    this.fileSystem = fileSystem;
	}
	
	@Override
	public OutputBitStream bitStream(String filename) throws IOException {
	    Path path = new Path(filename);
	    FSDataOutputStream outputStream = fileSystem.create(path, true); // overwrite
	    return new OutputBitStream(outputStream);
	}
	
	@Override
	public Writer writer(String filename) throws IOException {
	    Path path = new Path(filename);
	    FSDataOutputStream outputStream = fileSystem.create(path, true); // overwrite
	    return new OutputStreamWriter(outputStream);
	}
    }
    
    public HdfsSkipBitStreamIndexWriter(FileSystem fs, Path basename, int numberOfDocuments, boolean writeOffsets, int tempBufferSize, Map<Component, Coding> flags,
	    int quantum, int height) throws IOException {
	super(new HdfsSkipStreamFactory(fs), basename.toString(), numberOfDocuments, writeOffsets, tempBufferSize, flags, quantum, height);
    }
}
