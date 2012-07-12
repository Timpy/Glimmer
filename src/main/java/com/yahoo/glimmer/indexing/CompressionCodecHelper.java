package com.yahoo.glimmer.indexing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class CompressionCodecHelper {
    private static CompressionCodecFactory compressionCodecFactory;
    
    private synchronized static void initialiseCompressionCodecFactory(Configuration conf) {
	if (compressionCodecFactory == null) {
	    compressionCodecFactory = new CompressionCodecFactory(conf);
	}
    }

    public static CompressionCodec getCompressionCodec(Configuration conf, Path path) {
	if (compressionCodecFactory == null) {
	    initialiseCompressionCodecFactory(conf);
	}
	return compressionCodecFactory.getCodec(path);
    }

    public static InputStream wrapStream(Configuration conf, Path path, InputStream inputStream) throws IOException {
	CompressionCodec codec = getCompressionCodec(conf, path);
	if (codec != null) {
	    return codec.createInputStream(inputStream);
	}
	return inputStream;
    }
    
    public static OutputStream wrapStream(Configuration conf, Path path, OutputStream outputStream) throws IOException {
	CompressionCodec codec = getCompressionCodec(conf, path);
	if (codec != null) {
	    return codec.createOutputStream(outputStream);
	}
	return outputStream;
    }

    public static InputStream openInputStream(Configuration conf, Path path) throws IOException {
	FileSystem fs = FileSystem.get(conf);
	InputStream inputStream = fs.open(path);
	return wrapStream(conf, path, inputStream);
    }
    
    public static OutputStream openOutputStream(Configuration conf, Path path, boolean overwrite) throws IOException {
	FileSystem fs = FileSystem.get(conf);
	OutputStream outputStream = fs.create(path, overwrite);
	return wrapStream(conf, path, outputStream);
    }
}
