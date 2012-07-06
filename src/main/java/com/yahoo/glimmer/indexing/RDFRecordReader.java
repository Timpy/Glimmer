package com.yahoo.glimmer.indexing;

/*
 * Copyright (c) 2012 Yahoo! Inc. All rights reserved.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is 
 *  distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *  See accompanying LICENSE file.
 */

import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class RDFRecordReader extends RecordReader<LongWritable, Document> {

    private static final Log LOG = LogFactory.getLog(RDFRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private DocumentFactory factory = null;

    private LongWritable key = null;
    private Document value = null;

    public RDFRecordReader(DocumentFactory factory) {
	this.factory = factory;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
	FileSplit split = (FileSplit) genericSplit;

	Configuration job = context.getConfiguration();

	this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
	start = split.getStart();
	end = start + split.getLength();
	final Path file = split.getPath();
	compressionCodecs = new CompressionCodecFactory(job);
	final CompressionCodec codec = compressionCodecs.getCodec(file);

	// open the file and seek to the start of the split
	FileSystem fs = file.getFileSystem(job);
	FSDataInputStream fileIn = fs.open(split.getPath());
	boolean skipFirstLine = false;
	if (codec != null) {
	    in = new LineReader(codec.createInputStream(fileIn), job);
	    end = Long.MAX_VALUE;
	} else {
	    if (start != 0) {
		skipFirstLine = true;
		--start;
		fileIn.seek(start);
	    }

	    in = new LineReader(fileIn, job);
	}
	if (skipFirstLine) { // skip first line and re-establish "start".
	    start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
	}
	this.pos = start;
    }

    @Override
    public float getProgress() {
	if (start == end) {
	    return 0.0f;
	} else {
	    return Math.min(1.0f, (pos - start) / (float) (end - start));
	}
    }

    public synchronized void close() throws IOException {
	if (in != null) {
	    in.close();
	}
    }

    @Override
    public LongWritable getCurrentKey() {
	return key;
    }

    @Override
    public Document getCurrentValue() {
	return value;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
	if (key == null) {
	    key = new LongWritable();
	}
	key.set(pos);

	// Always create a new document...
	// TODO: find out if we can reuse the same document
	value = factory.getDocument(null, null);

	int newSize = 0;
	while (pos < end) {
	    Text line = new Text();
	    newSize = in.readLine(line, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
	    if (newSize == 0) {
		break;
	    }
	    pos += newSize;
	    if (newSize < maxLineLength) {
		// TODO: pass in document specific metadata if we need
		((RDFDocument) value).setContent(new ByteArrayInputStream(line.getBytes()));
		break;
	    } else {
		// line too long. we create an empty doc
		LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
		((RDFDocument) value).setContent(new ByteArrayInputStream(new byte[0]));
		break;
	    }

	}
	if (newSize == 0) {
	    key = null;
	    value = null;
	    return false;
	} else {
	    return true;
	}
    }
}
