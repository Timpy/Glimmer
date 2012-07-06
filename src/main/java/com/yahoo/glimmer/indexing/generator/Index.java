package com.yahoo.glimmer.indexing.generator;

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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.mg4j.index.CompressionFlags;
import it.unimi.dsi.mg4j.index.CompressionFlags.Coding;
import it.unimi.dsi.mg4j.index.CompressionFlags.Component;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.mg4j.index.IndexWriter;
import it.unimi.dsi.mg4j.index.SkipBitStreamIndexWriter;
import it.unimi.dsi.util.Properties;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.yahoo.glimmer.indexing.CombinedTermProcessor;

public class Index {
    private static final int HEIGHT = 10;
    private static final int QUANTUM = 8;
    private static final int TEMP_BUFFER_SIZE = 512 * 1024; // 512KB buffer per index

    private PrintWriter terms;
    private OutputBitStream index, offsets, posNumBits;
    private OutputStream properties;
    private IndexWriter indexWriter;

    private FileSystem fs;
    private String outputDir, indexName;
    private int numDocs;

    private boolean positions;

    public Index(FileSystem fs, String outputDir, String indexName, int numDocs, boolean positions) {
	this.fs = fs;
	this.outputDir = outputDir;
	// It seems like MG4J doesn't like index names with the '-' char
	this.indexName = indexName.replaceAll("\\-", "_");
	this.numDocs = numDocs;
	this.positions = positions;
    }

    public void open() throws IOException {
	Path indexPath = new Path(outputDir + "/" + indexName + DiskBasedIndex.INDEX_EXTENSION);
	FSDataOutputStream indexOutputStream = fs.create(indexPath, true);
	index = new OutputBitStream(indexOutputStream);// overwrite

	Path termsPath = new Path(outputDir + "/" + indexName + DiskBasedIndex.TERMS_EXTENSION);
	terms = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fs.create(termsPath, true), "UTF-8")));// overwrite

	Path offsetsPath = new Path(outputDir + "/" + indexName + DiskBasedIndex.OFFSETS_EXTENSION);
	offsets = new OutputBitStream(fs.create(offsetsPath, true));// overwrite

	if (positions) {
	    Path posNumBitsPath = new Path(outputDir + "/" + indexName + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION);
	    posNumBits = new OutputBitStream(fs.create(posNumBitsPath, true));// overwrite
	}

	Path propertiesPath = new Path(outputDir + "/" + indexName + DiskBasedIndex.PROPERTIES_EXTENSION);
	properties = fs.create(propertiesPath, true);// overwrite

	Map<Component, Coding> defaultStandardIndex = new Object2ObjectOpenHashMap<Component, Coding>(CompressionFlags.DEFAULT_STANDARD_INDEX);
	if (!positions) {
	    defaultStandardIndex.remove(CompressionFlags.Component.POSITIONS);
	    defaultStandardIndex.remove(CompressionFlags.Component.COUNTS);
	}

	indexWriter = new SkipBitStreamIndexWriter(index, offsets, posNumBits, numDocs, TEMP_BUFFER_SIZE, defaultStandardIndex, QUANTUM, HEIGHT);
    }

    public PrintWriter getTermsWriter() {
	return terms;
    }

    public boolean hasPositions() {
	return positions;
    }

    public IndexWriter getIndexWriter() {
	return indexWriter;
    }

    public OutputStream getPropertiesStream() {
	return properties;
    }

    public void close(long writtenOccurrences) throws IOException {
	try {
	    Properties props = indexWriter.properties();
	    System.out.println("Closing index " + indexName + " which has " + props.getProperty(it.unimi.dsi.mg4j.index.Index.PropertyKeys.TERMS) + " terms ");
	    if (positions) {
		props.setProperty(it.unimi.dsi.mg4j.index.Index.PropertyKeys.OCCURRENCES, writtenOccurrences);
	    }
	    props.setProperty(it.unimi.dsi.mg4j.index.Index.PropertyKeys.MAXCOUNT, -1);
	    props.setProperty(it.unimi.dsi.mg4j.index.Index.PropertyKeys.FIELD, indexName);
	    props.setProperty(it.unimi.dsi.mg4j.index.Index.PropertyKeys.TERMPROCESSOR, CombinedTermProcessor.getInstance());

	    props.save(properties);
	} catch (ConfigurationException e) {
	    throw new IOException(e.getMessage());
	}

	properties.close();
	terms.close();
	indexWriter.close();
    }
}