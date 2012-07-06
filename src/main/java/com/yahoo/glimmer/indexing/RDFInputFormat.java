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
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class RDFInputFormat extends FileInputFormat<LongWritable, Document> {

    public static final String DOCUMENTFACTORY_CLASS = "documentfactory.class";

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
	CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	return codec == null;
    }

    @Override
    public RecordReader<LongWritable, Document> createRecordReader(InputSplit split, TaskAttemptContext context) {
	// Configure the document factory
	Class<?> documentFactoryClass = context.getConfiguration().getClass(DOCUMENTFACTORY_CLASS, PropertyBasedDocumentFactory.class);
	DocumentFactory factory = RDFDocumentFactory.buildFactory(documentFactoryClass, context);
	
	return new RDFRecordReader(factory);
    }
}
