package com.yahoo.glimmer.indexing.preprocessor;

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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Writes to different output files depending on the contents of the value.
 * 
 * @author tep
 * 
 */
public class ResourceRecordWriter extends RecordWriter<Text, Text> {
    private static final char BY_SUBJECT_DELIMITER = '\t';

    public static enum OUTPUT {
	ALL("all"), BY_SUBJECT("bySubject"), CONTEXT("contexts"), OBJECT("objects"), PREDICATE("predicates"), SUBJECT("subjects");

	final String filename;

	private OUTPUT(String filename) {
	    this.filename = filename;
	}
    }

    private HashMap<OUTPUT, Writer> writersMap = new HashMap<OUTPUT, Writer>();

    public ResourceRecordWriter(FileSystem fs, Path taskWorkPath, CompressionCodec codecIfAny) throws IOException {
	if (fs.exists(taskWorkPath)) {
	    throw new IOException("Task work path already exists:" + taskWorkPath.toString());
	}
	fs.mkdirs(taskWorkPath);

	for (OUTPUT output : OUTPUT.values()) {
	    OutputStream out;
	    if (codecIfAny != null) {
		Path file = new Path(taskWorkPath, output.filename + codecIfAny.getDefaultExtension());
		out = fs.create(file, false);
		out = codecIfAny.createOutputStream(out);
	    } else {
		Path file = new Path(taskWorkPath, output.filename);
		out = fs.create(file, false);
	    }
	    writersMap.put(output, new OutputStreamWriter(out, Charset.forName("UTF-8")));
	}
    }

    /**
     * @param key
     *            A resource as an unquoted string.
     * @param value
     *            VALUE_DELIMITER separated <predicate> <object> <context> .
     *            string or one of 'ALL' 'PREDICATE' 'OBJECT' or 'CONTEXT' depending
     *            on where the key should be written.
     */
    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
	String keyString = key.toString();
	String valueString = value.toString();

	if (OUTPUT.ALL.name().equals(valueString)) {
	    Writer allWriter = writersMap.get(OUTPUT.ALL);
	    allWriter.write(keyString);
	    allWriter.write('\n');
	} else if (OUTPUT.PREDICATE.name().equals(valueString)) {
	    Writer predicateWriter = writersMap.get(OUTPUT.PREDICATE);
	    predicateWriter.write(keyString);
	    predicateWriter.write('\n');
	} else if (OUTPUT.OBJECT.name().equals(valueString)) {
	    Writer objectWriter = writersMap.get(OUTPUT.OBJECT);
	    objectWriter.write(keyString);
	    objectWriter.write('\n');
	} else if (OUTPUT.CONTEXT.name().equals(valueString)) {
	    Writer contextWriter = writersMap.get(OUTPUT.CONTEXT);
	    contextWriter.write(keyString);
	    contextWriter.write('\n');
	} else {
	    // SUBJECT
	    Writer subjectWriter = writersMap.get(OUTPUT.SUBJECT);
	    subjectWriter.write(keyString);
	    subjectWriter.write('\n');
	    Writer bySubjectWriter = writersMap.get(OUTPUT.BY_SUBJECT);
	    bySubjectWriter.write(keyString);
	    bySubjectWriter.write(BY_SUBJECT_DELIMITER);
	    bySubjectWriter.write(valueString);
	    bySubjectWriter.write('\n');
	}
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	for (Writer writer : writersMap.values()) {
	    writer.close();
	}
    }

    public static class OutputFormat extends FileOutputFormat<Text, Text> {
	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
	    Path taskWorkPath = getDefaultWorkFile(job, "");
	    Configuration conf = job.getConfiguration();
	    CompressionCodec outputCompressionCodec = null;
	    if (getCompressOutput(job)) {
		Class<? extends CompressionCodec> outputCompressorClass = getOutputCompressorClass(job, BZip2Codec.class);
		outputCompressionCodec = ReflectionUtils.newInstance(outputCompressorClass, conf);
	    }

	    FileSystem fs = FileSystem.get(conf);

	    return new ResourceRecordWriter(fs, taskWorkPath, outputCompressionCodec);
	}
    }
}
