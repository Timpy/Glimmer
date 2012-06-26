package com.yahoo.glimmer.indexing.preprocessor;

import java.io.IOException;
import java.io.OutputStream;
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
 * @author tep
 *
 */
public class ResourceRecordWriter extends RecordWriter<Text, Text> {
    private static final char BY_SUBJECT_DELIMITER = '\t';
    private static final String ALL = "all";
    private static final String BY_SUBJECT = "bySubject";
    private static final String SUBJECT = "subject";

    private static final String[] OUTPUTS = { ALL, BY_SUBJECT, SUBJECT, TuplesToResourcesMapper.CONTEXT_VALUE, TuplesToResourcesMapper.OBJECT_VALUE,
	    TuplesToResourcesMapper.PREDICATE_VALUE };

    private HashMap<String, OutputStream> outputStreamsMap = new HashMap<String, OutputStream>();

    public ResourceRecordWriter(FileSystem fs, Path taskWorkPath, CompressionCodec codecIfAny) throws IOException {
	if (fs.exists(taskWorkPath)) {
	    throw new IOException("Task work path already exists:" + taskWorkPath.toString());
	}
	fs.mkdirs(taskWorkPath);
	
	for (String key : OUTPUTS) {
	    OutputStream out;
	    if (codecIfAny != null) {
		Path file = new Path(taskWorkPath, key.toLowerCase() + codecIfAny.getDefaultExtension());
		out = fs.create(file, false);
		out = codecIfAny.createOutputStream(out);
	    } else {
		Path file = new Path(taskWorkPath, key.toLowerCase());
		out = fs.create(file, false);
	    }
	    outputStreamsMap.put(key, out);
	}
   }
    
    

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
	OutputStream out = outputStreamsMap.get(ALL);
	out.write(key.getBytes(), 0, key.getLength());
	out.write('\n');

	byte[] valueBytes = value.getBytes();
	int subjectsEndIdx = value.getLength();
	subjectsEndIdx = writeIfType(key, valueBytes, subjectsEndIdx, TuplesToResourcesMapper.CONTEXT_VALUE);
	if (subjectsEndIdx <= 0) {
	    return;
	}
	subjectsEndIdx = writeIfType(key, valueBytes, subjectsEndIdx, TuplesToResourcesMapper.OBJECT_VALUE);
	if (subjectsEndIdx <= 0) {
	    return;
	}
	subjectsEndIdx = writeIfType(key, valueBytes, subjectsEndIdx, TuplesToResourcesMapper.PREDICATE_VALUE);
	if (subjectsEndIdx <= 0) {
	    return;
	}

	// Bytes left in value after cutting CONTEXT/OBJECT/PREDICATE off the end..  Write subject and bySubject.
	out = outputStreamsMap.get(SUBJECT);
	out.write(key.getBytes(), 0, key.getLength());
	out.write('\n');

	out = outputStreamsMap.get(BY_SUBJECT);
	out.write(key.getBytes(), 0, key.getLength());
	out.write(BY_SUBJECT_DELIMITER);
	out.write(valueBytes, 0, subjectsEndIdx);
	out.write('\n');
    }

    private int writeIfType(Text key, byte[] valueBytes, int subjectsEndIdx, String type) throws IOException {
	byte[] typeBytes = type.getBytes();
	if (byteArrayRegionMatches(valueBytes, subjectsEndIdx - typeBytes.length, typeBytes, typeBytes.length)) {
	    OutputStream out = outputStreamsMap.get(type);
	    out.write(key.getBytes(), 0, key.getLength());
	    out.write('\n');
	    return subjectsEndIdx - typeBytes.length - ResourcesReducer.VALUE_DELIMITER.length();
	}
	return subjectsEndIdx;
    }

    static boolean byteArrayRegionMatches(byte[] big, int bigStart, byte[] small, int len) {
	if (bigStart < 0) {
	    return false;
	}
	int bi = bigStart;
	int si = 0;
	while (big[bi++] == small[si++] && len > si) {
	}
	return si == len;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	for (OutputStream out : outputStreamsMap.values()) {
	    out.close();
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
