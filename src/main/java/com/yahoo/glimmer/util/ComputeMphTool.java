package com.yahoo.glimmer.util;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.SafelyCloseable;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ComputeMphTool extends Configured implements Tool {
    public static final FsPermission ALL_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    @Override
    public int run(String[] args) throws Exception {
	JobConf job = new JobConf(getConf(), ComputeMphTool.class);
	String[] filenames = args;

	FileSystem fs = FileSystem.get(job);

	for (String filename : filenames) {
	    buildMph(fs, filename);
	}
	return 0;
    }

    public long buildMph(final FileSystem fs, final String srcFilename) throws IOException {
	final MapReducePartInputStreamEnumeration inputStreamEnumeration;
	try {
	    inputStreamEnumeration = new MapReducePartInputStreamEnumeration(fs, new Path(srcFilename));
	} catch (IOException e) {
	    throw new RuntimeException("Failed to open " + srcFilename, e);
	}

	Collection<MutableString> inCollection = new LineReaderCollection(new LineReaderCollection.ReaderFactory() {
	    @Override
	    public Reader newReader() {
		inputStreamEnumeration.reset();
		return new InputStreamReader(new SequenceInputStream(inputStreamEnumeration));
	    }
	});

	LcpMonotoneMinimalPerfectHashFunction<CharSequence> mph = new LcpMonotoneMinimalPerfectHashFunction<CharSequence>(inCollection,
		TransformationStrategies.prefixFreeUtf16());

	String destFilename = inputStreamEnumeration.removeCompressionSuffixIfAny(srcFilename);
	Path outPath = new Path(destFilename + ".mph");
	FSDataOutputStream outStream = fs.create(outPath, true);// overwrite
	fs.setPermission(outPath, ALL_PERMISSIONS);
	ObjectOutputStream outOs = new ObjectOutputStream(outStream);
	outOs.writeObject(mph);

	Path infoPath = new Path(destFilename + ".mph.info");
	FSDataOutputStream infoStream = fs.create(infoPath, true);// overwrite
	fs.setPermission(infoPath, ALL_PERMISSIONS);
	OutputStreamWriter infoWriter = new OutputStreamWriter(infoStream);
	infoWriter.write("size\t");
	infoWriter.write(Long.toString(mph.size64()));
	infoWriter.write("\nbits\t");
	infoWriter.write(Long.toString(mph.numBits()));
	infoWriter.write("\n");
	infoWriter.close();
	infoStream.close();

	return mph.size64();
    }

    /**
     * For static invocation from pig via define mph InvokeForLong(
     * 'com.yahoo.glimmer.util.ComputeMph.pigInvoker', filename);
     * The hash function itself will be serialized to DFS as filename.mph
     * 
     * TODO Test it.
     * 
     * @param inFilename
     * @return size of generated hash
     * @throws IOException
     */
    public static long pigInvoker(String filename) throws IOException {
	return new ComputeMphTool().buildMph(FileSystem.get(null), filename);
    }

    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new ComputeMphTool(), args);
	System.exit(ret);
    }

    /**
     * Presents a Reader as a Collection of MutableStrings with each line read
     * from the Reader as an element of the Collection.
     * {@link LineReaderCollection.ReaderFactory.newReader} is called each time
     * {@link LineReaderCollection.iterator} is called. Only the current line
     * and next line are held in memory.
     * 
     * Note that {@link LineReaderCollection.LineReaderIterator.next} always
     * returns the same instance of MutableString(but with different contents)
     * for each instance of {@link LineReaderCollection.LineReaderIterator}.
     * 
     * @author tep
     * 
     */
    private static class LineReaderCollection extends AbstractCollection<MutableString> {
	private final ReaderFactory readerFactory;
	private int size = -1;

	public LineReaderCollection(ReaderFactory readerFactory) {
	    this.readerFactory = readerFactory;
	}

	public interface ReaderFactory {
	    public Reader newReader();
	}

	private class LineReaderIterator implements Iterator<MutableString>, SafelyCloseable {
	    private FastBufferedReader fbr;
	    private MutableString current = new MutableString();
	    private MutableString next = new MutableString();
	    private boolean advance = true;

	    public LineReaderIterator(Reader reader) {
		fbr = new FastBufferedReader(reader);
	    }

	    @Override
	    public boolean hasNext() {
		if (fbr == null) {
		    return false;
		}

		if (advance) {
		    try {
			if (fbr.readLine(next) == null) {
			    close();
			    return false;
			}
		    } catch (IOException e) {
			throw new RuntimeException(e);
		    }
		    advance = false;
		}
		return true;
	    }

	    @Override
	    public MutableString next() {
		if (advance) {
		    if (!hasNext()) {
			throw new NoSuchElementException();
		    }
		}
		current.replace(next);
		advance = true;
		return current;
	    }

	    @Override
	    public void remove() {
		throw new UnsupportedOperationException();
	    }

	    @Override
	    public void close() throws IOException {
		// This gets called multiple times..
		if (fbr != null) {
		    fbr.close();
		    fbr = null;
		    advance = false;
		}
	    }
	}

	@Override
	public LineReaderIterator iterator() {
	    return new LineReaderIterator(readerFactory.newReader());
	}

	@Override
	public int size() {
	    if (size == -1) {
		LineReaderIterator i = iterator();
		size = 0;
		while (i.hasNext()) {
		    size++;
		    i.next();
		}
		try {
		    i.close();
		} catch (IOException e) {
		    throw new RuntimeException(e);
		}
	    }
	    return size;
	}
    }
}
