package com.yahoo.glimmer.util;

import it.unimi.dsi.big.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
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
import java.nio.charset.Charset;
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
import org.apache.log4j.Logger;

import cern.colt.Arrays;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import com.martiansoftware.jsap.stringparsers.ForNameStringParser;
import com.martiansoftware.jsap.stringparsers.IntegerStringParser;

public class ComputeMphTool extends Configured implements Tool {
    private final static Logger LOGGER = Logger.getLogger(ComputeMphTool.class);
    private static final String SRC_FILES_ARG = "srcFilenames";
    private static final String SIGNED_ARG = "signed";
    private static final String SIGNATURE_WIDTH_ARG = "signatureWidth";
    private static final String FILE_ENCODING_ARG = "encoding";
    public static final FsPermission ALL_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    @Override
    public int run(String[] args) throws Exception {
	final SimpleJSAP jsap = new SimpleJSAP(HashLookup.class.getName(), "Lookup hash value for 'cleaned' URI", new Parameter[] {
	    	new Switch(SIGNED_ARG, 's', SIGNED_ARG, "Sign the hashes."),
		new FlaggedOption(SIGNATURE_WIDTH_ARG, IntegerStringParser.getParser() , "32", JSAP.NOT_REQUIRED, 'w', "width", "Sign the hash with a hash width of w bits."),
		new FlaggedOption(FILE_ENCODING_ARG, ForNameStringParser.getParser(Charset.class) , "UTF-8", JSAP.NOT_REQUIRED, 'e', "encoding", "Set the input file encoding(default is UTF-8)."),
		new UnflaggedOption(SRC_FILES_ARG, JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY,
			"The filenames (or HDFS dirs if building hashes) to work with.") });

	JSAPResult jsapResult = jsap.parse(args);
	if (jsap.messagePrinted()) {
	    throw new IllegalArgumentException("");
	}

	String[] srcFilenames = jsapResult.getStringArray(SRC_FILES_ARG);
	Charset srcFileCharset = (Charset) jsapResult.getObject(FILE_ENCODING_ARG);
	int signatureWidth = 0;
	if (jsapResult.getBoolean(SIGNED_ARG)) {
	    signatureWidth = jsapResult.getInt(SIGNATURE_WIDTH_ARG);
	    LOGGER.info("Building signed hash with signature width of " + signatureWidth + " bits for " + srcFileCharset.displayName() + " files:" + Arrays.toString(srcFilenames));
	} else {
	    LOGGER.info("Building unsigned hash for " + srcFileCharset.displayName() + " files:" + srcFilenames);
	}
	
	
	JobConf job = new JobConf(getConf(), ComputeMphTool.class);
	FileSystem fs = FileSystem.get(job);
	for (String filename : srcFilenames) {
	    LOGGER.info("Building hash of " + filename);
	    buildHash(fs, filename, signatureWidth, srcFileCharset);
	}
	return 0;
    }

    public int buildHash(final FileSystem fs, final String srcFilename, final int signatureWidth, final Charset charset) throws IOException {
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
		return new InputStreamReader(new SequenceInputStream(inputStreamEnumeration), charset);
	    }
	});

	Object2LongFunction<? extends CharSequence> map = new LcpMonotoneMinimalPerfectHashFunction<CharSequence>(inCollection,
		TransformationStrategies.prefixFreeUtf16());
	String filenameExtention = ".map";
	if (signatureWidth > 0) {
	    map = new ShiftAddXorSignedStringMap(inCollection.iterator(), map, signatureWidth);
	    filenameExtention = ".smap";
	}

	String destFilename = inputStreamEnumeration.removeCompressionSuffixIfAny(srcFilename);
	Path outPath = new Path(destFilename + filenameExtention);
	FSDataOutputStream outStream = fs.create(outPath, true);// overwrite
	fs.setPermission(outPath, ALL_PERMISSIONS);
	ObjectOutputStream outOs = new ObjectOutputStream(outStream);
	outOs.writeObject(map);
	outOs.close();

	Path infoPath = new Path(destFilename + filenameExtention + ".info");
	FSDataOutputStream infoStream = fs.create(infoPath, true);// overwrite
	fs.setPermission(infoPath, ALL_PERMISSIONS);
	OutputStreamWriter infoWriter = new OutputStreamWriter(infoStream);
	infoWriter.write("size\t");
	infoWriter.write(Integer.toString(map.size()));
	infoWriter.write("\n");
	if (map instanceof LcpMonotoneMinimalPerfectHashFunction<?>) {
        	infoWriter.write("bits\t");
        	infoWriter.write(Long.toString(((LcpMonotoneMinimalPerfectHashFunction<?>)map).numBits()));
        	infoWriter.write("\n");
	}
	infoWriter.close();
	infoStream.close();

	return map.size();
    }

    /**
     * For static invocation from pig via define mph InvokeForInt(
     * 'com.yahoo.glimmer.util.ComputeMph.pigInvoker', filename, signed); The hash
     * function itself will be serialized to DFS as filename.<s>map
     * 
     * TODO Test it.
     * 
     * @param inFilename
     * @return size of generated hash
     * @throws IOException
     */
    public static int pigInvoker(String filename, int signatureWidth, String charsetName) throws IOException {
	return new ComputeMphTool().buildHash(FileSystem.get(null), filename, signatureWidth, Charset.forName(charsetName));
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
