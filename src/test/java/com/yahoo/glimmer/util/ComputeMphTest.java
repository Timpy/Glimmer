package com.yahoo.glimmer.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.big.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

public class ComputeMphTest {
    private static final String SOME_LINES = "a\nb\ncc\nd\n\u2200";

    private Mockery context;
    private Expectations expectations;
    private FileSystem fs;
    private PathMatcher outPath = new PathMatcher();
    private PathMatcher infoPath = new PathMatcher();
    private ByteArrayOutputStream outStream;
    private ByteArrayOutputStream infoStream;
    private ComputeMphTool computeMph;

    @Before
    public void before() throws IOException {
	context = new Mockery();
	context.setImposteriser(ClassImposteriser.INSTANCE);

	fs = context.mock(FileSystem.class);
	final FileStatus inPathStatus = context.mock(FileStatus.class, "inPathStatus");

	
	final Path inPath = new Path("filename");
	outPath = new PathMatcher();
	infoPath = new PathMatcher();
	
	outStream = new ByteArrayOutputStream(4096);
	infoStream = new ByteArrayOutputStream(4096);
	expectations = new Expectations() {{
		one(fs).getFileStatus(with(inPath));
		will(returnValue(inPathStatus));
		one(inPathStatus).isDir();
		will(returnValue(false));
		one(fs).getConf();
		will(returnValue(new Configuration()));
		allowing(inPathStatus).getPath();
		will(returnValue(inPath));
		allowing(fs).open(with(inPath));
		will(new Action() {
		    @Override
		    public void describeTo(Description arg0) {
		    }

		    @Override
		    public Object invoke(Invocation invocation) throws Throwable {
			ByteArrayInputStream in = new SeekablePositionedReadableByteArrayInputStream(SOME_LINES.getBytes());
			return new FSDataInputStream(in);
		    }
		});
		oneOf(fs).create(with(outPath), with(true));
		will(returnValue(new FSDataOutputStream(outStream, new Statistics("outStats"))));
		oneOf(fs).setPermission(with(outPath), with(ComputeMphTool.ALL_PERMISSIONS));
		oneOf(fs).create(with(infoPath), with(true));
		will(returnValue(new FSDataOutputStream(infoStream, new Statistics("infoStats"))));
		oneOf(fs).setPermission(with(infoPath), with(ComputeMphTool.ALL_PERMISSIONS));
	}};
	context.checking(expectations);
	computeMph = new ComputeMphTool();
    }

    @Test
    public void unsignedTest() throws IOException, ClassNotFoundException {
	outPath.setFilename("filename.map");
	infoPath.setFilename("filename.map.info");
	long hashSize = computeMph.buildHash(fs, "filename", 0, Charset.forName("UTF-8"));

	assertEquals(5, hashSize);
	context.assertIsSatisfied();

	assertEquals("size\t5\nbits\t391\n", infoStream.toString());

	// unmarshal the hash and check the values..
	ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(outStream.toByteArray()));
	Object readObject = ois.readObject();
	assertTrue(readObject instanceof LcpMonotoneMinimalPerfectHashFunction);
	@SuppressWarnings("unchecked")
	LcpMonotoneMinimalPerfectHashFunction<MutableString> mph = (LcpMonotoneMinimalPerfectHashFunction<MutableString>) readObject;

	assertEquals(0, mph.getLong("a"));
	assertEquals(1, mph.getLong("b"));
	assertEquals(2, mph.getLong("cc"));
	assertEquals(3, mph.getLong("d"));
	assertEquals(4, mph.getLong("\u2200"));
    }

    @Test
    public void signedTest() throws IOException, ClassNotFoundException {
	outPath.setFilename("filename.smap");
	infoPath.setFilename("filename.smap.info");
	long hashSize = computeMph.buildHash(fs, "filename", 32, Charset.forName("UTF-8"));

	assertEquals(5, hashSize);
	context.assertIsSatisfied();

	assertEquals("size\t5\n", infoStream.toString());

	// unmarshal the hash and check the values..
	ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(outStream.toByteArray()));
	Object readObject = ois.readObject();
	assertTrue(readObject instanceof ShiftAddXorSignedStringMap);
	ShiftAddXorSignedStringMap map = (ShiftAddXorSignedStringMap) readObject;

	assertEquals(-1, map.getLong("0"));
	assertEquals(0, map.getLong("a"));
	assertEquals(-1, map.getLong("aa"));
	assertEquals(1, map.getLong("b"));
	assertEquals(-1, map.getLong("bb"));
	assertEquals(2, map.getLong("cc"));
	assertEquals(-1, map.getLong("ca"));
	assertEquals(3, map.getLong("d"));
	assertEquals(-1, map.getLong("dx"));
	assertEquals(-1, map.getLong("\u2199"));
	assertEquals(4, map.getLong("\u2200"));
	assertEquals(-1, map.getLong("\u2201"));
    }
    
    private class PathMatcher extends BaseMatcher<Path> {
	private String filename;
	
	public void setFilename(String filename) {
	    this.filename = filename;
	};
	
	@Override
	public boolean matches(Object o) {
	    return filename.toString().equals(o.toString());
	}

	@Override
	public void describeTo(Description desc) {
	    desc.appendText("Matches a Path instance with a filename of " + filename);
	}
    }

    /**
     * The constructor of FSDataInputStream expects a InputStream which is also
     * Seekable and PositionedReadable. Generating the hash function should only
     * read the input sequentially, although it may do this more than once.
     * 
     * @author tep
     * 
     */
    private class SeekablePositionedReadableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {
	public SeekablePositionedReadableByteArrayInputStream(byte[] buf) {
	    super(buf);
	}

	@Override
	public void seek(long pos) throws IOException {
	    throw new UnsupportedOperationException();
	}

	@Override
	public long getPos() throws IOException {
	    throw new UnsupportedOperationException();
	}

	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
	    throw new UnsupportedOperationException();
	}

	@Override
	public int read(long position, byte[] buffer, int offset, int length) throws IOException {
	    throw new UnsupportedOperationException();
	}

	@Override
	public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
	    throw new UnsupportedOperationException();
	}

	@Override
	public void readFully(long position, byte[] buffer) throws IOException {
	    throw new UnsupportedOperationException();
	}
    }
}
