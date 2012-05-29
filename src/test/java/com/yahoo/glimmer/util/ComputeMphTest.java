package com.yahoo.glimmer.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

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
import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.glimmer.util.ComputeMphTool;

public class ComputeMphTest {
	private static final String SOME_LINES = 
			"a\n" +
			"b\n" +
			"cc\n" +
			"d\n";
	
	private Mockery context;
	private FileSystem fs;
	private Path inPath;
	private FileStatus inPathStatus;
	private Path outPath;
	private Path infoPath;
	
	@Before
	public void before() throws IOException {
		context = new Mockery();
		context.setImposteriser(ClassImposteriser.INSTANCE);
		
		fs = context.mock(FileSystem.class);
		inPathStatus = context.mock(FileStatus.class, "inPathStatus"); 
		
		inPath = new Path("filename");
		outPath = new Path("filename.mph");
		infoPath = new Path("filename.mph.info");
	}
	
	@Test
	public void test() throws IOException, ClassNotFoundException {
		final ByteArrayOutputStream outStream = new ByteArrayOutputStream(4096);
		final ByteArrayOutputStream infoStream = new ByteArrayOutputStream(4096);
		context.checking(new Expectations() {{
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
				public void describeTo(Description arg0) {}

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
		}});
		ComputeMphTool computeMph = new ComputeMphTool();
		long hashSize = computeMph.buildMph(fs, "filename");
		
		assertEquals(4, hashSize);
		context.assertIsSatisfied();

		assertEquals("size\t4\nbits\t360\n", infoStream.toString());
		
		// unmarshal the hash and check the values..
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(outStream.toByteArray()));
		Object readObject = ois.readObject();
		assertTrue(readObject instanceof LcpMonotoneMinimalPerfectHashFunction);
		@SuppressWarnings("unchecked")
		LcpMonotoneMinimalPerfectHashFunction<MutableString> mph = (LcpMonotoneMinimalPerfectHashFunction<MutableString>)readObject;

		assertEquals(0, (long)mph.get("a"));
		assertEquals(1, (long)mph.get("b"));
		assertEquals(2, (long)mph.get("cc"));
		assertEquals(3, (long)mph.get("d"));
		
	}
	
	/**
	 * The constructor of FSDataInputStream expects a InputStream which is also Seekable and PositionedReadable.
	 * Generating the hash function should only read the input sequentially, although it may do this more than once.
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
		public void readFully(long position, byte[] buffer, int offset,	int length) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void readFully(long position, byte[] buffer) throws IOException {
			throw new UnsupportedOperationException();
		}
	}
}
