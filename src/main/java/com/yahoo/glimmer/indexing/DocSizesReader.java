package com.yahoo.glimmer.indexing;

import java.io.IOException;

import it.unimi.dsi.io.InputBitStream;

/** Simple utility for dumping the sizes encoded in a .sizes file
 * 
 *  Reads from standard input and writes to standard output.
 * 
 * @author pmika
 *
 */
public class DocSizesReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		InputBitStream stream = new InputBitStream(System.in);
		while (stream.hasNext()) {
			System.out.println(stream.readGamma());
		}
	}

}
