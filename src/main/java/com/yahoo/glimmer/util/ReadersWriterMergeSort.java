package com.yahoo.glimmer.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReadersWriterMergeSort {
    public static int mergeSort(List<BufferedReader> sourceReaders, Writer writer) throws IOException {
	ReaderLineList sources = new ReaderLineList(sourceReaders.size());
	for (BufferedReader reader : sourceReaders) {
	    ReaderLine readerLine = new ReaderLine(reader);
	    sources.insert(readerLine);
	}

	int count = 0;
	String line;
	while ((line = sources.readLine()) != null) {
	    writer.write(line);
	    writer.write('\n');
	    count++;
	}

	return count;
    }

    private static class ReaderLineList extends ArrayList<ReaderLine> {
	private static final long serialVersionUID = 3646929636092521798L;

	public ReaderLineList(int size) {
	    super(size);
	}

	public void insert(ReaderLine toInsert) {
	    if (toInsert.getLine() == null) {
		return;
	    }
	    int insertIndex = Collections.binarySearch(this, toInsert);
	    if (insertIndex < 0) {
		add(-insertIndex -1, toInsert);
	    } else {
		throw new IllegalStateException("Duplicate line in input:" + toInsert.getLine());
	    }
	}

	public String readLine() throws IOException {
	    if (isEmpty()) {
		return null;
	    }
	    ReaderLine readerLine = remove(size() - 1);
	    String line = readerLine.getLine();
	    if (readerLine.nextLine()) {
		insert(readerLine);
	    }
	    return line;
	}
    }

    private static class ReaderLine implements Comparable<ReaderLine> {
	private final BufferedReader reader;
	private String line;

	public ReaderLine(BufferedReader reader) throws IOException {
	    this.reader = reader;
	    line = reader.readLine();
	}

	@Override
	public int compareTo(ReaderLine that) {
	    return -this.line.compareTo(that.line);
	}

	public String getLine() {
	    return line;
	}

	public boolean nextLine() throws IOException {
	    if (line != null) {
		line = reader.readLine();
		if (line != null) {
		    return true;
		}
	    }
	    return false;
	}
    }
}
