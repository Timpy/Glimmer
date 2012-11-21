package com.yahoo.glimmer.util;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class BySubjectRecord {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final char RECORD_DELIMITER = '\n';
    private static final char FIELD_DELIMITER = '\t';
    private static final int MAX_RELATIONS = 10000;

    private int id;
    /**
     * Because the doc id's have to line up with what is in the 'all resources'
     * hash(to avoid having a separate 'subjects' hash), the id's don't run
     * consecutively. This is a problem when we split the bySubjects file for
     * the collection builder. We can't assume the first record in the split is
     * 0, as it may be preceded by empty docs. The previousId is used to keep
     * the doc ids consistent when setting the first doc id in a split.
     */
    private int previousId = -1;
    private String subject;
    private final ArrayList<String> relations = new ArrayList<String>();
    
    private transient StringBuilder sb;

    
    public static class BySubjectRecordParseException extends Exception {
	private static final long serialVersionUID = 421747997614595011L;

	public BySubjectRecordParseException(String message) {
	    super(message);
	}

	public BySubjectRecordParseException(NumberFormatException e) {
	    super(e);
	}
    }

    public boolean parse(final byte[] bytes, final int start, final int end) {
	return parse(bytes, start, end, CHARSET);
    }

    public boolean parse(final byte[] bytes, final int start, final int end, final Charset charset) {
	ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes, start, end);
	try {
	    return parse(new InputStreamReader(inputStream, charset));
	} catch (IOException e) {
	    // This shouldn't happen reading from a ByteArrayInputStream.
	    throw new RuntimeException(e);
	}
    }
    
    public boolean parse(Reader reader) throws IOException {
	if (sb == null) {
	    sb = new StringBuilder();
	}
	return parse(reader, sb);
    }
    public boolean parse(Reader reader, StringBuilder sb) throws IOException {
	readUntil(reader, sb, FIELD_DELIMITER);
	try {
	    id = Integer.parseInt(sb.toString());
	} catch (NumberFormatException e) {
	    return false;
	}
	
	readUntil(reader, sb, FIELD_DELIMITER);
	try {
	    previousId = Integer.parseInt(sb.toString());
	} catch (NumberFormatException e) {
	    return false;
	}
	
	return parseContent(reader, sb);
    }
    
    public boolean parseContent(Reader reader) throws IOException {
	if (sb == null) {
	    sb = new StringBuilder();
	}
	return parseContent(reader, sb);
    }
    public boolean parseContent(Reader reader, StringBuilder sb) throws IOException {
	readUntil(reader, sb, FIELD_DELIMITER);
	subject = sb.toString();

	relations.clear();

	while (readUntil(reader, sb, FIELD_DELIMITER)) {
	    if (sb.length() > 0)
	    relations.add(sb.toString());
	}

	return true;
    }
    
    private static boolean readUntil(final Reader reader, final StringBuilder sb, final char stopChar) throws IOException {
	sb.setLength(0);
	int c;
	while ((c = reader.read()) != stopChar) {
	    if (c == -1) {
		return false;
	    }
	    sb.append((char)c);
	}
	return true;
    }

    public int getId() {
	return id;
    }

    public void setId(int id) {
	this.id = id;
    }

    public int getPreviousId() {
	return previousId;
    }

    public void setPreviousId(int previousId) {
	this.previousId = previousId;
    }

    public String getSubject() {
	return subject;
    }

    public void setSubject(String subject) {
	this.subject = subject;
    }

    public Iterable<String> getRelations() {
	return relations;
    }

    public Reader getRelationsReader() {
	return new Reader() {
	    private int relationsIndex;
	    private char[] relationChars = new char[4096];
	    private int relationCharsLength;
	    private int relationIndex;

	    @Override
	    public void close() throws IOException {
	    }

	    @Override
	    public int read(final char[] buffer, final int startIndex, final int len) throws IOException {
		int bufferIndex = startIndex;

		while (true) {
		    if (relationCharsLength == 0) {
			if (relations.size() <= relationsIndex) {
			    // No more relations. EOF or number of chars copied.
			    return bufferIndex == startIndex ? -1 : bufferIndex - startIndex;
			}

			String relationsString = relations.get(relationsIndex++);
			relationCharsLength = relationsString.length();
			if (relationChars.length < relationCharsLength + 2) {
			    // Allocate a bigger array.
			    relationChars = new char[relationCharsLength + 4096];
			}
			relationsString.getChars(0, relationCharsLength, relationChars, 0);
			
			relationChars[relationCharsLength++] = FIELD_DELIMITER;
			relationIndex = 0;
		    }

		    int remainingBufferChars = startIndex + len - bufferIndex;
		    int remainingRelationChars = relationCharsLength - relationIndex;

		    if (remainingBufferChars > remainingRelationChars) {
			// Write rest of relation chars and move to next
			// relation.
			System.arraycopy(relationChars, relationIndex, buffer, bufferIndex, remainingRelationChars);
			bufferIndex += remainingRelationChars;
			relationCharsLength = 0;
		    } else {
			// Fill the buffer with current relation
			System.arraycopy(relationChars, relationIndex, buffer, bufferIndex, remainingBufferChars);
			if (remainingBufferChars == remainingRelationChars) {
			    relationCharsLength = 0;
			} else {
			    relationIndex += remainingBufferChars;
			}
			return bufferIndex - startIndex;
		    }
		}
	    }
	};
    }

    public int getRelationsCount() {
	return relations.size();
    }

    public boolean hasRelations() {
	return !relations.isEmpty();
    }

    public boolean addRelation(String relation) {
	if (relations.size() > MAX_RELATIONS) {
	    return false;
	}
	relations.add(relation);
	return true;
    }

    public void clearRelations() {
	relations.clear();
    }

    public void writeTo(Writer writer) throws IOException {
	writer.write(Integer.toString(id));
	writer.write(FIELD_DELIMITER);
	writer.write(Integer.toString(previousId));
	writer.write(FIELD_DELIMITER);
	if (subject != null) {
	    writer.write(subject);
	}
	writer.write(FIELD_DELIMITER);
	for (String relation : relations) {
	    writer.write(relation);
	    writer.write(FIELD_DELIMITER);
	}
	writer.write(RECORD_DELIMITER);
    }

    @Override
    public String toString() {
	StringWriter stringWriter = new StringWriter(4096);
	try {
	    writeTo(stringWriter);
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return stringWriter.toString();
    }

    @Override
    public boolean equals(Object object) {
	if (object instanceof BySubjectRecord) {
	    BySubjectRecord that = (BySubjectRecord) object;
	    return id == that.id && previousId == that.previousId && (subject == null ? that.subject == null : subject.equals(that.subject))
		    && relations.equals(that.relations);
	}
	return false;
    }
}
