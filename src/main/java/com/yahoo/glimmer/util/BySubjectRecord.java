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

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;

public class BySubjectRecord {
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private static final char RECORD_DELIMITER = '\n';
    private static final char FIELD_DELIMITER = '\t';
    private static final String RELATION_DELIMITER = "  ";
    private static final int MAX_RELATIONS = 10000;

    private int id;
    private String subject;
    private final ArrayList<String> relations = new ArrayList<String>();
    
    public boolean parse(Text text) {
	return parse(text.getBytes(), 0, text.getLength());
    }
    public boolean parse(final byte[] bytes, final int start, final int length) {
	return parse(bytes, start, length, DEFAULT_CHARSET);
    }
    public boolean parse(final byte[] bytes, final int start, final int length, final Charset charset) {
	int end = start + length;
	
	int startOfIdIndex = start;
	while (startOfIdIndex < end && Character.isWhitespace(bytes[startOfIdIndex])) { startOfIdIndex++; }
	if (startOfIdIndex >= end) {
	    return false;
	}
	
	int endOfIdIndex = startOfIdIndex;
	while (endOfIdIndex < end && Character.isDigit(bytes[endOfIdIndex])) { endOfIdIndex++; };
	if (endOfIdIndex >= end || endOfIdIndex == startOfIdIndex) {
	    return false;
	}
	id = Integer.parseInt(new String(bytes, startOfIdIndex, endOfIdIndex - startOfIdIndex, charset));
	
	while (endOfIdIndex < end && bytes[endOfIdIndex] != FIELD_DELIMITER) { endOfIdIndex++; }
	if (endOfIdIndex >= end) {
	    return false;
	}
	
	int startOfSubjectIdx = endOfIdIndex + 1;
	while (startOfSubjectIdx < end && Character.isWhitespace(bytes[startOfSubjectIdx])) { startOfSubjectIdx++; }
	if (startOfSubjectIdx >= end) {
	    return false;
	}

	int endOfSubjectIndex = startOfSubjectIdx;
	while (endOfSubjectIndex < end && !Character.isWhitespace(bytes[endOfSubjectIndex])) { endOfSubjectIndex++; }
	if (endOfSubjectIndex >= end || endOfSubjectIndex == startOfSubjectIdx) {
	    return false;
	}
	subject = new String(bytes, startOfSubjectIdx, endOfSubjectIndex - startOfSubjectIdx, DEFAULT_CHARSET);
	
	while (endOfSubjectIndex < end && bytes[endOfSubjectIndex] != FIELD_DELIMITER) { endOfSubjectIndex++; }
	if (endOfSubjectIndex >= end) {
	    return false;
	}

	int startOfRelationIndex = endOfSubjectIndex + 1;
	int endOfRelationIndex = startOfRelationIndex;
	
	relations.clear();
	
	do {
	    int delimiterIndex = 0;
	    while (endOfRelationIndex < end) {
		if (bytes[endOfRelationIndex] == RELATION_DELIMITER.charAt(delimiterIndex)) {
		    endOfRelationIndex++;
		    delimiterIndex++;
		    if (delimiterIndex >= RELATION_DELIMITER.length()) {
			// RELATION_DELIMITER match.
			int relationLength = endOfRelationIndex - startOfRelationIndex - RELATION_DELIMITER.length();
			if (relationLength > 0) {
			    relations.add(new String(bytes, startOfRelationIndex, relationLength, DEFAULT_CHARSET));
			}
			break;
		    }
		} else if (bytes[endOfRelationIndex] == RECORD_DELIMITER) {
		    if (startOfRelationIndex < endOfRelationIndex) {
			relations.add(new String(bytes, startOfRelationIndex, endOfRelationIndex - startOfRelationIndex, DEFAULT_CHARSET));
		    }
		    endOfRelationIndex++;
		    break;
		} else if (endOfRelationIndex == end - 1) {
		    endOfRelationIndex++;
		    if (startOfRelationIndex < endOfRelationIndex) {
			relations.add(new String(bytes, startOfRelationIndex, endOfRelationIndex - startOfRelationIndex, DEFAULT_CHARSET));
		    }
		    break;
		} else {
		    endOfRelationIndex++;
		    delimiterIndex = 0;
		}
	    }
	    startOfRelationIndex = endOfRelationIndex;
	} while (endOfRelationIndex < end);

	return true;
    }

    public int getId() {
	return id;
    }

    public void setId(int id) {
	this.id = id;
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
			if (relationsIndex < relations.size()) {
			    // Append RELATION_DELIMITER
			    RELATION_DELIMITER.getChars(0, RELATION_DELIMITER.length(), relationChars, relationCharsLength);
			    relationCharsLength += RELATION_DELIMITER.length();
			}
			relationIndex = 0;
		    }
		    
		    int remainingBufferChars = startIndex + len - bufferIndex;
		    int remainingRelationChars = relationCharsLength - relationIndex;
		    
		    if (remainingBufferChars > remainingRelationChars) {
			// Write rest of relation chars and move to next relation.
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
	if (subject != null) {
	    writer.write(subject);
	}
	writer.write(FIELD_DELIMITER);
	boolean first = true;
	for (String relation : relations) {
	    if (first) {
		first = false;
	    } else {
		writer.write(RELATION_DELIMITER);
	    }
	    writer.write(relation);
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
	    return id == that.id && (subject == null ? that.subject == null : subject.equals(that.subject)) && relations.equals(that.relations);
	}
        return false;
    }
}
