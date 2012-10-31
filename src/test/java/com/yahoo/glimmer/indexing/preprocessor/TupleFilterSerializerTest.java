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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

public class TupleFilterSerializerTest {

    @Test(expected = IOException.class)
    public void emptyInputTest() throws IOException {
	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("".getBytes());
	TupleFilterSerializer.deserialize(byteArrayInputStream);
    }

    @Test
    public void emptyFilterTest() throws IOException {
	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("<com.yahoo.glimmer.indexing.preprocessor.RegexTupleFilter/>".getBytes());
	TupleFilter filter = TupleFilterSerializer.deserialize(byteArrayInputStream);
	assertTrue(filter instanceof RegexTupleFilter);
    }

    @Test
    public void regexFilterTest() throws IOException {
	ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(("<com.yahoo.glimmer.indexing.preprocessor.RegexTupleFilter>"
		+ "<subjectPattern><pattern>subjectA</pattern><flags>0</flags></subjectPattern>"
		+ "<objectPattern><pattern>http(s)+://(host|localhost)/objectB</pattern><flags>0</flags></objectPattern>"
		+ "<andNotOrConjunction>true</andNotOrConjunction>"
		+ "</com.yahoo.glimmer.indexing.preprocessor.RegexTupleFilter>").getBytes());
	TupleFilter filter = TupleFilterSerializer.deserialize(byteArrayInputStream);
	assertTrue(filter instanceof RegexTupleFilter);
	RegexTupleFilter regexTupleFilter = (RegexTupleFilter) filter;
	assertEquals("subjectA", regexTupleFilter.getSubjectPattern().toString());
	assertEquals("http(s)+://(host|localhost)/objectB", regexTupleFilter.getObjectPattern().toString());
	assertTrue(regexTupleFilter.isAndNotOrConjunction());
    }
}
