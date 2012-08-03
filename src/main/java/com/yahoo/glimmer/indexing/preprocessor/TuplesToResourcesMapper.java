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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Maps each input line containing a tuple of 3 or more elements to Key/Value
 * pairs of the following form KEY VALUE "subject"
 * "&lt;predicate&gt; &lt;object&gt; &lt;context&gt; ." "predicate			"PREDICATE"
 * "object" "OBJECT" "context" "CONTEXT"
 * 
 * If the object is a literal no key/value with a value of "OBJECT" is written.
 * 
 * Eg. for the tuple
 * "&lt;http://subject/&gt; &lt;http://predicate/&gt; &lt;http://object/&gt; &lt;http://context/&gt; ."
 * 
 * KEY VALUE http://subject/ &lt;http://predicate/&gt; &lt;http://object/&gt;
 * &lt;http://context/&gt; . http://predicate/ PREDICATE http://object/ OBJECT
 * http://context/ CONTEXT
 * 
 */
public class TuplesToResourcesMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(TuplesToResourcesMapper.class);
    private static final int MAX_NODES = 5; // Our Any23 extractions include a 5
					    // Literal which is the extractor
					    // used.

    public static final String INCLUDE_CONTEXTS_KEY = "includeContexts";
    public static final String SUBJECT_REGEX_KEY = "subjectRegex";
    public static final String PREDICATE_REGEX_KEY = "predicateRegex";
    public static final String OBJECT_REGEX_KEY = "objectRegex";
    public static final String CONTEXT_REGEX_KEY = "contextRegex";
    public static final String FILTER_CONJUNCTION_KEY = "andNotOrConjunction";

    public static enum TUPLE_ELEMENTS {
	SUBJECT, PREDICATE, OBJECT, CONTEXT;
    }
    
    public enum Counters {
	TOO_MANY_RELATIONS, LONG_TUPLES, LONG_TUPLE_ELEMENT;
    }

    private boolean includeContexts = true;
    private StringBuilder predicateObjectContextDot = new StringBuilder();
    private String[] nodesAsN3 = new String[MAX_NODES];
    private Pattern[] patterns = new Pattern[MAX_NODES];
    private boolean andNotOrConjunction; // Default is OR

    private InputSplit lastInputSplit;

    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws java.io.IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	boolean includeContexts = conf.getBoolean(INCLUDE_CONTEXTS_KEY, true);
	setIncludeContexts(includeContexts);

	setSubjectRegex(conf.get(SUBJECT_REGEX_KEY));
	setPredicateRegex(conf.get(PREDICATE_REGEX_KEY));
	setObjectRegex(conf.get(OBJECT_REGEX_KEY));
	setContextRegex(conf.get(CONTEXT_REGEX_KEY));
	setAndNotOrConjunction(conf.getBoolean(FILTER_CONJUNCTION_KEY, andNotOrConjunction));
    };

    public void setIncludeContexts(boolean includeContexts) {
	this.includeContexts = includeContexts;
    }

    public void setSubjectRegex(String regex) {
	patterns[TUPLE_ELEMENTS.SUBJECT.ordinal()] = checkRegex(regex);
    }

    public void setPredicateRegex(String regex) {
	patterns[TUPLE_ELEMENTS.PREDICATE.ordinal()] = checkRegex(regex);
    }

    public void setObjectRegex(String regex) {
	patterns[TUPLE_ELEMENTS.OBJECT.ordinal()] = checkRegex(regex);
    }

    public void setContextRegex(String regex) {
	patterns[TUPLE_ELEMENTS.CONTEXT.ordinal()] = checkRegex(regex);
    }

    private static Pattern checkRegex(String regex) {
	if (regex == null || regex.isEmpty()) {
	    return null;
	}
	return Pattern.compile(regex);
    }

    public void setAndNotOrConjunction(boolean andNotOrConjunction) {
	this.andNotOrConjunction = andNotOrConjunction;
    }

    @Override
    protected void map(LongWritable key, Text valueText, Mapper<LongWritable, Text, Text, Text>.Context context) throws java.io.IOException,
	    InterruptedException {

	if (!context.getInputSplit().equals(lastInputSplit)) {
	    lastInputSplit = context.getInputSplit();
	    if (lastInputSplit instanceof FileSplit) {
		FileSplit fileSplit = (FileSplit) lastInputSplit;
		LOG.info("Current FileSplit " + fileSplit.getPath().toString() + " start(length) bytes " + fileSplit.getStart() + "(" + fileSplit.getLength()
			+ ")");
	    } else {
		LOG.info("Current InputSplit " + lastInputSplit.toString());
	    }
	}

	String value = valueText.toString().trim();
	if (value.isEmpty()) {
	    return;
	}
	Node[] nodes;
	try {
	    nodes = NxParser.parseNodes(value);
	} catch (ParseException e) {
	    // NxParser 1.2.2 has problems with typed literals like:
	    // "27"^^<int uri>. This is fixed in 1.2.3
	    context.getCounter(MapCounters.NX_PARSER_EXCEPTION).increment(1l);
	    String s = value.replaceAll("\\^\\^<[^>]+>", "");
	    try {
		nodes = NxParser.parseNodes(s);
		LOG.info("Only parsed after remove of literal types:" + value);
	    } catch (ParseException e1) {
		context.getCounter(MapCounters.NX_PARSER_RETRY_EXCEPTION).increment(1l);
		LOG.info("Failed parsing even after remove of literal types:" + value);
		return;
	    }
	}

	if (nodes.length < 3) {
	    context.getCounter(MapCounters.SHORT_TUPLE).increment(1l);
	    LOG.info("Line parsed with less than 3 nodes at position" + key.toString());
	    return;
	}
	if (nodes.length > MAX_NODES) {
	    context.getCounter(MapCounters.LONG_TUPLE).increment(1l);
	    LOG.info("Line parsed with more than " + MAX_NODES + " nodes at position" + key.toString());
	    return;
	}

	int nodeCount = 0;

	// Skip relations that have long elements or don't match the given patterns
	int patternsTried = 0;
	int patternsMatched = 0;
	for (Node node : nodes) {
	    String n3 = node.toN3();
	    
	    if (n3.length() > 5000) {
		String elementName;
		if (nodeCount < TUPLE_ELEMENTS.values().length) {
		    elementName = TUPLE_ELEMENTS.values()[nodeCount].name();
		} else {
		    elementName = Integer.toString(nodeCount);
		}
		System.out.println("Long tuple element " + elementName + ". Length:" + n3.length() + " starting with " + n3.substring(0, 100));
		context.getCounter(Counters.LONG_TUPLE_ELEMENT).increment(1);
		return;
	    }

	    if (patterns[nodeCount] != null) {
		Matcher matcher = patterns[nodeCount].matcher(n3);
		if (matcher.find()) {
		    patternsMatched++;
		}
		patternsTried++;
	    }
	    nodesAsN3[nodeCount++] = n3;
	}

	if (andNotOrConjunction) {
	    // AND. tried should equal matched
	    if (patternsTried != patternsMatched) {
		return;
	    }
	} else {
	    // OR. If any patterns where tried at least one should have matched.
	    if (patternsTried > 0 && patternsMatched == 0) {
		return;
	    }
	}

	for (Node node : nodes) {
	    if (node instanceof Resource) {
		try {
		    new URI(node.toString());
		} catch (URISyntaxException e) {
		    context.getCounter(MapCounters.INVALID_RESOURCE).increment(1l);
		    LOG.info("Bad resource near position " + key.toString());
		    return;
		}
	    }
	}

	predicateObjectContextDot.setLength(0);

	Node node = nodes[TUPLE_ELEMENTS.SUBJECT.ordinal()];
	assert node instanceof org.semanticweb.yars.nx.Resource;
	Text subject = new Text(node.toString());

	node = nodes[TUPLE_ELEMENTS.PREDICATE.ordinal()];
	assert node instanceof org.semanticweb.yars.nx.Resource;
	context.write(new Text(node.toString()), new Text(TUPLE_ELEMENTS.PREDICATE.name()));
	predicateObjectContextDot.append(nodesAsN3[TUPLE_ELEMENTS.PREDICATE.ordinal()]);

	node = nodes[TUPLE_ELEMENTS.OBJECT.ordinal()];
	if (node instanceof org.semanticweb.yars.nx.Resource) {
	    context.write(new Text(node.toString()), new Text(TUPLE_ELEMENTS.OBJECT.name()));
	}
	predicateObjectContextDot.append(' ');
	predicateObjectContextDot.append(nodesAsN3[TUPLE_ELEMENTS.OBJECT.ordinal()]);

	if (includeContexts && nodeCount > TUPLE_ELEMENTS.CONTEXT.ordinal()) {
	    node = nodes[TUPLE_ELEMENTS.CONTEXT.ordinal()];
	    assert node instanceof org.semanticweb.yars.nx.Resource;
	    context.write(new Text(node.toString()), new Text(TUPLE_ELEMENTS.CONTEXT.name()));
	    predicateObjectContextDot.append(' ');
	    predicateObjectContextDot.append(nodesAsN3[TUPLE_ELEMENTS.CONTEXT.ordinal()]);
	}
	predicateObjectContextDot.append(" .");

	if (predicateObjectContextDot.length() > 10000) {
	    System.out.println("Long tuple. Length:" + predicateObjectContextDot.length() + " starting with " + predicateObjectContextDot.substring(0, 100));
	    context.getCounter(Counters.LONG_TUPLES).increment(1);
	} else {
	    // Write subject with object, predicate, object, context as value
	    context.write(subject, new Text(predicateObjectContextDot.toString()));
	}
    };

    static enum MapCounters {
	NX_PARSER_EXCEPTION, NX_PARSER_RETRY_EXCEPTION, LONG_TUPLE, SHORT_TUPLE, INVALID_RESOURCE
    }
}
