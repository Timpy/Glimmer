package com.yahoo.glimmer.indexing.preprocessor;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;


/**
 * Maps each input line containing a tuple of 3 or more elements to
 * Key/Value pairs of the following form
 * KEY				VALUE
 * "subject"			"&lt;predicate&gt; &lt;object&gt; &lt;context&gt; ."
 * "predicate			"PREDICATE"
 * "object"			"OBJECT"
 * "context"			"CONTEXT"
 * 
 * If the object is a literal no key/value with a value of "OBJECT" is written.
 * 
 * Eg. for the tuple "&lt;http://subject/&gt; &lt;http://predicate/&gt; &lt;http://object/&gt; &lt;http://context/&gt; ."
 * 
 * KEY				VALUE
 * http://subject/		&lt;http://predicate/&gt; &lt;http://object/&gt; &lt;http://context/&gt; .
 * http://predicate/		PREDICATE
 * http://object/		OBJECT
 * http://context/		CONTEXT
 * 
 */
public class TuplesToResourcesMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(TuplesToResourcesMapper.class);
    private static final int SUBJECT_IDX = 0;
    private static final int PREDICATE_IDX = 1;
    private static final int OBJECT_IDX = 2;
    private static final int CONTEXT_IDX = 3;

    public static final String INCLUDE_CONTEXTS_KEY = "includeContexts";
    public static final String PREDICATE_VALUE = "PREDICATE";
    public static final String OBJECT_VALUE = "OBJECT";
    public static final String CONTEXT_VALUE = "CONTEXT";

    private boolean includeContexts = true;
    private StringBuilder relations = new StringBuilder();
    
    protected void setup(Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
	String includeContextsString = context.getConfiguration().get(INCLUDE_CONTEXTS_KEY);
	assert includeContextsString != null;
	setIncludeContexts(Boolean.parseBoolean(includeContextsString));
    };

    public void setIncludeContexts(boolean includeContexts) {
	this.includeContexts = includeContexts;
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws java.io.IOException, InterruptedException {
	Node[] nodes;
	try {
	    nodes = NxParser.parseNodes(value.toString());
	} catch (ParseException e) {
	    // NxParser has problems with typed literals like: "27"^^<int uri>
	    context.getCounter(MapCounters.NX_PARSER_EXCEPTION).increment(1l);
	    LOG.info("Failed parsing at postion:" + key.toString());
	    String s = value.toString().replaceAll("\\^\\^<[^>]+>", "");
	    try {
		nodes = NxParser.parseNodes(s);
	    } catch (ParseException e1) {
		context.getCounter(MapCounters.NX_PARSER_RETRY_EXCEPTION).increment(1l);
		LOG.info("Failed parsing retry after remove of literal type:" + s);
		// throw new IOException("Map input at:" + key.toString(), e);
		return;
	    }
	}

	if (nodes.length < 3) {
	    context.getCounter(MapCounters.SHORT_TUPLE).increment(1l);
	    LOG.info("Line parsed with less than 3 nodes at position" + key.toString());
	    return;
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

	relations.setLength(0);
	Text subject = null;

	Node node = nodes[SUBJECT_IDX];
	String nodeN3 = node.toN3();
	assert node instanceof org.semanticweb.yars.nx.Resource;
	subject = new Text(node.toString());
	relations.append(nodeN3);

	node = nodes[PREDICATE_IDX];
	nodeN3 = node.toN3();
	assert node instanceof org.semanticweb.yars.nx.Resource;
	context.write(new Text(node.toString()), new Text(PREDICATE_VALUE));
	relations.append(' ');
	relations.append(nodeN3);

	node = nodes[OBJECT_IDX];
	nodeN3 = node.toN3();
	if (node instanceof org.semanticweb.yars.nx.Resource) {
	    context.write(new Text(node.toString()), new Text(OBJECT_VALUE));
	}
	relations.append(' ');
	relations.append(nodeN3);

	if (includeContexts && nodes.length > CONTEXT_IDX) {
	    node = nodes[CONTEXT_IDX];
	    nodeN3 = node.toN3();
	    assert node instanceof org.semanticweb.yars.nx.Resource;
	    context.write(new Text(node.toString()), new Text(CONTEXT_VALUE));
	    relations.append(' ');
	    relations.append(nodeN3);
	}
	relations.append(" .");

	// Write subject with object, predicate, object, context as value
	context.write(subject, new Text(relations.toString()));
    };

    static enum MapCounters {
	NX_PARSER_EXCEPTION, NX_PARSER_RETRY_EXCEPTION, SHORT_TUPLE, INVALID_RESOURCE;
    }
}
