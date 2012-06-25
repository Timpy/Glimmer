package com.yahoo.glimmer.indexing.preprocessor;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer(and Combiner)
 * 
 * For the given Subject resource key concatanates all <predicate> <object> <context> . for that key 
 * + appends PREDICATE, OBJECT and/or CONTEXT keywords if that keyword occurs once or more as a value.
 *
 */
public class ResourcesReducer extends Reducer<Text, Text, Text, Text> {
    public static final String VALUE_DELIMITER = "  ";
    private boolean keyPredicate;
    private boolean keyObject;
    private boolean keyContext;
    private StringBuilder relations = new StringBuilder();
    private String[] singleSplit = new String[1];

    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	relations.setLength(0);
	for (Text value : values) {
	    String valueString = value.toString();
	    
	    String[] split; 
	    if (valueString.contains(VALUE_DELIMITER)) {
		split = valueString.split(VALUE_DELIMITER);
	    } else {
		singleSplit[0] = valueString;
		split = singleSplit;
	    }
	    for (String s : split) {
		if (TuplesToResourcesMapper.PREDICATE_VALUE.equals(s)) {
		    keyPredicate = true;
		} else if (TuplesToResourcesMapper.OBJECT_VALUE.equals(s)) {
		    keyObject = true;
		} else if (TuplesToResourcesMapper.CONTEXT_VALUE.equals(s)) {
		    keyContext = true;
		} else {
		    prefixDelimiterAppender(s);
		}
	    }
	}
	
	if (keyPredicate) {
	    prefixDelimiterAppender(TuplesToResourcesMapper.PREDICATE_VALUE);
	    keyPredicate = false;
	}
	if (keyObject) {
	    prefixDelimiterAppender(TuplesToResourcesMapper.OBJECT_VALUE);
	    keyObject = false;
	}
	if (keyContext) {
	    prefixDelimiterAppender(TuplesToResourcesMapper.CONTEXT_VALUE);
	    keyContext = false;
	}
	
	context.write(key, new Text(relations.toString()));
    };
    
    private void prefixDelimiterAppender(String s) {
	if (relations.length() > 0) {
	    relations.append(ResourcesReducer.VALUE_DELIMITER);
	}
	relations.append(s);
    }
}
