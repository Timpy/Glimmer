package com.yahoo.glimmer.pig;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * Concatenate values in bag
 * 
 * Example:
 * 
 * Data:
 * 
 * a 1 a 1 b 2 b 3
 * 
 * Script:
 * 
 * a = load 'test-concat.txt' using PigStorage('\t') AS
 * (key:chararray,value:chararray); b = group a by key; describe b; dump b; c =
 * foreach b generate
 * group,com.yahoo.glimmer.pig.ConcatValuesInBag(a, ','); dump c;
 * 
 * Result:
 * 
 * (a,"1,1") (b,"2,3")
 * 
 * 
 * 
 * @author pmika@yahoo-inc.com
 * 
 */
public class ConcatValuesInBag extends EvalFunc<String> {

    // @Override
    public String exec(Tuple input) throws IOException {
	StringBuffer result = new StringBuffer();
	if (input == null || input.size() == 0 || input.get(0) == null)
	    return null;

	DataBag values = (DataBag) input.get(0);
	String separator = "";
	if (input.get(1) != null) {
	    separator = (String) input.get(1);
	}
	for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
	    Tuple t = it.next();
	    try {

		result.append(((String) t.get(1)).toString() + separator);

	    } catch (Exception exp) {
		System.err.println("Error processing: " + t.toString() + exp.getMessage());

	    }
	}

	// Remove last separator if there was anything in the String
	if (result.length() - separator.length() > 0) {
	    return result.substring(0, result.length() - separator.length());
	}
	return result.toString();
    }

}
