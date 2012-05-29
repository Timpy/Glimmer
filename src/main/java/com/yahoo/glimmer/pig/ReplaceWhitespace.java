package com.yahoo.glimmer.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * Replace sequences of any whitespace char with a single space
 * 
 * @author pmika
 * 
 */
public class ReplaceWhitespace extends EvalFunc<String> {

    public static final String replaceWhitespace(String str) {
	String result = str;
	result = result.replaceAll("\\s+", " ");
	return result;
    }

    // @Override
    public String exec(Tuple input) throws IOException {

	if (input == null || input.size() == 0 || input.get(0) == null)
	    return null;

	String tuple;
	if (input.get(0) instanceof DataByteArray) {
	    tuple = ((DataByteArray) input.get(0)).toString();
	} else if (input.get(0) instanceof String) {
	    tuple = (String) input.get(0);
	} else {
	    throw new IOException("Unknown argument type: " + input.get(0).getClass());
	}

	return replaceWhitespace(tuple);

    }

    // Test only

    public static void main(String[] args) {

	String str = "_:thisuserhttpx3Ax2Fx2Fwwwx2Ekanzakix2Ecomx2Fworksx2F2005x2Fmiscx2Fflickr2foafx3Fux3Dadamjscott \t\t <http://xmlns.com/foaf/0.1/made> \"test1 test2\" <http://www.kanzaki.com/works/2005/misc/flickr2foaf?u=adamjscott> .\n";

	System.out.println(replaceWhitespace(str));

    }

}
