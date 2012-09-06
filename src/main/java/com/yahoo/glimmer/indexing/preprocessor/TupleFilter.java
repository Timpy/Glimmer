package com.yahoo.glimmer.indexing.preprocessor;


/** TupleRewriter filter out/rewrite tuples during preprocessing of RDF
 * 
 * @author tep
 */
public interface TupleFilter {
    /**
     * @param tuple Mutable Tuple as read from input. Note that the passed in tuple and it's elements are 're-used'.
     * @return true to keep the tuple or false to skip it.
     */
    public boolean filter(Tuple tuple);
}