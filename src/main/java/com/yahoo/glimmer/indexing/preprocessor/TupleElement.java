package com.yahoo.glimmer.indexing.preprocessor;


public class TupleElement {
    public static enum Type {
	BNODE, LITERAL, RESOURCE, UNBOUND, VARIABLE;
    }

    public Type type;
    public String text;
    public String n3;

    public boolean isOfType(Type... types) {
	for (Type type : types) {
	    if (this.type == type) {
		return true;
	    }
	}
	return false;
    }
}