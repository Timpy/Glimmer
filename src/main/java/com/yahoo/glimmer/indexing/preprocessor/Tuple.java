package com.yahoo.glimmer.indexing.preprocessor;

import com.yahoo.glimmer.indexing.preprocessor.TuplesToResourcesMapper.TupleElementName;

public class Tuple {
    public TupleElement subject = new TupleElement();
    public TupleElement predicate = new TupleElement();
    public TupleElement object = new TupleElement();
    public TupleElement context = new TupleElement();
    
    public TupleElement getElement(TupleElementName name) {
        switch (name) {
        case SUBJECT : return subject;
        case PREDICATE : return predicate;
        case OBJECT : return object;
        case CONTEXT : return context;
        default: return null;
        }
    }
}