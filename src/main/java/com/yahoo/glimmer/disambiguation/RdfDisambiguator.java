package com.yahoo.glimmer.disambiguation;

import com.yahoo.glimmer.query.RDFResultItem;

public interface RdfDisambiguator {

    public boolean compare(RDFResultItem r1, RDFResultItem r2);

}
