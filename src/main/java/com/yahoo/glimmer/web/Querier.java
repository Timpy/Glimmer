package com.yahoo.glimmer.web;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.mg4j.query.SelectedInterval;
import it.unimi.dsi.mg4j.query.nodes.Query;
import it.unimi.dsi.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.dsi.mg4j.search.score.DocumentScoreInfo;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.yahoo.glimmer.disambiguation.RdfDisambiguator;
import com.yahoo.glimmer.query.QueryLogger;
import com.yahoo.glimmer.query.RDFIndex;
import com.yahoo.glimmer.query.RDFQueryResult;
import com.yahoo.glimmer.query.RDFResultItem;

/**
 * Wraps the details of doing a query against an RDFIndex.
 * 
 */
public class Querier {
    private final static Logger LOGGER = Logger.getLogger(Querier.class);
    
    private RdfDisambiguator disambiguator;
    
    public RDFQueryResult doQuery(RDFIndex index, Query query, int startItem, int maxNumItems, boolean dedupe, boolean deref) throws QueryBuilderVisitorException, IOException {
	if (startItem < 0 || maxNumItems < 0 || maxNumItems > 10000) {
	    throw new IllegalArgumentException("Bad item range - start:" + startItem + " maxNumItems:" + maxNumItems);
	}

	// When deduping, we ask for twice as many results
	// We will later reduce this to the original amount
	if (dedupe) {
	    maxNumItems *= 2;
	}

	// Reconfigure scorer
	// TODO is this supposed to redifine the scoring for everyone or just this query?
//	if (context != null) {
//	    try {
//		context.reload();
//		context.update(request);
//		LOGGER.info("Reconfiguring scorer");
//		index.reconfigure(context);
//	    } catch (Exception e) {
//		e.printStackTrace();
//	    }
//	}

	ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>();

	int numResults = index.process(query, startItem, maxNumItems, results);

	if (results.size() > maxNumItems) {
	    results.size(maxNumItems);
	}

	ObjectArrayList<RDFResultItem> resultItems = new ObjectArrayList<RDFResultItem>();
	if (!results.isEmpty()) {
	    for (int i = 0; i < results.size(); i++) {
		DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi = results.get(i);
		LOGGER.debug("Intervals for item " + i);
		LOGGER.debug("score " + dsi.score);
		final RDFResultItem resultItem = new RDFResultItem(index.getIndexedFields(), index.getCollection(), dsi.document, dsi.score);
		resultItems.add(resultItem);

	    }
	    // Dedupe results if requested
	    if (dedupe) {
		// This would replace the result list with the
		// disambiguated list
		resultItems = dedupe(disambiguator, resultItems, maxNumItems);
		// This just marks the duplicates
		// dedupe(disambiguator, resultItems, maxNumItems /
		// 2 );
	    }
	}

	// Stop the timer
	long time = 0;
	QueryLogger queryLogger = index.getQueryLogger();
	if (queryLogger != null) {
	    queryLogger.endQuery(query, numResults);
	    time = queryLogger.getTime();
	}
	RDFQueryResult result = new RDFQueryResult(null, query, numResults, resultItems, time);

	// Dereferencing
	if (deref) {
	    time = System.currentTimeMillis();
	    result.dereference(index.getSubjectsMPH());
	    LOGGER.info("Dereferencing took " + (System.currentTimeMillis() - time) + " ms");
	}
	return result;
    }
    
    /**
     * Remove duplicates from a result list until at least max unique results
     * are found or the list is exhausted
     * 
     * @return
     */
    public static ObjectArrayList<RDFResultItem> dedupe(RdfDisambiguator dis, List<RDFResultItem> list, int max) {
	ObjectArrayList<RDFResultItem> result = new ObjectArrayList<RDFResultItem>();
	for (RDFResultItem item : list) {
	    // Compare it with all existing items
	    boolean found = false;
	    for (RDFResultItem current : result) {
		if (dis.compare(current, item)) {
		    found = true;
		    // System.out.println(current.getText() + "SAMEAS\n" +
		    // item.getText());
		    // Store decisions in the items
		    current.addDuplicate(item);
		    item.addDuplicate(current);
		    break;
		}
	    }
	    if (!found) {
		result.add(item);
		if (result.size() >= max) {
		    break;
		}
	    }
	}
	return result;
    }
    
    public void setDisambiguator(RdfDisambiguator disambiguator) {
	this.disambiguator = disambiguator;
    }
}
