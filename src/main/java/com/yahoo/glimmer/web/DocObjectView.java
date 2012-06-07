package com.yahoo.glimmer.web;

import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.query.ResultItem;

import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.View;

import com.yahoo.glimmer.query.RDFIndex;
import com.yahoo.glimmer.query.RDFQueryResult;
import com.yahoo.glimmer.query.Util;

public class DocObjectView implements View {
    private boolean includeUri;

    @Override
    public String getContentType() {
	// Setting charset= means that response.getWriter() will return a Writer
	// that writes in that charset.
	return "text/plain; charset=UTF-8";
    }

    @Override
    public void render(Map<String, ?> model, HttpServletRequest request, HttpServletResponse response) throws Exception {
	Object object = model.get(QueryController.OBJECT_KEY);
	if (object == null) {
	    throw new IllegalArgumentException("Model does not contain an object!");
	}

	response.setContentType(getContentType());
	PrintWriter writer = response.getWriter();

	// Return URIs and docs separated by tab
	if (object instanceof RDFQueryResult) {
	    RDFIndex index = (RDFIndex) model.get(QueryController.INDEX_KEY);
	    if (index == null) {
		throw new IllegalArgumentException("Model does not contain an index!");
	    }
	    RDFQueryResult result = (RDFQueryResult) object;
	    for (ResultItem item : result.getResultItems()) {
		Document doc = index.getCollection().document(item.doc);
		if (includeUri) {
		    writer.println(item.uri + "\t");
		}
		Util.getText(doc);
		doc.close();
	    }
	} else {
	    writer.print(object.toString());
	}
    }

    public void setIncludeUri(boolean includeUri) {
	this.includeUri = includeUri;
    }
}
