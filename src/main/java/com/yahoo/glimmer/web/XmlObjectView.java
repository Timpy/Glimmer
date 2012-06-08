package com.yahoo.glimmer.web;

import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.query.ResultItem;

import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.View;

import com.thoughtworks.xstream.XStream;
import com.yahoo.glimmer.query.RDFIndex;
import com.yahoo.glimmer.query.RDFQueryResult;
import com.yahoo.glimmer.query.Util;

/**
 * @{View} that renders the model Object under OBJECT_KEY as XML
 * 
 * @author tep@yahoo-inc.com
 */
public class XmlObjectView implements View {
    private XStream xStream = new XStream();
    
    @Override
    public String getContentType() {
	// Setting charset= means that response.getWriter() will return a Writer that writes in that charset.
	return "text/xml; charset=UTF-8";
    }

    @Override
    public void render(Map<String, ?> model, HttpServletRequest request, HttpServletResponse response) throws Exception {
	Object object = model.get(QueryController.OBJECT_KEY);
	if (object == null) {
	    throw new IllegalArgumentException("Model does not contain an object!");
	}
	
	response.setContentType(getContentType());
	PrintWriter writer = response.getWriter();
	
	if (object instanceof RDFQueryResult) {
	    RDFIndex index = (RDFIndex) model.get(QueryController.INDEX_KEY);
	    if (index == null) {
		throw new IllegalArgumentException("Model does not contain an index!");
	    }
	    
	    RDFQueryResult result = (RDFQueryResult) object;
	    // FIXME: this is not XML!!!!
	    int resultCount = 0;
	    for (ResultItem item : result.getResultItems()) {
		resultCount++;
		writer.println("<result> " + resultCount + "</result>");
		if (index.getIndexIdfs() != null) {
		    writer.println("<documentSize> " + index.getIndexIdfs().sizes.getInt(item.doc) + "</documentSize>");
		}
		writer.println("<score>" + item.score + "</score>");
		writer.println("<uri>" + item.uri + "</uri>");
		
		Document d = index.getCollection().document(item.doc);
		writer.println("<contents>" + Util.getText(d) + "</contents>");
		
	    }
	} else {
	    xStream.toXML(object, writer);
	}
    }
}
