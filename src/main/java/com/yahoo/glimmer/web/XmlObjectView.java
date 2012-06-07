package com.yahoo.glimmer.web;

import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.View;

import com.thoughtworks.xstream.XStream;

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
	
	xStream.toXML(object, writer);
    }
}
