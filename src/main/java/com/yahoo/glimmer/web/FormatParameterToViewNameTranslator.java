package com.yahoo.glimmer.web;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.servlet.RequestToViewNameTranslator;

public class FormatParameterToViewNameTranslator implements RequestToViewNameTranslator {
    private String parameterName = "format";
    private String defaultValue = "js";
    private String namePostfix = "ObjectView";
    
    /**
     * Takes the named parameter from the request and returns the value appended with namePostfix.
     * If no parameter of the name is in the request a default of defaultValue is used.
     */
    @Override
    public String getViewName(HttpServletRequest request) throws Exception {
	String format = request.getParameter(parameterName);
	if (format == null) {
	    format = defaultValue;
	}
	return format + namePostfix;
    }
    
    public void setParameterName(String parameterName) {
	this.parameterName = parameterName;
    }
    public void setDefaultValue(String defaultValue) {
	this.defaultValue = defaultValue;
    }
    public void setNamePostfix(String namePostfix) {
	this.namePostfix = namePostfix;
    }
}
