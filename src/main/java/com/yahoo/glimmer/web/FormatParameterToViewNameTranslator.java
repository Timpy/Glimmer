package com.yahoo.glimmer.web;

/*
 * Copyright (c) 2012 Yahoo! Inc. All rights reserved.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is 
 *  distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *  See accompanying LICENSE file.
 */

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
