package com.yahoo.glimmer.query;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Jave Bean wire object containing RDFIndex stats.
 * 
 * @author tep
 */
public class RDFIndexStatistics {
    public static class ClassStat {
	private final int count;
	private String label;
	private List<String> properties;
	private List<String> children;
	
	public ClassStat(int count) {
	    this.count = count;
	}
	public int getCount() {
	    return count;
	}
	public String getLabel() {
	    return label;
	}
	public void setLabel(String label) {
	    this.label = label;
	}
	public List<String> getProperties() {
	    return properties;
	}
	public void addProperty(String property) {
	    properties.add(property);
	}
	public List<String> getChildren() {
	    return children;
	}
	public void addChild(String child) {
	    children.add(child);
	}
    }
    
    private String nameSpace;
    private Map<String, String> fields;
    private final Map<String, ClassStat> classes = new HashMap<String, ClassStat>();
    private final Map<String, Integer> properties = new HashMap<String, Integer>();
    
    public String getNameSpace() {
        return nameSpace;
    }
    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }
    public Map<String, String> getFields() {
        return fields;
    }
    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }
    public Map<String, ClassStat> getClasses() {
        return classes;
    }
    public void addClassStat(String name, ClassStat stat) {
        classes.put(name, stat);
    }
    public Map<String, Integer> getProperties() {
        return properties;
    }
    public void addPropertyStat(String name, Integer count) {
        properties.put(name, count);
    }
}
