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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLProperty;

import com.yahoo.glimmer.vocabulary.OwlUtils;

public class IndexStatistics {

    // Fields of the index
    protected List<String> fields = new ArrayList<String>();

    protected Map<String, ClassStat> classes = new HashMap<String, ClassStat>();

    protected Map<String, PropertyStat> properties = new HashMap<String, PropertyStat>();

    protected transient Map<String, Integer> predDist = new HashMap<String, Integer>();

    public static class PropertyStat {

	protected String label;
	protected int count;

	public PropertyStat(int count) {
	    this.count = count;
	}

	public PropertyStat(String label, int count) {
	    this.count = count;
	    this.label = label;
	}
    }

    public static class ClassStat {
	protected String label;
	protected List<String> properties = new ArrayList<String>();
	protected int count;
	protected List<String> children = new ArrayList<String>();

	public ClassStat(int count) {
	    this.count = count;
	}

	public ClassStat(String label, List<String> properties, int count, List<String> children) {

	    this.label = label;
	    this.properties = properties;
	    this.count = count;
	    this.children = children;
	}
    }

    public final static String TYPE_INDEX = "http_www_w3_org_1999_02_22_rdf_syntax_ns_type";

    public IndexStatistics(RDFIndex index) throws IOException {
	// Order the fields by frequency
	final Map<String, Integer> fieldDist = com.yahoo.glimmer.query.Util.getTermDistribution(index.getPredicateIndex());
	fields = new ArrayList<String>(fieldDist.keySet());
	Collections.sort(fields, new Comparator<String>() {
	    @Override
	    public int compare(String o1, String o2) {
		return fieldDist.get(o2).compareTo(fieldDist.get(o1));
	    }
	});

	// HACK
	for (String key : fieldDist.keySet()) {
	    if (key != null && fieldDist.get(key) != null) {
		predDist.put(removeVersion(key), fieldDist.get(key));
	    }
	}

	// Capture basic statistics about class frequency
	if (index.getIndexedFields().contains(TYPE_INDEX)) {
	    Map<String, Integer> classDist = Util.getTermDistribution(index.getField(TYPE_INDEX));
	    for (String clazzName : classDist.keySet()) {
		classes.put(removeVersion(clazzName), new ClassStat(classDist.get(clazzName)));
	    }
	}
    }

    public void loadInfoFromOntology(OWLOntology onto) {
	for (String clazzName : classes.keySet()) {
	    OWLClass clazz = null;
	    // Remove version if the class name contains a version number
	    if (onto.containsClassInSignature(IRI.create(clazzName))) {
		clazz = onto.getOWLOntologyManager().getOWLDataFactory().getOWLClass(IRI.create(clazzName));
	    } else {
		clazz = onto.getOWLOntologyManager().getOWLDataFactory().getOWLClass(IRI.create(removeVersion(clazzName)));
	    }

	    if (clazz != null) {
		ClassStat stat = classes.get(clazzName);

		for (OWLProperty<?, ?> prop : OwlUtils.getPropertiesInDomain(clazz, onto)) {
		    if (prop instanceof OWLDataProperty) {
			stat.properties.add(prop.getIRI().toString());
			if (predDist.containsKey(com.yahoo.glimmer.util.Util.encodeFieldName(removeVersion(prop.getIRI().toString())))) {
			    properties.put(prop.getIRI().toString(),
				    new PropertyStat(predDist.get(com.yahoo.glimmer.util.Util.encodeFieldName(removeVersion(prop.getIRI().toString())))));
			}
		    }
		}
		for (OWLClassExpression subExpr : clazz.getSubClasses(onto)) {
		    if (subExpr instanceof OWLClass) {
			stat.children.add(subExpr.asOWLClass().getIRI().toString());
		    }
		}

		stat.label = OwlUtils.getLabel(clazz, onto);

	    } else {
		System.err.println("Indexed type not in the ontology: " + clazzName);
	    }
	}

    }
    
    private String removeVersion(String uri) {
	// HACK: second part we shouldn't need
	String result = uri.replaceFirst("[0-9]+\\.[0-9]+\\.[0-9]+\\/", "");
	result = result.replaceFirst("[0-9]+_[0-9]_+[0-9]+_", "");
	return result;
    }
}
