package com.yahoo.glimmer.vocabulary;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.openrdf.model.impl.URIImpl;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAnnotation;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLNamedObject;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectUnionOf;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLProperty;

public class OwlUtils {
    public final static IRI DC_DESCRIPTION = IRI.create("http://purl.org/dc/terms/description");
    public final static IRI DC_DESCRIPTION_DEPRECATED = IRI.create("http://purl.org/dc/elements/1.1/description");

    public final static IRI DC_CREATOR = IRI.create("http://purl.org/dc/terms/creator");
    public final static IRI DC_CREATOR_DEPRECATED = IRI.create("http://purl.org/dc/elements/1.1/creator");

    public static String getLocalName(IRI iri) {
	return new URIImpl(iri.toString()).getLocalName();
    }

    private static String getLocalName(OWLNamedObject entity) {
	return getLocalName(entity.getIRI());
    }

    private static class LocalNameComparator implements Comparator<OWLNamedObject> {
	public int compare(OWLNamedObject o1, OWLNamedObject o2) {
	    return getLocalName(o1).compareTo(getLocalName(o2));
	}
    }

    public static String getLabel(OWLEntity entity, OWLOntology onto) {
	String vocabName = "";
	Set<OWLAnnotation> annots = entity.getAnnotations(onto);
	for (OWLAnnotation annot : annots) {
	    if (annot.getProperty().isLabel()) {
		vocabName = ((OWLLiteral) annot.getValue()).getLiteral();
	    }
	}
	return vocabName;
    }

    public static List<OWLProperty<?, ?>> getPropertiesInDomain(OWLClass aClass, OWLOntology onto) {

	List<OWLProperty<?, ?>> properties = new ArrayList<OWLProperty<?, ?>>();
	Set<OWLClass> superclasses = new HashSet<OWLClass>();

	// NOTE: we don't add superclasses for WOO:
	// properties inherited from superclasses should appear in their own
	// facet
	// superclasses =getSuperClasses(aClass, onto);

	// Add this class as well
	superclasses.add(aClass);

	for (OWLClass clazz : superclasses) {

	    Set<OWLAxiom> axioms = clazz.getReferencingAxioms(onto, true);
	    for (OWLAxiom axiom : axioms) {
		if (axiom instanceof OWLDataPropertyDomainAxiom) {

		    OWLDataPropertyDomainAxiom dataDomain = (OWLDataPropertyDomainAxiom) axiom;
		    OWLClassExpression dataDomainExpr = dataDomain.getDomain();

		    if (clazz.equals(dataDomainExpr) && !dataDomain.getProperty().isAnonymous()) {
			properties.add(dataDomain.getProperty().asOWLDataProperty());
		    } else if (dataDomainExpr instanceof OWLObjectUnionOf) {
			for (OWLClassExpression op : ((OWLObjectUnionOf) dataDomainExpr).getOperands()) {
			    // Let's hope we don't use unions of unions...
			    // otherwise switch to visitor pattern
			    if (clazz.equals(op) && !dataDomain.getProperty().isAnonymous()) {
				properties.add(dataDomain.getProperty().asOWLDataProperty());
			    }
			}
		    }

		} else if (axiom instanceof OWLObjectPropertyDomainAxiom) {
		    OWLObjectPropertyDomainAxiom objectDomain = (OWLObjectPropertyDomainAxiom) axiom;
		    OWLClassExpression objectDomainExpr = objectDomain.getDomain();
		    if (clazz.equals(objectDomainExpr) && !objectDomain.getProperty().isAnonymous()) {
			properties.add(objectDomain.getProperty().asOWLObjectProperty());
		    } else if (objectDomainExpr instanceof OWLObjectUnionOf) {
			for (OWLClassExpression op : ((OWLObjectUnionOf) objectDomainExpr).getOperands()) {
			    // Let's hope we don't use unions of unions...
			    // otherwise switch to visitor pattern
			    if (clazz.equals(op) && !objectDomain.getProperty().isAnonymous()) {
				properties.add(objectDomain.getProperty().asOWLObjectProperty());
			    }
			}
		    }
		}
	    }
	}
	Collections.sort(properties, new OwlUtils.LocalNameComparator());
	return properties;
    }
}
