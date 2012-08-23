package com.yahoo.glimmer.query;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLProperty;

import com.yahoo.glimmer.query.RDFIndexStatistics.ClassStat;
import com.yahoo.glimmer.vocabulary.OwlUtils;

public class RDFIndexStatisticsBuilder {
    public static RDFIndexStatistics create(final Map<String, String> sortedPredicates, final Map<String, Integer> typeTermDistribution) {
	RDFIndexStatistics stats = new RDFIndexStatistics();
	stats.setFields(sortedPredicates);

	// Capture basic statistics about class frequency
	for (String clazzName : typeTermDistribution.keySet()) {
	    Integer count = typeTermDistribution.get(clazzName);
	    stats.addClassStat(removeVersion(clazzName), new ClassStat(count));
	}
	
	return stats;
    }

    public static void addOntology(RDFIndexStatistics stats, File ontologyFile, final Map<String, Integer> predicateTermDistribution) throws FileNotFoundException {
	if (!ontologyFile.exists()) {
	    URL owlOntologyUrl = RDFIndexStatisticsBuilder.class.getClassLoader().getResource(ontologyFile.getPath());
	    if (owlOntologyUrl != null) {
		ontologyFile = new File(owlOntologyUrl.getFile());
	    } else {
		throw new FileNotFoundException("Can find ontology file or resource for " + ontologyFile);
	    }
	}

	OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
	OWLOntology onto;
	
	try {
	    onto = manager.loadOntologyFromOntologyDocument(ontologyFile);
	} catch (OWLOntologyCreationException e) {
	    throw new IllegalArgumentException("Ontology failed to load:" + e.getMessage());
	}

	Map<String, Integer> predDist = new HashMap<String, Integer>();
	
	for (String key : predicateTermDistribution.keySet()) {
	    Integer count = predicateTermDistribution.get(key);
	    if (key != null && count != null) {
		predDist.put(removeVersion(key), count);
	    }
	}

	for (String clazzName : stats.getClasses().keySet()) {
	    OWLClass clazz = null;
	    // Remove version if the class name contains a version number
	    if (onto.containsClassInSignature(IRI.create(clazzName))) {
		clazz = onto.getOWLOntologyManager().getOWLDataFactory().getOWLClass(IRI.create(clazzName));
	    } else {
		clazz = onto.getOWLOntologyManager().getOWLDataFactory().getOWLClass(IRI.create(removeVersion(clazzName)));
	    }

	    if (clazz != null) {
		ClassStat stat = stats.getClasses().get(clazzName);
		stat.setLabel(OwlUtils.getLabel(clazz, onto));

		for (OWLProperty<?, ?> prop : OwlUtils.getPropertiesInDomain(clazz, onto)) {
		    if (prop instanceof OWLDataProperty) {
			String name = prop.getIRI().toString();
			String encodeName = com.yahoo.glimmer.util.Util.encodeFieldName(removeVersion(name));
			
			stat.addProperty(name);
			if (predDist.containsKey(encodeName)) {
			    stats.addPropertyStat(name, predDist.get(encodeName));
			}
		    }
		}
		for (OWLClassExpression subExpr : clazz.getSubClasses(onto)) {
		    if (subExpr instanceof OWLClass) {
			stat.addChild(subExpr.asOWLClass().getIRI().toString());
		    }
		}


	    } else {
		System.err.println("Indexed type not in the ontology: " + clazzName);
	    }
	}
    }

    private static String removeVersion(String uri) {
	// HACK: second part we shouldn't need
	uri = uri.replaceFirst("[0-9]+\\.[0-9]+\\.[0-9]+\\/", "");
	uri = uri.replaceFirst("[0-9]+_[0-9]_+[0-9]+_", "");
	return uri;
    }
}