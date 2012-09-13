package com.yahoo.glimmer.query;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.yahoo.glimmer.query.RDFIndexStatistics.ClassStat;

public class RDFIndexStatisticsBuilderTest {
    private Map<String, String> sortedPredicates = new HashMap<String, String>();
    private Map<String, Integer> typeTermDistribution = new HashMap<String, Integer>();
    private InputStream ontologyIs;
    private Map<String, Integer> predicateTermDistribution;

    @Before
    public void before() {
	sortedPredicates.put("subject", "subject");
	sortedPredicates.put("predicate", "predicate");
	sortedPredicates.put("object", "object");
	sortedPredicates.put("type", "http_www_w3_org_1999_02_22_rdf_syntax_ns_type");
	sortedPredicates.put("domain", "http_schema_org_Property_domain");
	sortedPredicates.put("range", "http_schema_org_Property_range");
	sortedPredicates.put("subClassOf", "http_schema_org_Type_subClassOf");

	typeTermDistribution.put("http://schema.org/Thing", 100);

	typeTermDistribution.put("http://schema.org/LocalBusiness", 16);
	typeTermDistribution.put("http://schema.org/EntertainmentBusiness", 6);
	typeTermDistribution.put("http://schema.org/ArtGallery", 5);
	typeTermDistribution.put("http://schema.org/MovieTheater", 2);

	typeTermDistribution.put("http://schema.org/CreativeWork", 20);
	typeTermDistribution.put("http://schema.org/Article", 3);

	ontologyIs = RDFIndexStatisticsBuilderTest.class.getClassLoader().getResourceAsStream("schemaDotOrg.owl");

	predicateTermDistribution = new HashMap<String, Integer>();
	predicateTermDistribution.put("http_schema_org_url", 20);
	predicateTermDistribution.put("http_schema_org_description", 10);
	predicateTermDistribution.put("http_schema_org_name", 22);
	predicateTermDistribution.put("http_schema_org_awards", 1);
	predicateTermDistribution.put("http_schema_org_nonsense", 100);
    }

    @Test
    public void test() throws IOException {
	RDFIndexStatisticsBuilder builder = new RDFIndexStatisticsBuilder();
	builder.setSortedPredicates(sortedPredicates);
	builder.setTypeTermDistribution(typeTermDistribution);

	RDFIndexStatistics stats = builder.build();

	assertNotNull(stats.getFields());
	assertEquals(7, stats.getFields().size());
	assertEquals("http_www_w3_org_1999_02_22_rdf_syntax_ns_type", stats.getFields().get("type"));

	assertNotNull(stats.getClasses());
	assertEquals(7, stats.getClasses().size());
	assertTrue(stats.getClasses().containsKey("http://schema.org/CreativeWork"));
	assertEquals(20, stats.getClasses().get("http://schema.org/CreativeWork").getCount());
	assertNull(stats.getClasses().get("http://schema.org/CreativeWork").getLabel());
	assertNull(stats.getClasses().get("http://schema.org/CreativeWork").getChildren());
	assertNull(stats.getClasses().get("http://schema.org/CreativeWork").getProperties());

	assertNull(stats.getProperties());
	assertNull(stats.getRootClasses());

	builder.setOwlOntologyInputStream(ontologyIs);
	builder.setPredicateTermDistribution(predicateTermDistribution);
	stats = builder.build();

	assertNotNull(stats.getRootClasses());
	assertEquals(1, stats.getRootClasses().size());
	assertTrue(stats.getRootClasses().contains("http://schema.org/Thing"));
	ClassStat stat = stats.getClasses().get("http://schema.org/Thing");
	
	//System.out.println(RDFIndexStatisticsBuilder.toString(stats));
	
	// Assert ontology tree is correct..
	assertThing(stats, stat);

	assertNotNull(stats.getProperties());
	assertEquals(4, stats.getProperties().size());
	assertEquals((Integer)20, stats.getProperties().get("http://schema.org/url"));
	assertEquals((Integer)10, stats.getProperties().get("http://schema.org/description"));
	assertEquals((Integer)22, stats.getProperties().get("http://schema.org/name"));
	assertEquals((Integer)1, stats.getProperties().get("http://schema.org/awards"));
    }

    private static void assertThing(RDFIndexStatistics stats, ClassStat stat) {
	Iterator<String> ci = stat.getChildren().iterator();
	assertCreativeWork(stats, stats.getClasses().get(ci.next()));
	assertOrganization(stats, stats.getClasses().get(ci.next()));
	assertPlace(stats, stats.getClasses().get(ci.next()));
	assertFalse(ci.hasNext());
	assertClassStat(stat, "Thing", 152, "http://schema.org/description", "http://schema.org/name", "http://schema.org/url");
    }

    private static void assertCreativeWork(RDFIndexStatistics stats, ClassStat stat) {
	Iterator<String> ci = stat.getChildren().iterator();
	assertArticle(stats, stats.getClasses().get(ci.next()));
	assertFalse(ci.hasNext());
	assertClassStat(stat, "CreativeWork", 23, "http://schema.org/awards", "http://schema.org/contentRating", "http://schema.org/datePublished",
		"http://schema.org/genre", "http://schema.org/headline", "http://schema.org/inLanguage", "http://schema.org/interactionCount", "http://schema.org/keywords");
    }

    private static void assertArticle(RDFIndexStatistics stats, ClassStat stat) {
	assertNull(stat.getChildren());
	assertClassStat(stat, "Article", 3, "http://schema.org/articleBody", "http://schema.org/articleSection");
    }

    private static void assertOrganization(RDFIndexStatistics stats, ClassStat stat) {
	Iterator<String> ci = stat.getChildren().iterator();
	assertLocalBusiness(stats, stats.getClasses().get(ci.next()));
	assertFalse(ci.hasNext());
	assertClassStat(stat, "Organization", 29);
    }

    private static void assertLocalBusiness(RDFIndexStatistics stats, ClassStat stat) {
	Iterator<String> ci = stat.getChildren().iterator();
	assertEntertainmentBusiness(stats, stats.getClasses().get(ci.next()));
	assertFalse(ci.hasNext());
	assertClassStat(stat, "LocalBusiness", 29, "http://schema.org/currenciesAccepted", "http://schema.org/paymentAccepted", "http://schema.org/priceRange");
    }

    private static void assertEntertainmentBusiness(RDFIndexStatistics stats, ClassStat stat) {
	Iterator<String> ci = stat.getChildren().iterator();
	assertArtGallery(stats, stats.getClasses().get(ci.next()));
	assertMovieTheater(stats, stats.getClasses().get(ci.next()));
	assertFalse(ci.hasNext());
	assertClassStat(stat, "EntertainmentBusiness", 13);
    }

    private static void assertArtGallery(RDFIndexStatistics stats, ClassStat stat) {
	assertNull(stat.getChildren());
	assertClassStat(stat, "ArtGallery", 5);
    }

    private static void assertMovieTheater(RDFIndexStatistics stats, ClassStat stat) {
	assertNull(stat.getChildren());
	assertClassStat(stat, "MovieTheater", 2);
    }
    
    private static void assertPlace(RDFIndexStatistics stats, ClassStat stat) {
	Iterator<String> ci = stat.getChildren().iterator();
	assertCivicStructure(stats, stats.getClasses().get(ci.next()));
	assertLocalBusiness(stats, stats.getClasses().get(ci.next()));
	assertFalse(ci.hasNext());
	assertClassStat(stat, "Place", 29);
    }
    
    private static void assertCivicStructure(RDFIndexStatistics stats, ClassStat stat) {
	Iterator<String> ci = stat.getChildren().iterator();
	assertMovieTheater(stats, stats.getClasses().get(ci.next()));
	assertFalse(ci.hasNext());
	assertClassStat(stat, "CivicStructure", 2);
    }

    private static void assertClassStat(ClassStat stat, String label, int count, String... properties) {
	assertNotNull(stat);

	//System.out.println(stat.getLabel() + ':' + stat.getCount() + '/' + stat.getInheritedCount() + " (" + count + ")");
	assertEquals(label, stat.getLabel());
	//assertEquals(count, stat.getCount());
	if (stat.getProperties() != null) {
	    assertArrayEquals(properties, stat.getProperties().toArray());
	} else {
	    assertEquals(0, properties.length);
	}
    }
}
