package com.yahoo.glimmer.query;

import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.mg4j.index.IndexIterator;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.query.nodes.AbstractQueryBuilderVisitor;
import it.unimi.dsi.mg4j.query.nodes.Align;
import it.unimi.dsi.mg4j.query.nodes.And;
import it.unimi.dsi.mg4j.query.nodes.Consecutive;
import it.unimi.dsi.mg4j.query.nodes.Difference;
import it.unimi.dsi.mg4j.query.nodes.False;
import it.unimi.dsi.mg4j.query.nodes.LowPass;
import it.unimi.dsi.mg4j.query.nodes.MultiTerm;
import it.unimi.dsi.mg4j.query.nodes.Not;
import it.unimi.dsi.mg4j.query.nodes.Or;
import it.unimi.dsi.mg4j.query.nodes.OrderedAnd;
import it.unimi.dsi.mg4j.query.nodes.Prefix;
import it.unimi.dsi.mg4j.query.nodes.Query;
import it.unimi.dsi.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.dsi.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.dsi.mg4j.query.nodes.Range;
import it.unimi.dsi.mg4j.query.nodes.Remap;
import it.unimi.dsi.mg4j.query.nodes.Select;
import it.unimi.dsi.mg4j.query.nodes.Term;
import it.unimi.dsi.mg4j.query.nodes.True;
import it.unimi.dsi.mg4j.query.nodes.Weight;
import it.unimi.dsi.mg4j.query.parser.QueryParser;
import it.unimi.dsi.mg4j.query.parser.QueryParserException;
import it.unimi.dsi.mg4j.query.parser.SimpleParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class RDFQueryParser implements QueryParser {
    private final static Logger LOGGER = Logger.getLogger(RDFQueryParser.class);

    private SimpleParser parser;

    private Index precompIndex;
    private List<String> properties;
    private Set<String> fields;
    private String defaultField;
    private String uriField;
    private Map<String, ? extends TermProcessor> termProcessors;
    private Object2LongFunction<CharSequence> mph;
    private final static Pattern RESOURCE_PATTERN = Pattern.compile("\\((http://.*)\\)");

    public RDFQueryParser(RDFIndex index) {

	final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(index.getIndexedFields()
		.size());
	for (String alias : index.getIndexedFields())
	    termProcessors.put(alias, index.getField(alias).termProcessor);

	init(index.getAlignmentIndex(), index.getAllFields(), index.getIndexedFields(), index.getDefaultField(), index.getURIField(), termProcessors,
		index.getSubjectsMPH());
    }

    public RDFQueryParser(Index precompIndex, List<String> properties, Set<String> fields, String defaultField, String uriField,
	    final Map<String, ? extends TermProcessor> termProcessors, final Object2LongFunction<CharSequence> mph) {
	init(precompIndex, properties, fields, defaultField, uriField, termProcessors, mph);
    }

    protected void init(Index precompIndex, List<String> properties, Set<String> fields, String defaultField, String uriField,
	    final Map<String, ? extends TermProcessor> termProcessors, final Object2LongFunction<CharSequence> mph) {
	this.precompIndex = precompIndex;
	this.properties = properties;
	this.fields = fields;
	this.defaultField = defaultField;
	this.uriField = uriField;
	this.termProcessors = termProcessors;
	this.mph = mph;
	parser = new SimpleParser(fields, defaultField, termProcessors);
    }

    // private static class QueryPart {
    // String query;
    // String field;
    // boolean phrase;
    //
    // public QueryPart(String query, String field, boolean phrase) {
    // this.query = query;
    // this.field = field;
    // this.phrase = phrase;
    // }
    //
    // public String toString() {
    // return "(" + query + "," + field + "," + phrase+ ")";
    // }
    // }

    public final static String cleanQuery(String query) {
	// Replace a dotted word such as www.yahoo.com with a phrase query
	// without dots
	String[] parts = query.split("[ ]+");
	String yahooQuery = "";
	for (String part : parts) {
	    if (part.contains(".")) {
		if (part.contains("\"")) {
		    // The query is already a phrase query
		    part = part.replaceAll("\\.", " ");
		} else {
		    // Make it a phrase query
		    part = part.replaceAll("\\.", " ");
		    if (!part.trim().equals(""))
			part = "\"" + part + "\"";
		}
	    }
	    yahooQuery += part + " ";
	}

	StringBuffer result = new StringBuffer();
	for (int i = 0; i < yahooQuery.length(); i++) {
	    if (!Character.isLetterOrDigit(yahooQuery.charAt(i)) && !(yahooQuery.charAt(i) == '"') && !(yahooQuery.charAt(i) == ':')) {
		result.append(" ");
	    } else {
		result.append(yahooQuery.charAt(i));
	    }
	}
	// Lowercase
	// Separate parenthesis from terms with at least one space
	// result = result.replaceAll("\"", " \" ");

	// Normalize whitespace
	return result.toString().trim().toLowerCase().replaceAll("[\\s]+", " ");
    }

    @Override
    public Query parse(String unparsed) throws QueryParserException {

	if (unparsed == null || unparsed.equals("")) {
	    throw new QueryParserException("Empty query");
	}

	Query query = null;

	try {
	    LOGGER.info("Unparsed query:" + unparsed);
	    Matcher m = RESOURCE_PATTERN.matcher(unparsed);
	    boolean result = m.find();
	    if (result) {
		StringBuffer sb = new StringBuffer();
		do {
		    m.appendReplacement(sb, Long.toString(mph.get(m.group(1))));
		    result = m.find();
		} while (result);
		m.appendTail(sb);
		unparsed = sb.toString();
	    }
	    LOGGER.info("Query after resource encoding:" + unparsed);
	    query = parser.parse(unparsed);
	    LOGGER.info("Query as parsed by MG4J:" + query);
	    query = query.accept(new MyVisitor());
	    LOGGER.info("Query after expansion:" + query);

	} catch (QueryBuilderVisitorException e) {
	    throw new QueryParserException(e);
	}

	return query;
    }

    @Override
    public String escape(String token) {
	return parser.escape(token);
    }

    @Override
    public MutableString escape(MutableString token) {
	return parser.escape(token);
    }

    @Override
    public Query parse(MutableString query) throws QueryParserException {
	return parse(query.toString());
    }

    @Override
    public QueryParser copy() {
	return new RDFQueryParser(precompIndex, properties, fields, defaultField, uriField, termProcessors, mph);
    }

    /*
     * public static Query parseQuery(String raw, boolean vertical, Index
     * precompIndex, List<String> properties, ObjectSet<String> fields, Context
     * context) throws IOException { //Normalize whitespace. Encode special
     * characters String temp = cleanQuery(raw);
     * 
     * //TODO: segment each field query using QLAS
     * 
     * //TODO: remove terms in a blacklist final ObjectArrayList<Query>
     * conjuncts = new ObjectArrayList<Query>();
     * 
     * 
     * for(QueryPart part:parts){ final ObjectArrayList<Query> disjuncts = new
     * ObjectArrayList<Query>();
     * 
     * String[] tokens = part.query.split( "[ ]+" ); ObjectArrayList<Term> terms
     * = new ObjectArrayList<Term>(); for( int i = 0; i < tokens.length; i++ )
     * if ( tokens[ i ].length() != 0 ) terms.add( new Term( tokens[ i ].trim()
     * ) );
     * 
     * if(part.phrase){ //Phrase query
     * 
     * DocumentIterator[] precompIterator = new DocumentIterator[ terms.size()
     * ]; for( int i = precompIterator.length; i-- != 0; ) if ( ! (
     * precompIterator[ i ] = precompIndex.documents( terms.get( i ).toString()
     * ) ).hasNext() ) throw new NoSuchElementException( "Term " + terms.get( i
     * ) + " is not part of the precomputed index " ); if(terms.size() > 0){
     * DocumentIterator ii = AndDocumentIterator.getInstance( precompIterator );
     * //TODO: here we might be adding terms that are not in the index and
     * WOOScorer might fail, because they don't have an IDF
     * 
     * //for( int f = -1; ( f = ii.nextDocument() ) !=
     * precompIndex.numberOfDocuments - 1; ) { for( int f = -1; ( f =
     * ii.nextDocument() ) != - 1; ) { ObjectArrayList<Align> alignedTerms = new
     * ObjectArrayList<Align>(); if ( vertical) { if ( fields.contains(
     * properties.get( f ) ) ) { disjuncts.add( new Select( properties.get( f ),
     * new Consecutive( terms.toArray( new Term[ terms.size() ] ) ) ) ); } else
     * { throw new NoSuchElementException( "indexMap does not contain" +
     * properties.get( f ) ); } } else { for( Term t: terms ) alignedTerms.add(
     * new Align( new Select( "token", t ), new Select( "property", new Term( f
     * ) ) ) ); disjuncts.add( new Consecutive( alignedTerms.toArray( new Align[
     * alignedTerms.size() ] ) ) ); } }
     * 
     * disjuncts.add( new Select( context.WURI_INDEX , new Consecutive(
     * terms.toArray( new Term[ terms.size() ] ) ) ));
     * 
     * ii.dispose(); } } else { //Not a phrase query for (Term term : terms) {
     * 
     * IndexIterator ii = precompIndex.documents( term.term );
     * 
     * //if ( ! ii.hasNext() ) throw new NoSuchElementException( "Term " +
     * token.trim() + " is not part of the precomputed index " );
     * 
     * //TODO: we are dropping terms from the query that are not in the
     * collection...is this the right thing to do? //Skip terms that are not in
     * the precomputed index //if (! ii.hasNext() ) continue;
     * 
     * //for( int f = -1; ( f = ii.nextDocument() ) !=
     * precompIndex.numberOfDocuments - 1; ) { if (ii.hasNext() ) {
     * 
     * for( int f = -1; ( f = ii.nextDocument() ) != - 1; ) {
     * 
     * if ( vertical ) { if(fields.contains( properties.get( f ) ) ) { //
     * System.err.println( "From vertical index: " + properties.get( f ) );
     * disjuncts.add( new Select( properties.get( f ), term ) ); } else {
     * //System.err.println( "From horizontal  index: " + properties.get( f ) );
     * disjuncts.add( new Align( new Select( "token", term ), new Select(
     * "property", new Term ( f ) ) ) ); } }
     * 
     * 
     * } } else { //If the term is not in the fields that we indexed using the
     * vertical index, we look for it in the horizontal index disjuncts.add( new
     * Select( "token", term ) ); }
     * 
     * //ALERT NEW //if ( VERTICAL ) { disjuncts.add( new Select(
     * context.WURI_INDEX, term ) ); // disjuncts.add( new Select( "token", new
     * Term( token ) ) ); // disjuncts.add( new Select( "property", new Term(
     * token ) ) ); //} //else { // disjuncts.add( new Align( new Select(
     * "token", new Term( token ) ), new Select( "property", new Term (
     * context.WURI_INDEX) ) ) ); //} ii.dispose(); } }
     * 
     * if(disjuncts.size() > 0) conjuncts.add( new Or( disjuncts.toArray( new
     * Query[ disjuncts.size() ] ) ) ); }
     * 
     * 
     * 
     * 
     * // System.err.println( result );
     * 
     * return new And( conjuncts.toArray( new Query[ conjuncts.size() ] ) );
     * 
     * }
     */
    public class MyVisitor extends AbstractQueryBuilderVisitor<Query> {
	private boolean insideConsecutive = false;
	private boolean insideSelect = false;

	public Query[] newArray(int len) {
	    return new Query[len];
	}

	public QueryBuilderVisitor<Query> prepare() {
	    return this;
	}

	public boolean visitPre(Consecutive node) throws QueryBuilderVisitorException {
	    insideConsecutive = true;
	    return true;
	}

	public boolean visitPre(Select node) throws QueryBuilderVisitorException {
	    insideSelect = true;
	    return true;
	}

	@Override
	public Query visit(Term term) throws QueryBuilderVisitorException {

	    // Don't rewrite terms inside Consecutive
	    if (insideConsecutive || insideSelect) {
		return term;
	    }
	    // NOTE: this Term node might be already inside a Select
	    final ObjectArrayList<Query> disjuncts = new ObjectArrayList<Query>();

	    IndexIterator ii;
	    try {
		if (precompIndex != null) {
		    ii = precompIndex.documents(term.term);
		    if (ii.hasNext()) {
			for (int f = -1; (f = ii.nextDocument()) != -1;) {
			    if (fields.contains(properties.get(f))) {
				// System.err.println( "From vertical index: " +
				// properties.get( f ) );
				disjuncts.add(new Select(properties.get(f), term));
			    }
			}
		    }
		    ii.dispose();
		} else {
		    // No alignment index: we look in all fields
		    for (String field : fields) {
			disjuncts.add(new Select(field, term));
		    }

		}
		disjuncts.add(new Select(defaultField, term));
		disjuncts.add(new Select(uriField, term));
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	    return new Or(disjuncts.toArray(new Query[disjuncts.size()]));
	}

	@Override
	public Query visit(Prefix node) throws QueryBuilderVisitorException {
	    // TODO Auto-generated method stub
	    return node;
	}

	@Override
	public Query visit(Range node) throws QueryBuilderVisitorException {
	    // TODO Auto-generated method stub
	    return node;
	}

	@Override
	public Query visit(True node) throws QueryBuilderVisitorException {
	    // TODO Auto-generated method stub
	    return node;
	}

	@Override
	public Query visit(False node) throws QueryBuilderVisitorException {
	    // TODO Auto-generated method stub
	    return node;
	}

	public Query visitPost(And node, Query[] subNode) throws QueryBuilderVisitorException {
	    return new And(subNode);
	}

	public Query visitPost(Consecutive node, Query[] subNode) throws QueryBuilderVisitorException {
	    insideConsecutive = false;

	    if (insideSelect) {
		return new Consecutive(subNode);
	    }
	    // Create a disjunct of selects on all fields
	    final ObjectArrayList<Query> disjuncts = new ObjectArrayList<Query>();
	    for (String property : properties) {
		if (fields.contains(property))
		    disjuncts.add(new Select(property, new Consecutive(subNode)));

	    }
	    disjuncts.add(new Select(uriField, new Consecutive(subNode)));

	    return new Or(disjuncts.toArray(new Query[disjuncts.size()]));

	}

	public Query visitPost(OrderedAnd node, Query[] subNode) throws QueryBuilderVisitorException {
	    return new OrderedAnd(subNode);
	}

	public Query visitPost(Difference node, Query[] subNode) throws QueryBuilderVisitorException {
	    return new Difference(subNode[0], subNode[1]);
	}

	public Query visitPost(LowPass node, Query subNode) throws QueryBuilderVisitorException {
	    return new LowPass(subNode, node.k);
	}

	public Query visitPost(Not node, Query subNode) throws QueryBuilderVisitorException {
	    return new Not(subNode);
	}

	public Query visitPost(Or node, Query[] subNode) throws QueryBuilderVisitorException {
	    return new Or(subNode);
	}

	public Query visitPost(Align node, Query[] subNode) throws QueryBuilderVisitorException {
	    return new Align(subNode[0], subNode[1]);
	}

	public Query visitPost(MultiTerm node, Query[] subNode) throws QueryBuilderVisitorException {
	    // TODO: is this semantics sensible?
	    return new Or(subNode);
	}

	public Query visitPost(Select node, Query subNode) throws QueryBuilderVisitorException {
	    insideSelect = false;
	    return new Select(node.index, subNode);
	}

	public Query visitPost(Remap node, Query subNode) throws QueryBuilderVisitorException {
	    return new Remap(subNode, node.indexRemapping);
	}

	public Query visitPost(Weight node, Query subNode) throws QueryBuilderVisitorException {
	    return new Weight(node.weight, subNode);
	}

	// @Override
	// public Query visitPost(Expand node, Query subNodeResult)
	// throws QueryBuilderVisitorException {
	//
	// return new Expand(subNodeResult);
	// }

	@Override
	public MyVisitor copy() {
	    // TODO Auto-generated method stub
	    return new MyVisitor();
	}

    }

}
