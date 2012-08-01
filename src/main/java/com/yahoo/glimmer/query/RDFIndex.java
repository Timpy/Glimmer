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

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceOpenHashMap;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.di.mg4j.document.Document;
import it.unimi.di.mg4j.document.DocumentCollection;
import it.unimi.di.mg4j.index.BitStreamIndex;
import it.unimi.di.mg4j.index.DiskBasedIndex;
import it.unimi.di.mg4j.index.Index;
import it.unimi.di.mg4j.index.Index.UriKeys;
import it.unimi.di.mg4j.index.TermProcessor;
import it.unimi.di.mg4j.query.QueryEngine;
import it.unimi.di.mg4j.query.SelectedInterval;
import it.unimi.di.mg4j.query.nodes.Query;
import it.unimi.di.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.mg4j.search.score.CountScorer;
import it.unimi.di.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.mg4j.search.score.Scorer;
import it.unimi.dsi.sux4j.io.FileLinesList;
import it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.util.SemiExternalGammaList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import com.yahoo.glimmer.indexing.ConcatenatedDocumentCollection;
import com.yahoo.glimmer.indexing.SimpleCompressedDocumentCollection;
import com.yahoo.glimmer.indexing.TitleListDocumentCollection;
import com.yahoo.glimmer.util.Util;

public class RDFIndex {
    private final static Logger LOGGER = Logger.getLogger(RDFIndex.class);
    public final static int MAX_STEMMING = 1024;

    /** The query engine. */
    private QueryEngine queryEngine;
    /** The document collection. */
    private DocumentCollection documentCollection = null;
    /** An optional title list if the document collection is not present. */
    /** The token index */
    protected BitStreamIndex indexIdfs;
    /** Term counts in the token index */
    protected SemiExternalGammaList frequencies = null;
    /** Document priors */
    protected HashMap<Integer, Integer> documentPriors = null;
    /** MPH used to encode URIs for retrieving from the collection */
    protected LcpMonotoneMinimalPerfectHashFunction<CharSequence> subjectsMPH;
    /** The alignment index **/
    protected Index precompIndex;
    /** The predicate index **/
    protected Index predicateIndex;
    /** Query logger for performance measurement */
    private QueryLogger queryLogger;
    protected String tokenField = "token";
    protected String uriField;
    /**
     * All fields (includes non-indexed fields) This is a list because it's used
     * to look up field names by position.
     */
    private List<String> fields;

    protected IndexStatistics stats;

    protected RDFQueryParser parser;

    public RDFIndex(Context context) {
	init(context);
    }

    @SuppressWarnings("unchecked")
    private void init(Context context){
	
	// Load the collection or titlelist
	try {
	    if (context.getCollection() != null) {
		LOGGER.info("Loading collection from " + context.getCollection());

		// Check if collection is a file or directory
		String collectionString = context.getCollection();
		if (new File(collectionString).isFile()) {
		    documentCollection = (it.unimi.di.mg4j.document.SimpleCompressedDocumentCollection) BinIO.loadObject(collectionString);
		    documentCollection.filename(collectionString);

		} else {
		    // A directory of collections
		    // We are using our own SimpleCompressed.. class

		    // HACK: add file separator
		    if (!collectionString.endsWith(System.getProperty("file.separator"))) {
			collectionString = collectionString + System.getProperty("file.separator");
		    }
		    List<String> names = new ArrayList<String>();
		    List<DocumentCollection> collections = new ArrayList<DocumentCollection>();

		    String[] fileNames = new File(collectionString).list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
			    if (name.endsWith(".collection"))
				return true;
			    return false;
			}

		    });
		    if (fileNames == null) {
			throw new RuntimeException("Collection directory is invalid");
		    }
		    // Sort names to get the collection order right
		    Arrays.sort(fileNames);
		    for (String file : fileNames) {

			SimpleCompressedDocumentCollection collection = (SimpleCompressedDocumentCollection) BinIO.loadObject(collectionString + file);
			// TODO: disable this line next time I regenerate the
			// collection
			collection.basename = file.substring(0, file.lastIndexOf('.'));
			collection.filename(collectionString + file);
			names.add(collectionString + file);
			collections.add(collection);

		    }
		    ;
		    documentCollection = new ConcatenatedDocumentCollection(names.toArray(new String[] {}), collections.toArray(new DocumentCollection[] {}));
		}
	    }
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}

	if (documentCollection == null) {
	    LOGGER.info("No collection specified, we will try to use a title list...");

	    if (context.getTitleList() != null && !context.getTitleList().equals("")) {
		LOGGER.info("Loading titlelist...");
		List<MutableString> titleList;
		try {
		    titleList = new FileLinesList(context.getTitleList(), "ASCII");
		    LOGGER.info("Loaded titlelist of size " + titleList.size() + ".");
		    documentCollection = new TitleListDocumentCollection(titleList);
		} catch (Exception e) {
		    throw new IllegalArgumentException(e);
		}
	    }
	}

	// Load MPH
	if (context.getMph() == null || context.getMph().equals("")) {
	    LOGGER.warn("Warning, no mph specified!");
	} else {
	    LOGGER.info("Loading Minimal Perfect Hash (MPH)...");
	    try {
		subjectsMPH = (LcpMonotoneMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(context.getMph());
	    } catch (IOException e) {
		e.printStackTrace();
	    } catch (ClassNotFoundException e) {
		e.printStackTrace();
	    }
	    // System.out.println("Loaded MPH of size " + mph.size());
	}

	EnumMap<UriKeys, String> map = new EnumMap<UriKeys, String>(UriKeys.class);

	if (context.getLOAD_INDEXES_INTO_MEMORY())
	    map.put(UriKeys.INMEMORY, "true");
	else
	    map.put(UriKeys.MAPPED, "true");

	// Load the indices
	if (context.getPathToIndex() == null || context.getPathToIndex().equals("")) {
	    throw new IllegalArgumentException("<index> is a mandatory servlet init parameter");
	}

	final String[] basenameWeight;
	if (context.getPathToIndex().endsWith(System.getProperty("file.separator"))) {
	    // List .index files in directory
	    File[] indexFiles = new File(context.getPathToIndex()).listFiles(new FilenameFilter() {
		public boolean accept(File dir, String name) {
		    if (name.endsWith(".properties"))
			return true;
		    return false;
		}
	    });
	    basenameWeight = new String[indexFiles.length];

	    for (int i = 0; i < indexFiles.length; i++) {
		LOGGER.info("Loading index: '" + indexFiles[i] + "'");
		String baseName = indexFiles[i].getName().substring(0, indexFiles[i].getName().lastIndexOf('.'));
		basenameWeight[i] = indexFiles[i].getParent() + System.getProperty("file.separator") + baseName;
	    }
	} else {
	    // Single base name
	    basenameWeight = new String[] { context.getPathToIndex() };
	}

	Object2ReferenceMap<String, Index> indexMap;
	try {
	    // This method also loads weights from the index URI
	    // We ignore these weights
	    Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
	    indexMap = loadIndicesFromSpec(basenameWeight, context.getLOAD_DOCUMENT_SIZES(), documentCollection, index2Weight, context.getLOAD_DOCUMENT_SIZES(), map);
	} catch (Exception e) {
	    throw new IllegalArgumentException(e);
	}

	LOGGER.info("Loaded " + basenameWeight.length + " indices.");

	if (context != null && context.getTokenIndex() != null) {
	    indexIdfs = (BitStreamIndex) indexMap.get(context.getTokenIndex());
	    if (indexIdfs == null) {
		// could always load sizes!
		try {
		    LOGGER.info("Loading token index.");
		    indexIdfs = (BitStreamIndex) DiskBasedIndex.getInstance(context.getTokenIndex(), true, true, true, map);
		} catch (Exception e) {
		    throw new RuntimeException(e);

		}
		indexMap.put(tokenField, indexIdfs);
	    }
	}

	// Load field list
	if (context != null && context.getFieldList() != null) {
	    fields = new ArrayList<String>();
	    LOGGER.info("Loading field list from " + context.getFieldList());
	    for (MutableString line : new FileLinesCollection(context.getFieldList(), "UTF-8")) {
		fields.add(Util.encodeFieldName(line.toString()));
	    }
	}

	// Load the alignment index
	try {
	    LOGGER.info("Loading alignment index from " + context.getAlignmentIndex());
	    precompIndex = Index.getInstance(context.getAlignmentIndex() + "?mapped=1");
	} catch (Exception e) {
	    LOGGER.error("Failed to load alignment index", e);
	}

	// Load the predicate index
	try {
	    LOGGER.info("Loading predicate index from " + context.getPropertyIndex());
	    predicateIndex = Index.getInstance(context.getPropertyIndex() + "?mapped=1");
	} catch (Exception e) {
	    throw new IllegalArgumentException(e);
	}

	if (context != null && context.getWuriIndex() != null && indexMap.get(context.getWuriIndex()) == null) {
	    try {
		LOGGER.info("Loading uri index from " + context.getWuriIndex());
		uriField = context.getWuriIndex();
		Index index_wuri = (BitStreamIndex) DiskBasedIndex.getInstance(uriField, true, false, true, map);
		indexMap.put(uriField, index_wuri);
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	} else {
	    LOGGER.warn("WURI Index is null, tried to load from " + context.getWuriIndex());
	}

	// Loading frequencies
	if (context != null && context.getTokenIndex() != null) {
	    try {
		LOGGER.info("Loading frequencies from " + context.getTokenIndex() + DiskBasedIndex.FREQUENCIES_EXTENSION);
		frequencies = new SemiExternalGammaList(new InputBitStream(context.getTokenIndex() + DiskBasedIndex.FREQUENCIES_EXTENSION), 1,
			indexIdfs.termMap.size());
		if (frequencies.size() != indexIdfs.numberOfDocuments) {
		    LOGGER.warn("Loaded " + frequencies.size() + " frequency values but index_idfs.numberOfDocuments is " + indexIdfs.numberOfDocuments);
		}
	    } catch (Exception e) {
		LOGGER.error("Failed to load token index: " + context.getTokenIndex());
		throw new IllegalArgumentException(e);
	    }
	} else {
	    LOGGER.warn("Token index is null");
	}

	// This is empty for non-payload indices
	final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();

	queryEngine = new QueryEngine(null, // we will only pass in parsed
					    // queries
		new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(((Object2ReferenceLinkedOpenHashMap<String, Index>) indexMap)
			.firstKey()), MAX_STEMMING), indexMap);

	// We set up an interval selector only if there is a collection for
	// snippeting
	// queryEngine.intervalSelector = documentCollection != null ? new
	// IntervalSelector(4, 40): new IntervalSelector();
	queryEngine.multiplex = false;
	queryEngine.intervalSelector = null;

	// Load priors
	if (context != null && context.getPathToDocumentPriors() != null) {
	    LOGGER.info("Loading priors from " + context.getPathToDocumentPriors());
	    try {
		documentPriors = (HashMap<Integer, Integer>) BinIO.loadObject(context.getPathToDocumentPriors());
	    } catch (Exception e) {
		LOGGER.warn("Failed to load priors", e);
	    }
	} else {
	    LOGGER.info("Path to priors is null");
	}

	// Sets field weight and scorer
	reconfigure(context);

	// Configure the query logger
	queryLogger = new QueryLogger();

	// Init query parser
	final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(getIndexedFields().size());
	for (String alias : getIndexedFields())
	    termProcessors.put(alias, getField(alias).termProcessor);
	parser = new RDFQueryParser(getAlignmentIndex(), getAllFields(), getIndexedFields(), "token", context.getWuriIndex(), termProcessors, getSubjectsMPH());

	// Compute stats
	try {
	    stats = new IndexStatistics(this);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}

	// Load the ontology if provided
	if (context.getOntoPath() != null) {
	    OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
	    File owlOntologyFile = new File(context.getOntoPath());
	    if (!owlOntologyFile.exists()) {
		URL owlOntologyUrl = this.getClass().getClassLoader().getResource(context.getOntoPath());
		if (owlOntologyUrl != null) {
		    owlOntologyFile = new File(owlOntologyUrl.getFile());
		}
	    }
	    try {
		OWLOntology onto = manager.loadOntologyFromOntologyDocument(owlOntologyFile);
		stats.loadInfoFromOntology(onto);
	    } catch (OWLOntologyCreationException e) {
		throw new IllegalArgumentException("Ontology failed to load:" + e.getMessage());
	    }
	}

    }

    /**
     * Parses a given array of index URIs/weights, loading the correspoding
     * indices and writing the result of parsing in the given maps.
     * 
     * @param basenameWeight
     *            an array of index URIs of the form
     *            <samp><var>uri</var>[:<var>weight</var>]</samp>, specifying
     *            the URI of an index and the weight for the index (1, if
     *            missing).
     * @param loadSizes
     *            forces size loading.
     * @param documentCollection
     *            an optional document collection, or <code>null</code>.
     * @param name2Index
     *            an empty, writable map that will be filled with pairs given by
     *            an index basename (or field name, if available) and an
     *            {@link Index}.
     * @param index2Weight
     *            an empty, writable map that will be filled with a map from
     *            indices to respective weights.
     */
    protected Object2ReferenceMap<String, Index> loadIndicesFromSpec(final String[] basenameWeight, boolean loadSizes,
	    final DocumentCollection documentCollection, final Reference2DoubleMap<Index> index2Weight, boolean sizes, EnumMap<UriKeys, String> map)
	    throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException,
	    IllegalAccessException, InvocationTargetException, NoSuchMethodException {

	Object2ReferenceLinkedOpenHashMap<String, Index> name2Index = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);

	for (int i = 0; i < basenameWeight.length; i++) {

	    // We must be careful, as ":" is used by Windows to separate the
	    // device from the path.
	    final int split = basenameWeight[i].lastIndexOf(':');
	    double weight = 1;

	    if (split != -1) {
		try {
		    weight = Double.parseDouble(basenameWeight[i].substring(split + 1));
		} catch (NumberFormatException e) {
		}
	    }

	    final Index index;

	    if (split == -1 || basenameWeight[i].startsWith("mg4j://")) {
		// index = Index.getInstance(basenameWeight[i], true,
		// loadSizes);

		// System.out.println("BASENAME: " + basenameWeight[i]);
		try {
		    index = (BitStreamIndex) DiskBasedIndex.getInstance(basenameWeight[i], true, sizes, true, map);
		    index2Weight.put(index, 1);
		} catch (ArrayIndexOutOfBoundsException e) {
		    // Empty index
		    System.err.println("Failed to open index: " + basenameWeight[i]);
		    continue;
		}
	    } else {
		index = (BitStreamIndex) DiskBasedIndex.getInstance(basenameWeight[i], true, sizes, true, map);
		// index = Index.getInstance(basenameWeight[i].substring(0,
		// split));
		index2Weight.put(index, weight);
	    }
	    /*
	     * if (documentCollection != null && index.numberOfDocuments !=
	     * documentCollection.size()) throw new
	     * IllegalArgumentException("Index " + index + " has " +
	     * index.numberOfDocuments +
	     * " documents, but the document collection has size " +
	     * documentCollection.size());
	     */
	    name2Index.put(index.field != null ? index.field : basenameWeight[i], index);
	}
	return name2Index;
    }

    private Reference2DoubleOpenHashMap<Index> loadB(Context context) {
	Reference2DoubleOpenHashMap<Index> b = new Reference2DoubleOpenHashMap<Index>();

	double db = context.getB();

	for (String indexName : getIndexedFields()) {
	    // TODO load from file if needed
	    b.put(getField(indexName), db);
	}
	b.put(indexIdfs, db);
	return b;
    }

    /**
     * Compute index weights from context
     * 
     * 
     * @param context
     * @return
     */
    private Reference2DoubleOpenHashMap<Index> loadWeights(Context context) {
	Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();

	Object2ReferenceMap<String, Index> indexMap = queryEngine.indexMap;

	for (String index : indexMap.keySet()) {
	    String w = context.getString("w." + index);
	    if (w == null) { // unimportant
		index2Weight.put((Index) indexMap.get(index), context.getwf_unimportant() * indexMap.keySet().size());
	    } else {
		if (w.equals(SetDocumentPriors.IMPORTANT))
		    index2Weight.put((Index) indexMap.get(index), context.getwf_important() * indexMap.keySet().size());
		else if (w.equals(SetDocumentPriors.UNIMPORTANT))
		    index2Weight.put((Index) indexMap.get(index), context.getwf_unimportant() * indexMap.keySet().size());
		else if (w.equals(SetDocumentPriors.NEUTRAL))
		    index2Weight.put((Index) indexMap.get(index), context.getwf_neutral() * indexMap.keySet().size());
	    }
	}
	if (context.getWuriIndex() != null) {
	    index2Weight.put((Index) indexMap.get(context.getWuriIndex()), context.getwuri() * indexMap.keySet().size());

	}
	// System.out.println("Final weights:"+index2Weight);
	return index2Weight;
    }

    protected Scorer configureScorer(Context context) throws FileNotFoundException, IOException {

	Reference2DoubleOpenHashMap<Index> bByIndex = loadB(context);

	double[] documentWeights = new double[3];
	documentWeights[Integer.parseInt(SetDocumentPriors.IMPORTANT)] = context.getws_important();
	documentWeights[Integer.parseInt(SetDocumentPriors.UNIMPORTANT)] = context.getws_unimportant();
	documentWeights[Integer.parseInt(SetDocumentPriors.NEUTRAL)] = context.getws_neutral();

	return new WOOScorer(context.getK1(), bByIndex, indexIdfs.termMap, frequencies, indexIdfs.sizes, (double) indexIdfs.numberOfOccurrences
		/ indexIdfs.numberOfDocuments, indexIdfs.numberOfDocuments, context.getw_matches(), documentWeights, context.getdl_cutoff(), documentPriors,
		context.getmax_number_of_fields_norm());

    }

    /**
     * We partially reinitialize the index: we reload the weights and the scorer
     * 
     * @param context
     */
    public void reconfigure(Context context) {
	// Recomputes index weights
	queryEngine.setWeights(loadWeights(context));

	// Configure scorer
	try {

	    // Configure scorer
	    Scorer scorer = configureScorer(context);
	    queryEngine.score(scorer);
	    // Only valid if we have a scorer
	    // ALERT WTF
	    // queryEngine.equalize( context.SIZE_TOP_K );
	} catch (Exception e) {
	    e.printStackTrace();
	    System.err.println("WOO Scorer failed to configure, using default scorer");
	    queryEngine.score(new CountScorer());
	    System.exit(-1);
	}

    }

    /**
     * The indexed fields, including the token and uri fields of the horizontal
     * index.
     * 
     * @return
     */
    public Set<String> getIndexedFields() {
	return queryEngine.indexMap.keySet();
    }

    /**
     * All the potential fields in the vertical index. Only the subset returned
     * by {@link #getIndexedFields()} are indexed.
     * 
     * Note: this does not include the fields of the horizontal index.
     * 
     * @return
     */
    public List<String> getAllFields() {
	return fields;
    }

    public BitStreamIndex getField(String alias) {
	return (BitStreamIndex) queryEngine.indexMap.get(alias);
    }

    public long getDocID(String uri) {
	return subjectsMPH.get(uri);
    }

    public DocumentCollection getCollection() {
	return documentCollection;
    }

    public BitStreamIndex getPredicateIndex() {
	return (BitStreamIndex) predicateIndex;
    }

    public BitStreamIndex getAlignmentIndex() {
	return (BitStreamIndex) precompIndex;
    }

    public LcpMonotoneMinimalPerfectHashFunction<CharSequence> getSubjectsMPH() {
	return subjectsMPH;
    }

    public String getDefaultField() {
	return tokenField;
    }

    public String getURIField() {
	return uriField;
    }

    public String getTitle(int id) throws IOException {
	String title;
	Document d = documentCollection.document(id);
	title = ((MutableString) d.title()).toString();
	d.close();
	return title;
    }

    public IndexStatistics getStatistics() {
	return stats;
    }

    public RDFQueryParser getParser() {
	return parser;
    }

    public int process(final Query[] query, final int offset, final int length,
	    final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results) throws QueryBuilderVisitorException, IOException {
	return queryEngine.copy().process(query, offset, length, results);
    }

    public int process(final Query query, final int offset, final int length,
	    final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results) throws QueryBuilderVisitorException, IOException {
	return queryEngine.copy().process(query, offset, length, results);
    }

    public void destroy() {
	try {
	    if (documentCollection != null)
		documentCollection.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public QueryLogger getQueryLogger() {
	return queryLogger;
    }
    
    public BitStreamIndex getIndexIdfs() {
	return indexIdfs;
    }
}
