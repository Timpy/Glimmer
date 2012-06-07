package com.yahoo.glimmer.query;

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
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentCollection;
import it.unimi.dsi.mg4j.index.BitStreamIndex;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.mg4j.index.Index.UriKeys;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.query.QueryEngine;
import it.unimi.dsi.mg4j.query.SelectedInterval;
import it.unimi.dsi.mg4j.query.nodes.Query;
import it.unimi.dsi.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.dsi.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.dsi.mg4j.search.score.CountScorer;
import it.unimi.dsi.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.mg4j.search.score.Scorer;
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
import com.yahoo.glimmer.indexing.RDFDocumentFactory;
import com.yahoo.glimmer.indexing.SimpleCompressedDocumentCollection;
import com.yahoo.glimmer.indexing.TitleListDocumentCollection;

public class RDFIndex {
    private final static Logger LOGGER = Logger.getLogger(RDFIndex.class);
    public final static int MAX_STEMMING = 1024;

    /** The query engine. */
    private QueryEngine queryEngine;
    /** The document collection. */
    private DocumentCollection documentCollection = null;
    /** An optional title list if the document collection is not present. */
    /** The token index */
    protected BitStreamIndex index_idfs;
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

    private void init(Context context) {
	init(context, context.COLLECTION, context.TITLE_LIST, context.pathToIndex, context.LOAD_DOCUMENT_SIZES, context.MPH);
    }

    @SuppressWarnings("unchecked")
    private void init(Context context, String collectionString, String titleListString, String indexString, String loadSizesString, String mphString) {
	// Load the collection or titlelist
	try {
	    if (collectionString != null) {
		LOGGER.info("Loading collection from " + collectionString);

		// Check if collection is a file or directory
		if (new File(collectionString).isFile()) {
		    documentCollection = (it.unimi.dsi.mg4j.document.SimpleCompressedDocumentCollection) BinIO.loadObject(collectionString);
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

	    if (titleListString != null && !titleListString.equals("")) {
		LOGGER.info("Loading titlelist...");
		List<MutableString> titleList;
		try {
		    titleList = new FileLinesList(titleListString, "ASCII");
		    LOGGER.info("Loaded titlelist of size " + titleList.size() + ".");
		    documentCollection = new TitleListDocumentCollection(titleList);
		} catch (Exception e) {
		    throw new IllegalArgumentException(e);
		}
	    }
	}

	// Load MPH
	if (mphString == null || mphString.equals("")) {
	    LOGGER.warn("Warning, no mph specified!");
	} else {
	    LOGGER.info("Loading Minimal Perfect Hash (MPH)...");
	    try {
		subjectsMPH = (LcpMonotoneMinimalPerfectHashFunction<CharSequence>) BinIO.loadObject(mphString);
	    } catch (IOException e) {
		e.printStackTrace();
	    } catch (ClassNotFoundException e) {
		e.printStackTrace();
	    }
	    // System.out.println("Loaded MPH of size " + mph.size());
	}

	EnumMap<UriKeys, String> map = new EnumMap<UriKeys, String>(UriKeys.class);

	if (context.LOAD_INDEXES_INTO_MEMORY)
	    map.put(UriKeys.INMEMORY, "true");
	else
	    map.put(UriKeys.MAPPED, "true");

	// Load the indices
	if (indexString == null || indexString.equals("")) {
	    throw new IllegalArgumentException("<index> is a mandatory servlet init parameter");
	}

	final String[] basenameWeight;
	if (indexString.endsWith(System.getProperty("file.separator"))) {
	    // List .index files in directory
	    File[] indexFiles = new File(indexString).listFiles(new FilenameFilter() {
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
	    basenameWeight = new String[] { indexString };
	}

	Object2ReferenceMap<String, Index> indexMap;
	try {
	    boolean loadSizes = false;
	    if (loadSizesString != null && !loadSizesString.equals("")) {
		loadSizes = Boolean.parseBoolean(loadSizesString);
	    }
	    // This method also loads weights from the index URI
	    // We ignore these weights
	    Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
	    indexMap = loadIndicesFromSpec(basenameWeight, loadSizes, documentCollection, index2Weight, loadSizes, map);
	} catch (Exception e) {
	    throw new IllegalArgumentException(e);
	}

	LOGGER.info("Loaded " + basenameWeight.length + " indices.");

	if (context != null && context.TOKEN_INDEX != null) {
	    index_idfs = (BitStreamIndex) indexMap.get(context.TOKEN_INDEX);
	    if (index_idfs == null) {
		// could always load sizes!
		try {
		    LOGGER.info("Loading token index.");
		    index_idfs = (BitStreamIndex) DiskBasedIndex.getInstance(context.TOKEN_INDEX, true, true, true, map);
		} catch (Exception e) {
		    throw new RuntimeException(e);

		}
		indexMap.put(tokenField, index_idfs);
	    }
	}

	// Load field list
	if (context != null && context.FIELD_LIST != null) {
	    fields = new ArrayList<String>();
	    LOGGER.info("Loading field list from " + context.FIELD_LIST);
	    for (java.util.Iterator<it.unimi.dsi.lang.MutableString> iterator = new it.unimi.dsi.io.FileLinesCollection(context.FIELD_LIST, "UTF-8").iterator(); iterator
		    .hasNext();)
		fields.add(RDFDocumentFactory.encodeFieldName(iterator.next().toString()));
	}

	// Load the alignment index
	try {
	    LOGGER.info("Loading alignment index from " + context.ALIGNMENT_INDEX);
	    precompIndex = Index.getInstance(context.ALIGNMENT_INDEX + "?mapped=1");
	} catch (Exception e) {
	    LOGGER.error("Failed to load alignment index", e);
	}

	// Load the predicate index
	try {
	    LOGGER.info("Loading predicate index from " + context.PROPERTY_INDEX);
	    predicateIndex = Index.getInstance(context.PROPERTY_INDEX + "?mapped=1");
	} catch (Exception e) {
	    throw new IllegalArgumentException(e);
	}

	if (context != null && context.WURI_INDEX != null && indexMap.get(context.WURI_INDEX) == null) {
	    try {
		LOGGER.info("Loading uri index from " + context.WURI_INDEX);
		uriField = context.WURI_INDEX;
		Index index_wuri = (BitStreamIndex) DiskBasedIndex.getInstance(uriField, true, false, true, map);
		indexMap.put(uriField, index_wuri);
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	} else {
	    LOGGER.warn("WURI_INDEX is null, tried to load from " + context.WURI_INDEX);
	}

	// Loading frequencies
	if (context != null && context.TOKEN_INDEX != null) {
	    try {
		LOGGER.info("Loading frequencies from " + context.TOKEN_INDEX + DiskBasedIndex.FREQUENCIES_EXTENSION);
		frequencies = new SemiExternalGammaList(new InputBitStream(context.TOKEN_INDEX + DiskBasedIndex.FREQUENCIES_EXTENSION), 1,
			index_idfs.termMap.size());
		if (frequencies.size() != index_idfs.numberOfDocuments) {
		    LOGGER.warn("Loaded " + frequencies.size() + " frequency values but index_idfs.numberOfDocuments is " + index_idfs.numberOfDocuments);
		}
	    } catch (Exception e) {
		LOGGER.error("Failed to load token index: " + context.TOKEN_INDEX);
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
	if (context != null && context.pathToDocumentPriors != null) {
	    LOGGER.info("Loading priors from " + context.pathToDocumentPriors);
	    try {
		documentPriors = (HashMap<Integer, Integer>) BinIO.loadObject(context.pathToDocumentPriors);
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
	parser = new RDFQueryParser(getAlignmentIndex(), getAllFields(), getIndexedFields(), "token", context.WURI_INDEX, termProcessors, getSubjectsMPH());

	// Compute stats
	try {
	    stats = new IndexStatistics(this);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}

	// Load the ontology if provided
	if (context.ontoPath != null) {
	    OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
	    File owlOntologyFile = new File(context.ontoPath);
	    if (!owlOntologyFile.exists()) {
		URL owlOntologyUrl = this.getClass().getClassLoader().getResource(context.ontoPath);
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

	double db = context.b;

	for (String indexName : getIndexedFields()) {
	    // TODO load from file if needed
	    b.put(getField(indexName), db);
	}
	b.put(index_idfs, db);
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
	    String w = context.get("w." + index);
	    if (w == null) { // unimportant
		index2Weight.put((Index) indexMap.get(index), context.wf_unimportant * indexMap.keySet().size());
	    } else {
		if (w.equals(SetDocumentPriors.IMPORTANT))
		    index2Weight.put((Index) indexMap.get(index), context.wf_important * indexMap.keySet().size());
		else if (w.equals(SetDocumentPriors.UNIMPORTANT))
		    index2Weight.put((Index) indexMap.get(index), context.wf_unimportant * indexMap.keySet().size());
		else if (w.equals(SetDocumentPriors.NEUTRAL))
		    index2Weight.put((Index) indexMap.get(index), context.wf_neutral * indexMap.keySet().size());
	    }
	}
	if (context.WURI_INDEX != null) {
	    index2Weight.put((Index) indexMap.get(context.WURI_INDEX), context.wuri * indexMap.keySet().size());

	}
	// System.out.println("Final weights:"+index2Weight);
	return index2Weight;
    }

    protected Scorer configureScorer(Context context) throws FileNotFoundException, IOException {

	Reference2DoubleOpenHashMap<Index> bByIndex = loadB(context);

	double[] documentWeights = new double[3];
	documentWeights[Integer.parseInt(SetDocumentPriors.IMPORTANT)] = context.ws_important;
	documentWeights[Integer.parseInt(SetDocumentPriors.UNIMPORTANT)] = context.ws_unimportant;
	documentWeights[Integer.parseInt(SetDocumentPriors.NEUTRAL)] = context.ws_neutral;

	return new WOOScorer(context.k1, bByIndex, index_idfs.termMap, frequencies, index_idfs.sizes, (double) index_idfs.numberOfOccurrences
		/ index_idfs.numberOfDocuments, index_idfs.numberOfDocuments, context.w_matches, documentWeights, context.dl_cutoff, documentPriors,
		context.max_number_of_fields_norm);

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
}
