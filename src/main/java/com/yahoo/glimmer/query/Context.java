package com.yahoo.glimmer.query;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

public class Context extends Properties {
    private static final long serialVersionUID = 8416864704849882837L;

    public static final String k1S = "k1";
    public static final String bS = "b";
    public static final String wMatchS = "w_matches";
    public static final String wfImportantS = "wf_important";
    public static final String wfUnimportantS = "wf_unimportant";
    public static final String wfNeutralS = "wf_neutral";
    public static final String wUriS = "wuri";
    public static final String wsImportantS = "ws_important";
    public static final String wsUnimportantS = "ws_unimportant";
    public static final String wsNeutralS = "ws_neutral";
    public static final String dlCutS = "dl_cutoff";

    // Config file location
    private String config_name;

    // NEEDED
    public String RUNS_FILE;
    public int RESULTS_CUTOFF;
    public String RUNS_NAME;
    public boolean NORMALISE_LENGTHS = false;
    public boolean USE_TIES;
    public String PROPERTY_INDEX;
    public String FIELD_LIST;
    public String QUERY_FILE;
    public boolean BREAK_TIES;
    public boolean LOAD_TFS;
    public boolean INCLUDE_CONTEXT;
    public boolean PRINT_ALL;
    public int REL_THRESHOLD;
    public int SIZE_TOP_K;
    public double b;
    public double k1;
    public String pathToIndex;
    public String pathToPriorRules;
    public String pathToDocumentPriors;
    public boolean remove_stopwords;
    public String SW_LIST;
    public String TITLE_LIST;
    public boolean LOAD_INDEXES_INTO_MEMORY;
    public String LOAD_DOCUMENT_SIZES;
    public double w_matches;
    private Properties pro = new Properties();
    public String TOKEN_INDEX;
    public String WURI_INDEX;

    public double wf_important;
    public int max_number_of_fields_norm;
    public double wf_unimportant;
    public double wf_neutral;
    public double wuri;
    public double ws_important;
    public double ws_unimportant;
    public double ws_neutral;
    public double dl_cutoff;
    public String COLLECTION;
    public String SEGMENTATION_CACHE;
    public String BLACKLIST_FILENAME;
    public String QRELS;
    public String MPH;
    public boolean store_cache;
    public String ALIGNMENT_INDEX;
    public boolean USE_SEGMENTS;
    public String ontoPath;
    public String multiIndexPath;

    public void load(String args) throws FileNotFoundException, IOException {
	config_name = args;
	InputStream fs;
	try {
	    fs = new FileInputStream(args);
	} catch (FileNotFoundException fnfe) {
	    URL resource = Context.class.getClassLoader().getResource(args);
	    fs = new FileInputStream(new File(resource.getFile()));
	}

	pro.load(fs);
	USE_TIES = Boolean.parseBoolean(pro.getProperty("use.ties", "true"));
	USE_SEGMENTS = Boolean.parseBoolean(pro.getProperty("use.segments", "true"));
	SIZE_TOP_K = Integer.parseInt(pro.getProperty("top.k", "1000"));
	pathToIndex = pro.getProperty("index.path");
	pathToPriorRules = pro.getProperty("prior.rules");
	pathToDocumentPriors = pro.getProperty("document.priors");
	QUERY_FILE = pro.getProperty("query.file");
	ALIGNMENT_INDEX = pro.getProperty("alignment.index");
	MPH = pro.getProperty("mph");
	b = Double.parseDouble(pro.getProperty("b", "0.75"));
	k1 = Double.parseDouble(pro.getProperty("k1", "1.2"));
	SW_LIST = pro.getProperty("sw.list");
	TITLE_LIST = pro.getProperty("title.list");
	remove_stopwords = Boolean.parseBoolean(pro.getProperty("remove.stopwords", "false"));
	// LOAD_DOCUMENT_SIZES =
	// Boolean.parseBoolean(pro.getProperty("load.sizes", "false"));
	LOAD_DOCUMENT_SIZES = pro.getProperty("load.sizes", "false");
	LOAD_INDEXES_INTO_MEMORY = Boolean.parseBoolean(pro.getProperty("load.memory", "false"));
	w_matches = Double.parseDouble(pro.getProperty("w.matches", "1"));
	TOKEN_INDEX = pro.getProperty("token.index");
	WURI_INDEX = pro.getProperty("wuri.index");
	PROPERTY_INDEX = pro.getProperty("property.index");
	FIELD_LIST = pro.getProperty("field.list");
	COLLECTION = pro.getProperty("collection");
	wf_important = Double.parseDouble(pro.getProperty("wf.important", "1"));
	wf_unimportant = Double.parseDouble(pro.getProperty("wf.unimportant", "1"));
	wf_neutral = Double.parseDouble(pro.getProperty("wf.neutral", "1"));
	wuri = Double.parseDouble(pro.getProperty("wuri", "1"));
	ws_important = Double.parseDouble(pro.getProperty("ws.important", "1"));
	ws_unimportant = Double.parseDouble(pro.getProperty("ws.unimportant", "1"));
	ws_neutral = Double.parseDouble(pro.getProperty("ws.neutral", "1"));
	dl_cutoff = Double.parseDouble(pro.getProperty("dl.cutoff", "100"));
	max_number_of_fields_norm = Integer.parseInt(pro.getProperty("max.norm", "5"));
	RESULTS_CUTOFF = Integer.parseInt(pro.getProperty("min.results", "100"));
	SEGMENTATION_CACHE = pro.getProperty("segmentation.cache");
	BLACKLIST_FILENAME = pro.getProperty("blacklist.filename", "blacklist.txt");
	QRELS = pro.getProperty("qrels");
	RUNS_FILE = pro.getProperty("runs.file");
	RUNS_NAME = pro.getProperty("run.name", "Y!NLRABCN");
	store_cache = Boolean.parseBoolean(pro.getProperty("store.cache", "true"));
	ontoPath = pro.getProperty("ontology.path");
	multiIndexPath = pro.getProperty("multiindex.path");
    }

    public void reload() throws FileNotFoundException, IOException {
	// must be loaded first
	if (config_name != null)
	    load(config_name);
    }

    public Context(String args) throws FileNotFoundException, IOException {
	load(args);
    }

    public String get(String key) {

	return pro.getProperty(key);
    }

    public void put(String key, String value) {
	pro.setProperty(key, value);
    }

    public void update(HttpServletRequest request) {
	// Override context based on request params
	if (request != null) {
	    String sk1 = request.getParameter(k1S);
	    if (sk1 != null && !sk1.equals("")) {
		k1 = Double.parseDouble(sk1);
	    }
	    String sb = request.getParameter(bS);
	    if (sb != null && !sb.equals("")) {
		b = Double.parseDouble(sb);
	    }
	    String sw_matches = request.getParameter(wMatchS);
	    if (sw_matches != null && !sw_matches.equals("")) {
		w_matches = Double.parseDouble(sw_matches);
	    }
	    String swf_important = request.getParameter(wfImportantS);
	    if (swf_important != null && !swf_important.equals("")) {
		wf_important = Double.parseDouble(swf_important);
	    }
	    String swf_unimportant = request.getParameter(wfUnimportantS);
	    if (swf_unimportant != null && !swf_unimportant.equals("")) {
		wf_unimportant = Double.parseDouble(swf_unimportant);
	    }
	    String swf_neutral = request.getParameter(wfNeutralS);
	    if (swf_neutral != null && !swf_neutral.equals("")) {
		wf_neutral = Double.parseDouble(swf_neutral);
	    }
	    String swuri = request.getParameter(wUriS);
	    if (swuri != null && !swuri.equals("")) {
		wuri = Double.parseDouble(swuri);
	    }
	    String sws_important = request.getParameter(wsImportantS);
	    if (sws_important != null && !sws_important.equals("")) {
		ws_important = Double.parseDouble(sws_important);
	    }
	    String sws_unimportant = request.getParameter(wsUnimportantS);
	    if (sws_unimportant != null && !sws_unimportant.equals("")) {
		ws_unimportant = Double.parseDouble(sws_unimportant);
	    }
	    String sws_neutral = request.getParameter(wsNeutralS);
	    if (sws_neutral != null && !sws_neutral.equals("")) {
		ws_neutral = Double.parseDouble(sws_neutral);
	    }
	    String sdl_cutoff = request.getParameter(dlCutS);
	    if (sdl_cutoff != null && !sdl_cutoff.equals("")) {
		dl_cutoff = Double.parseDouble(sdl_cutoff);
	    }
	}

    }

}