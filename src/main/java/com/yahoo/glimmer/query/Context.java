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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class Context extends Properties {
    private static final long serialVersionUID = 8416864704849882837L;
    
    public static final String ALIGNMENT_INDEX_KEY = "alignment.index";
    public static final String B_TAG = "b";
    public static final String BLACKLIST_FILENAME_KEY = "blacklist.filename";
    public static final String COLLECTION_KEY = "collection";
    public static final String DL_CUTOFF_TAG = "dl.cutoff";
    public static final String DOCUMENT_PRIORS_KEY = "document.priors";
    public static final String FIELD_LIST_KEY = "field.list";
    public static final String INDEX_PATH_KEY = "index.path";
    public static final String K1_TAG = "k1";
    public static final String LOAD_MEMORY_TAG = "load.memory";
    public static final String LOAD_SIZES_TAG = "load.sizes";
    public static final String MAX_NORM_TAG = "max.norm";
    public static final String MIN_RESULTS_TAG = "min.results";
    public static final String MPH_KEY = "mph";
    public static final String MULTIINDEX_PATH_KEY = "multiindex.path";
    public static final String MULTIINDEX_DIR_PREFIX_KEY = "multiindex.dirprefix";
    public static final String ONTOLOGY_PATH_KEY = "ontology.path";
    public static final String PRIOR_RULES_KEY = "prior.rules";
    public static final String PROPERTY_INDEX_KEY = "property.index";
    public static final String QRELS_KEY = "qrels";
    public static final String QUERY_FILE_KEY = "query.file";
    public static final String REMOVE_STOPWORDS_TAG = "remove.stopwords";
    public static final String RUN_NAME_TAG = "run.name";
    public static final String RUNS_FILE_KEY = "runs.file";
    public static final String SEGMENTATION_CACHE_KEY = "segmentation.cache";
    public static final String STORE_CACHE_TAG = "store.cache";
    public static final String SW_LIST_KEY = "sw.list";
    public static final String TITLE_LIST_KEY = "title.list";
    public static final String TOKEN_INDEX_KEY = "token.index";
    public static final String TOP_K_KEY = "top.k";
    public static final String USE_SEGMENTS_KEY = "use.segments";
    public static final String USE_TIES_KEY = "use.ties";
    public static final String W_MATCHES_TAG = "w.matches";
    public static final String WF_IMPORTANT_TAG = "wf.important";
    public static final String WF_NEUTRAL_TAG = "wf.neutral";
    public static final String WF_UNIMPORTANT_TAG = "wf.unimportant";
    public static final String WS_IMPORTANT_TAG = "ws.important";
    public static final String WS_NEUTRAL_TAG = "ws.neutral";
    public static final String WS_UNIMPORTANT_TAG = "ws.unimportant";
    public static final String WURI_INDEX_KEY = "wuri.index";
    public static final String WURI_TAG = "wuri";

    public Context(Context that) {
	super(that);
    }
    public Context(String filename) throws FileNotFoundException, IOException{
	super();
	InputStream fs;
	try {
	    fs = new FileInputStream(filename);
	} catch (FileNotFoundException fnfe) {
	    URL resource = Context.class.getClassLoader().getResource(filename);
	    fs = new FileInputStream(new File(resource.getFile()));
	}
	super.load(fs);
    }
    
    public String getAlignmentIndex() {
	return getProperty(ALIGNMENT_INDEX_KEY);
    }
    
    public double getB() {
	return getDouble(B_TAG, 0.75);
    }

    public String getBLACKLIST_FILENAME() {
	return getProperty(BLACKLIST_FILENAME_KEY, "blacklist.txt");
    }

    public String getCollection() {
	return getProperty(COLLECTION_KEY);
    }

    public double getdl_cutoff() {
	return getDouble(DL_CUTOFF_TAG, 100);
    }

    public String getFieldList() {
	return getProperty(FIELD_LIST_KEY);
    }

    public double getK1() {
	return getDouble(K1_TAG, 1.2);
    }

    public boolean getLOAD_DOCUMENT_SIZES() {
	return getBoolean(LOAD_SIZES_TAG, false);
    }

    public boolean getLOAD_INDEXES_INTO_MEMORY() {
	return getBoolean(LOAD_MEMORY_TAG, false);
    }

    public int getmax_number_of_fields_norm() {
	return getInt(MAX_NORM_TAG, 5);
    }

    public String getMph() {
	return getProperty(MPH_KEY);
    }

    public String getMultiIndexPath() {
	return getProperty(MULTIINDEX_PATH_KEY);
    }
    
    public String getMultiIndexDirPrefix() {
	return getProperty(MULTIINDEX_DIR_PREFIX_KEY);
    }

    public String getOntoPath() {
	return getProperty(ONTOLOGY_PATH_KEY);
    }

    public String getPathToDocumentPriors() {
	return getProperty(DOCUMENT_PRIORS_KEY);
    }

    public String getPathToIndex() {
	return getProperty(INDEX_PATH_KEY);
    }

    public String getPathToPriorRules() {
	return getProperty(PRIOR_RULES_KEY);
    }

    public String getPropertyIndex() {
	return getProperty(PROPERTY_INDEX_KEY);
    }

    public String getQRels() {
	return getProperty(QRELS_KEY);
    }

    public String getQueryFile() {
	return getProperty(QUERY_FILE_KEY);
    }

    public boolean getRemoveStopwords() {
	return getBoolean(REMOVE_STOPWORDS_TAG, false);
    }

    public int getresultsCutoff() {
	return getInt(MIN_RESULTS_TAG, 100);
    }

    public String getRunsFile() {
	return getProperty(RUNS_FILE_KEY);
    }

    public String getRunsName() {
	return getString(RUN_NAME_TAG, "Y!NLRABCN");
    }

    public String getSegmentationCache() {
	return getProperty(SEGMENTATION_CACHE_KEY);
    }

    public int getSizeTopK() {
	return getInt(TOP_K_KEY, 1000);
    }

    public boolean getstore_cache() {
	return getBoolean(STORE_CACHE_TAG, true);
    }

    public String getSwList() {
	return getProperty(SW_LIST_KEY);
    }

    public String getTitleList() {
	return getProperty(TITLE_LIST_KEY);
    }

    public String getTokenIndex() {
	return getProperty(TOKEN_INDEX_KEY);
    }

    public boolean getUseSegements() {
	return getBoolean(USE_SEGMENTS_KEY, true);
    }

    public boolean getUseTies() {
	return getBoolean(USE_TIES_KEY, true);
    }

    public double getw_matches() {
	return getDouble(W_MATCHES_TAG, 1);
    }

    public double getwf_important() {
	return getDouble(WF_IMPORTANT_TAG, 1);
    }

    public double getwf_neutral() {
	return getDouble(WF_NEUTRAL_TAG, 1);
    }

    public double getwf_unimportant() {
	return getDouble(WF_UNIMPORTANT_TAG, 1);
    }

    public double getws_important() {
	return getDouble(WS_IMPORTANT_TAG, 1);
    }

    public double getws_neutral() {
	return getDouble(WS_NEUTRAL_TAG, 1);
    }

    public double getws_unimportant() {
	return getDouble(WS_UNIMPORTANT_TAG, 1);
    }

    public double getwuri() {
	return getDouble(WURI_TAG, 1);
    }

    public String getWuriIndex() {
	return getProperty(WURI_INDEX_KEY);
    }

    @Deprecated
    public void reload() throws FileNotFoundException, IOException {
	throw new RuntimeException("This shouldn't be called.");
	// must be loaded first
//	if (config_name != null)
//	    load(config_name);
//    }
    }

    public Boolean getBoolean(String key) {
	String value = getProperty(key);
	if (value == null) {
	    return null;
	}
	return Boolean.parseBoolean(value);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
	String value = getProperty(key);
	if (value == null) {
	    return defaultValue;
	}
	return Boolean.parseBoolean(value);
    }
    
    public Double getDouble(String key) {
	String value = getProperty(key);
	if (value == null) {
	    return null;
	}
	return Double.parseDouble(value);
    }

    public double getDouble(String key, double defaultValue) {
	String value = getProperty(key);
	if (value == null) {
	    return defaultValue;
	}
	return Double.parseDouble(value);
    }
    
    public int getInt(String key, int defaultValue) {
	String value = getProperty(key);
	if (value == null) {
	    return defaultValue;
	}
	return Integer.parseInt(value);
    }

    public Integer getInteger(String key) {
	String value = getProperty(key);
	if (value == null) {
	    return null;
	}
	return Integer.parseInt(value);
    }
    
    public String getString(String key) {
	return getProperty(key);
    }

    public String getString(String key, String defaultValue) {
	return getProperty(key, defaultValue);
    }
    
    public void setIndexPath(String indexPath) {
	setProperty(INDEX_PATH_KEY, indexPath + File.separator + "vertical" + File.separator);
	setProperty(TOKEN_INDEX_KEY, indexPath + File.separator + "horizontal" + File.separator + "token");
	setProperty(PROPERTY_INDEX_KEY, indexPath + File.separator + "horizontal" + File.separator + "property");
	setProperty(WURI_INDEX_KEY, indexPath + File.separator + "horizontal" + File.separator + "uri");
	setProperty(TITLE_LIST_KEY, indexPath + File.separator + "subjects.txt");
	setProperty(FIELD_LIST_KEY, indexPath + File.separator + "predicates.txt");
	setProperty(MPH_KEY, indexPath + File.separator + "subjects.mph");
	setProperty(COLLECTION_KEY, indexPath + File.separator + COLLECTION_KEY + File.separator);
	setProperty(ALIGNMENT_INDEX_KEY, indexPath + File.separator + "vertical" + File.separator + "alignment");
    }
}