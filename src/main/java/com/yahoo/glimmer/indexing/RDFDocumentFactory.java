package com.yahoo.glimmer.indexing;

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

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.AbstractObject2LongFunction;
import it.unimi.dsi.mg4j.document.DocumentFactory.FieldType;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collection;

import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.semanticweb.yars.nx.parser.ParseException;

import com.yahoo.glimmer.util.Util;

/* Common superclass to HorizontalDocumentFactory and VerticalDocumentFactory.
 * 
 */
public abstract class RDFDocumentFactory {
    private static final Log LOG = LogFactory.getLog(RDFDocumentFactory.class);
    
    private static final String CONF_INDEX_TYPE_KEY = "IndexType";
    private static final String CONF_WITH_CONTEXTS_KEY = "WithContexts";
    private static final String CONF_FIELDNAMES_KEY = "RdfFieldNames";
    private static final String CONF_RESOURCES_HASH_KEY = "ResourcesFilename";

    private static final Collection<String> PREDICATE_BLACKLIST = Arrays.asList("stag", "tagspace", "ctag", "rel", "mm");

    /** Determines if we use the namespaces table for abbreviating field names */
    public static final boolean USE_NAMESPACES = false;
    public static final char NAMESPACE_SEPARATOR = '_';

    private String[] fields;
    private AbstractObject2LongFunction<CharSequence> resourcesHashFunction;

    // TODO How to read these?
    private Counters counters = new Counters();

    // Include NQuad contexts in processing.
    private boolean withContexts;

    public static enum IndexType {
	VERTICAL(VerticalDocumentFactory.class), HORIZONTAL(HorizontalDocumentFactory.class), UNDEFINED(null);

	private final Class<?> factoryClass;

	private IndexType(Class<?> factoryClass) {
	    this.factoryClass = factoryClass;
	}

	public Class<?> getFactoryClass() {
	    return factoryClass;
	}
    }

    public abstract RDFDocument getDocument();

    protected static void setupConf(Configuration conf, IndexType type, boolean withContexts, String resourcesHash, String... fields) {
	conf.setEnum(CONF_INDEX_TYPE_KEY, type);
	conf.setBoolean(CONF_WITH_CONTEXTS_KEY, withContexts);
	conf.setStrings(CONF_FIELDNAMES_KEY, fields);
	if (resourcesHash != null) {
	    conf.set(CONF_RESOURCES_HASH_KEY, resourcesHash);
	}
    }

    public static String[] getFieldsFromConf(Configuration conf) {
	String[] fields = conf.getStrings(CONF_FIELDNAMES_KEY);
	if (fields == null) {
	    throw new IllegalStateException("Fields not set set in the config.");
	}
	return fields;
    }

    public static IndexType getIndexType(Configuration conf) {
	IndexType indexType = conf.getEnum(CONF_INDEX_TYPE_KEY, IndexType.UNDEFINED);
	if (indexType == IndexType.UNDEFINED) {
	    throw new IllegalStateException("Index type not set in config.");
	}
	return indexType;
    }

    public static RDFDocumentFactory buildFactory(Configuration conf) {
	IndexType indexType = getIndexType(conf);

	RDFDocumentFactory factory;
	try {
	    Constructor<?> factoryConstructor = indexType.factoryClass.getConstructor();
	    factory = ((RDFDocumentFactory) factoryConstructor.newInstance());
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
	factory.setFields(getFieldsFromConf(conf));
	factory.setWithContexts(conf.getBoolean(CONF_WITH_CONTEXTS_KEY, false));
	String resourcesHashFilename = conf.get(CONF_RESOURCES_HASH_KEY);
	if (resourcesHashFilename != null) {
	    // Load the hash func.
	    try {
		Path resourcesHashPath = new Path(resourcesHashFilename);
		InputStream resourcesHashInputStream = CompressionCodecHelper.openInputStream(conf, resourcesHashPath);
		
		@SuppressWarnings("unchecked")
		AbstractObject2LongFunction<CharSequence> hash = (AbstractObject2LongFunction<CharSequence>) BinIO.loadObject(resourcesHashInputStream);
		factory.setResourcesHashFunction(hash);
		LOG.info("Loaded resource hash from " + resourcesHashFilename + " with " + hash.size() + " entires.");
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	} else {
	    LOG.info("No resource hash filename set in conf.  No hash has been loaded.");
	}
	return factory;
    }

    protected StatementCollectorHandler parseStatements(String url, String data) throws TransformerConfigurationException, RDFParseException,
	    RDFHandlerException, MalformedURLException, IOException, TransformerException, ParseException {
	StatementCollectorHandler handler = new StatementCollectorHandler();
	// NTuples format where tuples are separated by double spaces
	String[] lines = data.split("  ");
	for (String line : lines) {
	    handler.handleStatement(Util.parseStatement(line));
	}
	return handler;
    }

    public void setResourcesHashFunction(AbstractObject2LongFunction<CharSequence> resourcesHashFunction) {
	this.resourcesHashFunction = resourcesHashFunction;
    }
    
    /**
     * @param url
     * @return The hash value for the given url, or null if the url is not in
     *         the hash function. nulls will only be returned when the hash
     *         function being used is signed. For unsigned hash functions some
     *         value smaller than the size of the hash will be returned.
     */
    public Integer lookupResource(String url) {
	Long value = resourcesHashFunction.get(url);
	if (value == null || value < 0) {
	    return null;
	}
	if (value > Integer.MAX_VALUE) {
	    throw new RuntimeException("Hash value bigger that max int.");
	}
	return value.intValue();
    }

    public boolean isWithContexts() {
	return withContexts;
    }

    public void setWithContexts(Boolean withContexts) {
	this.withContexts = withContexts;
    }

    public static boolean isOnPredicateBlacklist(final String predicate) {
	return PREDICATE_BLACKLIST.contains(predicate);
    }

    public void setFields(String[] fields) {
	this.fields = fields;
    }

    public int numberOfFields() {
	ensureFieldIndex(0);
	return fields.length;
    }

    public String fieldName(final int index) {
	ensureFieldIndex(index);
	return fields[index];
    }

    public int fieldIndex(final String fieldName) {
	ensureFieldIndex(0);
	for (int i = 0; i < fields.length; i++) {
	    if (fields[i].equals(fieldName)) {
		return i;
	    }
	}
	return -1;
    }

    public FieldType fieldType(final int index) {
	ensureFieldIndex(index);
	return FieldType.TEXT;
    }

    public void ensureFieldIndex(final int index) {
	if (fields == null) {
	    throw new IllegalStateException("Fields not loaded.");
	}
	if (index < 0 || index >= fields.length) {
	    throw new IndexOutOfBoundsException("For field index " + index + ". There are only " + fields.length + " fields.");
	}
    }

    public static enum RdfCounters {
	EMPTY_LINES, EMPTY_DOCUMENTS, BLACKLISTED_TRIPLES, UNINDEXED_PREDICATE_TRIPLES, RDF_TYPE_TRIPLES, INDEXED_TRIPLES
    }

    public void incrementCounter(RdfCounters counter, int by) {
	counters.findCounter(counter).increment(by);
    }

    public Counters getCounters() {
	return counters;
    }

    public String getInputStreamEncodeing() {
	return "UTF-8";
    }
}
