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
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.mg4j.util.MG4JClassParser;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.nio.charset.Charset;

import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.semanticweb.yars.nx.parser.ParseException;

import com.yahoo.glimmer.util.Util;

/* Common superclass to HorizontalDocumentFactory and VerticalDocumentFactory.
 * 
 */
public abstract class RDFDocumentFactory extends PropertyBasedDocumentFactory {
    private static final long serialVersionUID = 3508901442129214511L;
    /** Determines if we use the namespaces table for abbreviating field names */
    public static final boolean USE_NAMESPACES = false;
    public static final String[] PREDICATE_BLACKLIST = { "stag", "tagspace", "ctag", "rel", "mm" };
    public static final char NAMESPACE_SEPARATOR = '_';
    public static final String RESOURCES_FILENAME_KEY = "ResourcesFilename";

    private TaskAttemptContext taskAttemptContext;
    /** The word reader used for all documents. */
    protected transient WordReader wordReader;
    private AbstractObject2LongFunction<CharSequence> resourcesHash;

    // Include NQuad contexts in processing.
    private Boolean withContexts;

    public static enum MetadataKeys {
	RESOURCES_HASH, WITH_CONTEXTS, HADOOP_FILE_SYSTEM, PREDICATES_CACHE_FILENAME // ,
										     // INDEXED_PROPERTIES,
										     // INDEXED_PROPERTIES_FILENAME
										     // TASK_IO_CONTEXT,
    };

    public RDFDocumentFactory(final Properties properties) throws ConfigurationException {
	super(properties);
    }

    public RDFDocumentFactory(final Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
	super(defaultMetadata);
    }

    public RDFDocumentFactory(final String[] property) throws ConfigurationException {
	super(property);
    }

    public RDFDocumentFactory() {
	super();
    }

    protected boolean parseProperty(final String key, final String[] values, final Reference2ObjectMap<Enum<?>, Object> metadata) throws ConfigurationException {
	if (sameKey(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, key)) {
	    metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, Charset.forName(ensureJustOne(key, values)).toString());
	    return true;
	} else if (sameKey(PropertyBasedDocumentFactory.MetadataKeys.WORDREADER, key)) {
	    try {
		final String spec = (ensureJustOne(key, values)).toString();
		metadata.put(PropertyBasedDocumentFactory.MetadataKeys.WORDREADER, spec);
		// Just to check
		ObjectParser.fromSpec(spec, WordReader.class, MG4JClassParser.PACKAGE);
	    } catch (ClassNotFoundException e) {
		throw new ConfigurationException(e);
	    }
	    // TODO: this must turn into a more appropriate exception
	    catch (Exception e) {
		throw new ConfigurationException(e);
	    }
	    return true;
	}
	return super.parseProperty(key, values, metadata);
    }

    protected boolean onPredicateBlackList(String name) {
	// Filter out namespaces on blacklist
	for (String black : PREDICATE_BLACKLIST) {
	    if (name.startsWith(black)) {
		return true;
	    }
	}
	return false;
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

    protected void init() {
	try {
	    Object o = defaultMetadata.get(PropertyBasedDocumentFactory.MetadataKeys.WORDREADER);
	    wordReader = o == null ? new FastBufferedReader() : ObjectParser.fromSpec(o.toString(), WordReader.class, MG4JClassParser.PACKAGE);
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}

	Configuration conf = getTaskAttemptContext().getConfiguration();
	String resourcesCacheFilename = conf.get(RESOURCES_FILENAME_KEY);
	if (resourcesCacheFilename != null) {
	    try {
		Path resourcesCachePath = new Path(resourcesCacheFilename);
		FileSystem fs = FileSystem.get(conf);
		@SuppressWarnings("unchecked")
		AbstractObject2LongFunction<CharSequence> resourcesHash = (AbstractObject2LongFunction<CharSequence>) BinIO.loadObject(fs
			.open(resourcesCachePath));
		System.out.println("Loaded resource hash from " + resourcesCacheFilename + " with " + resourcesHash.size() + " entires.");
		defaultMetadata.put(MetadataKeys.RESOURCES_HASH, resourcesHash);
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	}
    }

    public TaskAttemptContext getTaskAttemptContext() {
	return taskAttemptContext;
    }

    public void setTaskAttemptContext(TaskAttemptContext taskAttemptContext) {
	this.taskAttemptContext = taskAttemptContext;
    }

    public void setResourcesHash(AbstractObject2LongFunction<CharSequence> resourcesHash) {
	this.resourcesHash = resourcesHash;
    }

    @SuppressWarnings("unchecked")
    public Long resourcesHashLookup(String key) {
	if (resourcesHash == null) {
	    resourcesHash = (AbstractObject2LongFunction<CharSequence>) super.resolve(MetadataKeys.RESOURCES_HASH, defaultMetadata);
	    if (resourcesHash == null) {
		throw new IllegalStateException("resourcesHashLookup() called when no resources hash filename was given to buildFactory()");
	    }
	}
	return resourcesHash.get(key);
    }

    public boolean isWithContexts() {
	if (withContexts == null) {
	    withContexts = (Boolean) resolve(MetadataKeys.WITH_CONTEXTS, defaultMetadata, Boolean.FALSE);
	}
	return withContexts;
    }

    private void readObject(final ObjectInputStream s) throws IOException, ClassNotFoundException {
	s.defaultReadObject();
	init();
    }

    public static RDFDocumentFactory buildFactory(Class<?> documentFactoryClass, TaskAttemptContext taskContext) {
	Reference2ObjectMap<Enum<?>, Object> defaultMetadata = new Reference2ObjectArrayMap<Enum<?>, Object>();
	defaultMetadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "UTF-8");

	try {
	    Constructor<?> constr = documentFactoryClass.getConstructor(Reference2ObjectMap.class);
	    RDFDocumentFactory factory = ((RDFDocumentFactory) constr.newInstance(defaultMetadata));
	    factory.setTaskAttemptContext(taskContext);
	    factory.init();
	    return factory;
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }

    @Override
    protected void ensureFieldIndex(final int index) {
	super.ensureFieldIndex(index);
    }

    @Override
    protected Object resolveNotNull(final Enum<?> key, final Reference2ObjectMap<Enum<?>, Object> docMetadata) {
	return super.resolveNotNull(key, docMetadata);
    }

    @Override
    protected Object resolve(final Enum<?> key, final Reference2ObjectMap<Enum<?>, Object> docMetadata) {
	return super.resolve(key, docMetadata);
    }

    public static enum Counters {
	EMPTY_LINES, EMPTY_DOCUMENTS, BLACKLISTED_TRIPLES, UNINDEXED_PREDICATE_TRIPLES, RDF_TYPE_TRIPLES, INDEXED_TRIPLES
    }

    public void incrementCounter(RDFDocumentFactory.Counters counter, int by) {
	// This will never actually count anything as the factory used for
	// loading the docs is the one created in RDFInputFormat and not the one
	// created in the Mapper.
	// In RDFInputFormat we only have access to a TaskAttemptContext.
	if (taskAttemptContext instanceof TaskInputOutputContext<?, ?, ?, ?>) {
	    ((TaskInputOutputContext<?, ?, ?, ?>) taskAttemptContext).getCounter(counter).increment(by);
	}
    }
}
