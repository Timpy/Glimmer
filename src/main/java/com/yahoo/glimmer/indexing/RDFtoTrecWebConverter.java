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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringEscapeUtils;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * This class takes a directory of files containing compressed rdf files. It
 * converts them into TrecWeb format.
 * 
 * Note: This assumes that the triples are sorted by subject. The RDF attributes
 * are mapped onto SGML tags and the RDF objects are the contents of the tag.
 * 
 * This is was written for the UMass participation in the Semsearch Evaluation
 * workshop. It is open source code, available on Github,
 * https://github.com/daltonj/CIIRShared
 * 
 * @author jdalton
 * 
 */
public class RDFtoTrecWebConverter {

    public static final char NAMESPACE_SEPARATOR = '_';

    private PrintWriter m_outputWriter;
    private PrintWriter m_docNoToUrlWriter;
    private File m_outputDir;
    int m_curDoc = 0;

    private File m_curFile;

    public RDFtoTrecWebConverter(File outputDir) throws Exception {
	m_outputDir = outputDir;
    }

    private void convert(File file) throws Exception {
	System.out.println("Now processing file: " + file.getName());

	if (!file.getName().endsWith(".gz")) {
	    System.out.println("Skipping file; needs to be a compressed collection file.");
	    return;
	}

	m_curFile = file;
	String outputXmlPath = m_outputDir + "/" + file.getName().replace(".gz", "");
	m_outputWriter = new PrintWriter(outputXmlPath);

	File outputDocNo = new File(m_outputDir + "/docNoToUrl/");
	if (!outputDocNo.exists()) {
	    outputDocNo.mkdir();
	}
	m_docNoToUrlWriter = new PrintWriter(new File(m_outputDir + "/docNoToUrl/" + file.getName().replace(".gz", "")));

	BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
	try {
	    String line = null;
	    while ((line = reader.readLine()) != null) {

		// First part is URL, second part is docfeed
		String subj = line.substring(0, line.indexOf('\t')).trim();
		if (subj.toLowerCase().startsWith("_:node") || subj.toLowerCase().startsWith("<http://") || subj.toLowerCase().startsWith("http://")) {

		    String data = line.substring(line.indexOf('\t')).trim();
		    StatementCollectorHandler handler = parseStatements(subj, data);
		    attributesToXml(subj, handler);
		} else {
		    System.out.println("Skipping subject: " + subj);
		}
	    }

	} finally {
	    reader.close();
	}

    }

    /**
     * Write a subject as XML with all of its predicates and objects.
     * 
     * @param subject
     * @param attributeMap
     * @throws Exception
     */
    public void attributesToXml(String subject, StatementCollectorHandler handler) throws Exception {

	if (m_curDoc % 5000 == 0) {
	    System.out.println("Now processing doc number: " + m_curDoc + " ;url:" + subject);
	}

	startDoc();
	String url = clean(subject);
	writeDocNo(url);
	writeUrl(url);

	for (Statement stmt : handler.getStatements()) {

	    String predicate = stmt.getPredicate().toString();

	    Value objVal = stmt.getObject();
	    String object;
	    if (objVal instanceof URIImpl) {
		object = ((URIImpl) objVal).toString();
	    } else {
		object = ((Literal) objVal).getLabel();
	    }
	    String fieldName = predicateToName(predicate);

	    addField(fieldName, object);
	}

	endDoc();
	m_curDoc++;
    }

    private String predicateToName(String predicate) {
	String result = null;
	// Generate a short name from the URI
	if (predicate != null && predicate.startsWith("http://")) {
	    URI pred = new URIImpl(predicate);
	    result = pred.getNamespace().substring("http://".length()).replaceAll("[\\./#\\-\\~]+", "_");
	    if (result != null && result.length() > 0) {
		if (result.charAt(result.length() - 1) != NAMESPACE_SEPARATOR) {
		    result += NAMESPACE_SEPARATOR;
		}
		result += pred.getLocalName().replaceAll("[\\./#\\-\\~]+", "_");
	    } else {
		// something strange is going on
		result = null;
	    }
	}
	return result;
    }

    private void writeUrl(String url) {
	m_outputWriter.println("<url>");
	m_outputWriter.print(StringEscapeUtils.escapeXml(url));
	m_outputWriter.print("</url>");
    }

    private void writeDocNo(String url) {

	String docNo = m_curFile.getName().replace("-urified.gz", "");
	docNo = docNo + "_" + m_curDoc;
	m_docNoToUrlWriter.println(docNo + "\t" + url);
	m_outputWriter.print("<DOCNO>");
	m_outputWriter.print(docNo);
	m_outputWriter.print("</DOCNO>\n<DOCHDR>\n" + url + "\n</DOCHDR>\n");

    }

    private String clean(String str) {
	String newString = str.replace("<", "");
	newString = newString.replace(">", "");
	return newString;
    }

    private void startDoc() {
	m_outputWriter.println("\n<DOC>");
    }

    private void endDoc() {
	m_outputWriter.println("\n</DOC>");
    }

    private void addField(String field, String value) {
	m_outputWriter.println("<" + field + ">");
	m_outputWriter.print(StringEscapeUtils.escapeXml(value));
	m_outputWriter.println("\n</" + field + ">");
    }

    protected StatementCollectorHandler parseStatements(String url, String data) throws Exception {
	StatementCollectorHandler handler = new StatementCollectorHandler();
	// NTuples format where tuples are separated by double spaces
	String[] lines = data.split("  ");
	for (String line : lines) {
	    Node[] nodes = NxParser.parseNodes(line);
	    Resource subject = null;
	    if (nodes[0] instanceof org.semanticweb.yars.nx.Resource) {
		subject = new URIImpl(nodes[0].toString());
	    } else if (nodes[0] instanceof org.semanticweb.yars.nx.BNode) {
		String nodeID = nodes[0].toString().substring(org.semanticweb.yars.nx.BNode.PREFIX.length());
		subject = new BNodeImpl(nodeID);
	    }
	    URI predicate = new URIImpl(nodes[1].toString());
	    Value object = null;
	    if (nodes[2] instanceof org.semanticweb.yars.nx.Resource) {
		object = new URIImpl(nodes[2].toString());
	    } else if (nodes[2] instanceof org.semanticweb.yars.nx.BNode) {
		String nodeID = nodes[2].toString().substring(org.semanticweb.yars.nx.BNode.PREFIX.length());
		object = new BNodeImpl(nodeID);
	    } else {
		if (((org.semanticweb.yars.nx.Literal) nodes[2]).getDatatype() != null) {
		    URI datatype = new URIImpl(((org.semanticweb.yars.nx.Literal) nodes[2]).getDatatype().toString());
		    object = new LiteralImpl(((org.semanticweb.yars.nx.Literal) nodes[2]).getUnescapedData(), datatype);

		} else if (((org.semanticweb.yars.nx.Literal) nodes[2]).getLanguageTag() != null) {
		    String language = ((org.semanticweb.yars.nx.Literal) nodes[2]).getLanguageTag();
		    object = new LiteralImpl(((org.semanticweb.yars.nx.Literal) nodes[2]).getUnescapedData(), language);
		} else {
		    object = new LiteralImpl(((org.semanticweb.yars.nx.Literal) nodes[2]).getUnescapedData());
		}
	    }
	    // System.out.println("TRIPLE: " + new StatementImpl(subject,
	    // predicate, object));
	    handler.handleStatement(new StatementImpl(subject, predicate, object));
	}

	return handler;
    }

    public static void main(String[] args) throws Exception {

	System.out.println("arguments: " + args[0] + " " + args[1]);
	File inputFile = new File(args[0]);
	File outputDir = new File(args[1]);

	RDFtoTrecWebConverter converter = new RDFtoTrecWebConverter(outputDir);

	if (inputFile.isDirectory()) {
	    cleanDirectory(outputDir);

	    FilenameFilter filter = new FilenameFilter() {
		public boolean accept(File dir, String name) {
		    return name.startsWith("part-");
		}
	    }; // End of anonymous inner class

	    File[] xmlFiles = inputFile.listFiles(filter);
	    for (File xmlFile : xmlFiles) {
		converter.convert(xmlFile);
	    }

	} else if (inputFile.getName().startsWith("part-")) {
	    converter.convert(inputFile);
	}
    }

    private static void cleanDirectory(File dir) throws Exception {
	if (dir.exists()) {
	    File[] xmlFiles = dir.listFiles();
	    for (File file : xmlFiles) {
		file.delete();
	    }
	} else {
	    dir.mkdir();
	    if (!dir.exists()) {
		throw new Exception("Unable to create output directory.");
	    }
	}
    }
}
