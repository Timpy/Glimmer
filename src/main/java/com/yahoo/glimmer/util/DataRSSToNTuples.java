package com.yahoo.glimmer.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.Map;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.rio.rdfxml.RDFXMLParserFactory;

public class DataRSSToNTuples {
	
	  private final static int INPUT_DATA_MAXSIZE = 1024 * 100; //50 kbyte
	
	  private final static int RDF_DATA_MAXSIZE = 1024 * 100; //50 kbyte

	  private RDFXMLParser parser = (RDFXMLParser) new RDFXMLParserFactory().getParser();

	  
	  private Transformer fixTransformer, rdfaTransformer;
	  private Map<String, String> nsMap;

	  public DataRSSToNTuples() throws TransformerConfigurationException {
		try {
			//This is used when loading the XSLTs from the compiled jar
			InputStream fixStylesheet = getClass().getClassLoader().getResourceAsStream("fixDataRSS.xsl");
			InputStream rdfaStylesheet = getClass().getClassLoader().getResourceAsStream("RDFa2RDFXML.xsl");
			InputStream namespaces = getClass().getClassLoader().getResourceAsStream("t_namespaces.html");
			init(fixStylesheet, rdfaStylesheet, namespaces);
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	  }
	 
	  public DataRSSToNTuples(InputStream fixStylesheet, InputStream rdfaStylesheet, InputStream namespaces) throws TransformerConfigurationException {
		
	 	init(fixStylesheet, rdfaStylesheet, namespaces);

	    //For some really strange reason this appears empty when accessed in convert()
	    //prefix2nsMap = loadNamespaces();
	  }

	  public void init(InputStream fixStylesheet, InputStream rdfaStylesheet, InputStream namespaces) throws TransformerConfigurationException {
	    fixTransformer = Util.initTransformer(fixStylesheet);
	    rdfaTransformer = Util.initTransformer(rdfaStylesheet);
	    nsMap = Util.loadPrefix2NamespaceMap(namespaces);
	  }
	  
	  public void convert(URL url, String data, boolean removeDatatypes, RDFHandler handler) throws IOException, TransformerConfigurationException, TransformerException, RDFParseException, RDFHandlerException {
		  //System.out.println("Processing: " + url);
		  try {
		    if (data.length() > INPUT_DATA_MAXSIZE) {
				System.err.println("Input data for transformation exceeded size of " + INPUT_DATA_MAXSIZE + " for " + url);
				return;
			}
		    
			String fixedData = Util.applyStylesheet(data, fixTransformer);

		    //Add namespace declarations
			int feedStartPos = fixedData.indexOf("<feed");
			String nsData = fixedData.substring(0, feedStartPos + "<feed".length());
			
			for (String key:nsMap.keySet()) {
				nsData += " xmlns:" + key +"=\"" + nsMap.get(key) + "\" ";
			}	
			nsData += fixedData.substring(feedStartPos + "<feed".length(), fixedData.length());
			
			//Optionally, remove datatype attributes
			if (removeDatatypes)
				nsData = nsData.replaceAll("datatype=\"xsd:.*?\"", "");
			
		    //Apply RDFa stylesheet
			rdfaTransformer.setParameter("CURRURL", url.toExternalForm());
			String rdf =  Util.applyStylesheet(nsData, rdfaTransformer);
		    
			if (rdf.length() > RDF_DATA_MAXSIZE) {
				System.err.println("RDF/XML result exceeded size of " + RDF_DATA_MAXSIZE + " for " + url);
				return;
			}
			
		    parser.setRDFHandler(handler);
		    parser.parse(new StringReader(rdf), url.toExternalForm());   
		  } catch (StackOverflowError e) {
			  System.err.println("StackOverflowException for url:" + url + " \nData was: " + data);
		  }
	  }
	  
	  public String convert(URL url, String data, String adjunctid, boolean removeDatatypes, boolean nTriples) throws IOException, TransformerConfigurationException, TransformerException, RDFParseException, RDFHandlerException {
			//Read RDF/XML and output NTuples
		    
		    NTuplesWriter writer;
		    if (nTriples) {
		    	writer = new NTuplesWriter(url, nTriples);
		    } else {
		    	writer = new NTuplesWriter(url, adjunctid);
		    }
		    convert(url, data, removeDatatypes, writer);
		    return writer.getResult();

	  }

}
