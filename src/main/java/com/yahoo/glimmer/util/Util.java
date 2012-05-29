package com.yahoo.glimmer.util;

/**
 * Utility methods for microsearch
 * 
 * @author Peter Mika (pmika@yahoo-inc.com)
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.w3c.dom.Document;

public class Util {

    public final static SimpleDateFormat XSD_DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZ");

    private static final int MAX_TOTAL_CONNECTIONS = 500;

    private static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 100;

    private static final int SO_TIMEOUT = 60000;

    private static final int CONNECTION_TIMEOUT = 60000;

    private final static int BUFFER_SIZE = 2048;

    protected static Logger _logger = Logger.getLogger(Util.class);

    public final static String NAMESPACES_TABLE = "t_namespaces.html";
    public final static Pattern NAMESPACES_PATTERN = Pattern.compile("<tr><td>(.*?)</td><td>(.*?)</td><td>.*?</td><td>(.*?)</td></tr>");

    public static final HashMap<String, String> SM_NAMESPACES = new HashMap<String, String>();

    static {
	SM_NAMESPACES.put("atom", "http://www.w3.org/2005/Atom");
	SM_NAMESPACES.put("cc", "http://creativecommons.org/licenses/");
	SM_NAMESPACES.put("dc", "http://purl.org/dc/elements/1.1/");
	SM_NAMESPACES.put("foaf", "http://xmlns.com/foaf/0.1/");
	SM_NAMESPACES.put("geo", "http://www.georss.org/georss");
	SM_NAMESPACES.put("media", "http://search.yahoo.com/mrss/");
	SM_NAMESPACES.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
	SM_NAMESPACES.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
	SM_NAMESPACES.put("review", "http://www.purl.org/stuff/rev#");
	SM_NAMESPACES.put("vcal", "http://www.w3.org/2002/12/cal#");
	SM_NAMESPACES.put("vcard", "http://www.w3.org/2006/vcard/ns#");
	SM_NAMESPACES.put("rel", "http://search.yahoo.com/searchmonkey-relation/");
	SM_NAMESPACES.put("assert", "http://search.yahoo.com/searchmonkey/assert/");
	SM_NAMESPACES.put("commerce", "http://search.yahoo.com/searchmonkey/commerce/");
	SM_NAMESPACES.put("context", "http://search.yahoo.com/searchmonkey/context/");
	SM_NAMESPACES.put("finance", "http://search.yahoo.com/searchmonkey/finance/");
	SM_NAMESPACES.put("job", "http://search.yahoo.com/searchmonkey/job/");
	SM_NAMESPACES.put("news", "http://search.yahoo.com/searchmonkey/news/");
	SM_NAMESPACES.put("page", "http://search.yahoo.com/searchmonkey/page/");
	SM_NAMESPACES.put("product", "http://search.yahoo.com/searchmonkey/product/");
	SM_NAMESPACES.put("reference", "http://search.yahoo.com/searchmonkey/reference/");
	SM_NAMESPACES.put("resume", "http://search.yahoo.com/searchmonkey/resume/");
	SM_NAMESPACES.put("social", "http://search.yahoo.com/searchmonkey/social/");
	SM_NAMESPACES.put("tagspace", "http://search.yahoo.com/searchmonkey/tagspace/");
	SM_NAMESPACES.put("country", "http://search.yahoo.com/searchmonkey-datatype/country/");
	SM_NAMESPACES.put("currency", "http://search.yahoo.com/searchmonkey-datatype/currency/");
	SM_NAMESPACES.put("use", "http://search.yahoo.com/searchmonkey-datatype/use/");
	SM_NAMESPACES.put("xfn", "http://gmpg.org/xfn/11#");
	SM_NAMESPACES.put("xsd", "http://www.w3.org/2001/XMLSchema#");

    }

    private static HttpClient _httpClient;

    static {

	_logger.info("Initializing MultiThreadedHttpConnectionManager with " + "CONNECTION_TIMEOUT=" + CONNECTION_TIMEOUT + "ms and " + "SO_TIMEOUT="
		+ SO_TIMEOUT + "ms " + "DEFAULT_MAX_CONNECTIONS_PER_HOST=" + DEFAULT_MAX_CONNECTIONS_PER_HOST + " " + "MAX_TOTALCONNECTIONS="
		+ MAX_TOTAL_CONNECTIONS);

	MultiThreadedHttpConnectionManager mthc = new MultiThreadedHttpConnectionManager();
	mthc.getParams().setDefaultMaxConnectionsPerHost(DEFAULT_MAX_CONNECTIONS_PER_HOST);
	mthc.getParams().setMaxTotalConnections(MAX_TOTAL_CONNECTIONS);
	_httpClient = new HttpClient(mthc);

	mthc.getParams().setConnectionTimeout(CONNECTION_TIMEOUT);
	mthc.getParams().setSoTimeout(SO_TIMEOUT);
    }

    /**
     * Retrieve a document from the given URL using HTTP GET
     * 
     * @param url
     * @return Document as stream
     * @throws HttpException
     * @throws IOException
     * @see org.apache.commons.httpclient.methods.GetMethod
     */
    public static InputStream getDocumentAsInputStream(String url) throws HttpException, IOException {
	// Execute a GET method on the URL
	GetMethod get = new GetMethod(url);
	get.setFollowRedirects(true);
	get.addRequestHeader("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 4.0)");
	_httpClient.executeMethod(get);
	return get.getResponseBodyAsStream();
    }

    /**
     * Retrieve a document from the given URL using HTTP GET. The method
     * getDocumentAsInputStream is preferred over this one, because reading a
     * document requires to buffer a document of unknown size.
     * 
     * @param url
     * @return Document as String
     * @throws HttpException
     * @throws IOException
     * @see #getDocumentAsInputStream
     * 
     */
    public static String getDocumentAsString(String url) throws HttpException, IOException {
	String result = "";
	// Execute a GET method on the URL
	GetMethod get = new GetMethod(url);
	get.setFollowRedirects(true);
	get.addRequestHeader("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 4.0)");
	try {
	    _httpClient.executeMethod(get);
	    result = get.getResponseBodyAsString();
	    if (get.getStatusCode() != 200)
		throw new HttpException("Response code is " + get.getStatusCode());
	} finally {
	    get.releaseConnection();
	}
	return result;
    }

    /**
     * Retrieve a document from the given URL through a proxy. The method
     * getDocumentAsInputStream is preferred over this one, because reading a
     * document requires to buffer a document of unknown size.
     * 
     * @param url
     * @return Document as String
     * @throws HttpException
     * @throws IOException
     * @see #getDocumentAsInputStream
     * 
     */
    public static String getDocumentAsString(String url, String proxyURL, int proxyPort, String hlfsReturn) throws HttpException, IOException {
	String result = "";
	// Execute a GET method on the URL
	GetMethod get = new GetMethod(url);
	HostConfiguration config = new HostConfiguration();
	config.setHost(url);
	config.setProxy(proxyURL, proxyPort);
	get.setFollowRedirects(true);
	get.addRequestHeader("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 4.0)");
	get.addRequestHeader("HLFS_Return", hlfsReturn);
	try {
	    _httpClient.executeMethod(config, get);
	    result = get.getResponseBodyAsString();
	} finally {
	    get.releaseConnection();
	}
	return result;
    }

    public static String getDocumentAsString(URL url) throws HttpException, IOException {
	return getDocumentAsString(url.toExternalForm());
    }

    public static String getDocumentAsString(URL url, String proxyURL, int proxyPort) throws HttpException, IOException {
	return getDocumentAsString(url.toExternalForm(), proxyURL, proxyPort, "OnlyProcessed");
    }

    public static Transformer initTransformer(InputStream is) throws TransformerConfigurationException {

	// System.setProperty("javax.xml.transform.TransformerFactory",
	// "net.sf.saxon.TransformerFactoryImpl");

	// 1. Instantiate the TransformerFactory.
	TransformerFactory tFactory = javax.xml.transform.TransformerFactory.newInstance();
	// 2a. Get the stylesheet from the XML source.
	Source sheetSource = new StreamSource(is);

	// 2b. Process the stylesheet and generate a Transformer.
	Transformer transformer = tFactory.newTransformer(sheetSource);

	return transformer;
    }

    /**
     * Apply an XSLT transformation
     * 
     * @param doc
     * @param transformer
     * @return
     * @throws TransformerConfigurationException
     * @throws TransformerException
     */
    public static String applyStylesheet(Document doc, Transformer transformer) throws TransformerConfigurationException, TransformerException {

	Source source = new DOMSource(doc);

	// 3. Use the Transformer to perform the transformation and send the
	// the output to a Result object.
	Writer jsTarget = new StringWriter();
	transformer.transform(source, new StreamResult(jsTarget));
	return jsTarget.toString();
    }

    public static String applyStylesheet(String text, Transformer transformer) throws TransformerConfigurationException, TransformerException {

	Source source = new StreamSource(new StringReader(text));

	// 3. Use the Transformer to perform the transformation and send the
	// the output to a Result object.
	Writer jsTarget = new StringWriter();
	transformer.transform(source, new StreamResult(jsTarget));
	return jsTarget.toString();
    }

    public static String applyStylesheet(String text, String stylesheet) throws TransformerConfigurationException, TransformerException {

	// Use Saxon8
	// System.setProperty("javax.xml.transform.TransformerFactory",
	// "net.sf.saxon.TransformerFactoryImpl");

	Source source = new StreamSource(new StringReader(text));

	// 1. Instantiate the TransformerFactory.
	TransformerFactory tFactory = javax.xml.transform.TransformerFactory.newInstance();
	// 2a. Get the stylesheet from the XML source.
	Source sheetSource = null;
	if (stylesheet != null && !stylesheet.equals("")) {
	    sheetSource = new StreamSource(stylesheet);
	} else {
	    String media = null, title = null, charset = null;
	    sheetSource = tFactory.getAssociatedStylesheet(source, media, title, charset);
	}

	// 2b. Process the stylesheet and generate a Transformer.
	Transformer transformer = tFactory.newTransformer(sheetSource);

	return applyStylesheet(text, transformer);

    }

    /**
     * Generate a SHA1 digest of the string passed in and encode using hex
     * encoding.
     * 
     * @param str
     *            String to be encoded
     * @return Digest
     */
    public final static String createSHA1(String str) throws UnsupportedEncodingException, NoSuchAlgorithmException {
	String result = "";
	byte[] theTextToDigestAsBytes = str.getBytes("8859_1" /* encoding */);
	MessageDigest md = MessageDigest.getInstance("SHA");
	md.update(theTextToDigestAsBytes);
	byte[] digest = md.digest();
	// Write out a formatted string
	for (int i = 0; i < digest.length; i++) {
	    String hex = Integer.toHexString(digest[i]);
	    if (hex.length() == 1)
		hex = "0" + hex;
	    result += hex.substring(hex.length() - 2);
	}
	return result.toUpperCase();
    }

    /**
     * Efficient file reading from disk
     * 
     * @param file
     * @return
     * @throws IOException
     */
    public static String readFile(File file) throws IOException {
	InputStream inStream = new FileInputStream(file);

	byte[] docBytes = new byte[(int) file.length() + BUFFER_SIZE];

	int pos = 0;
	try {

	    boolean isMore = true;
	    while (isMore) {
		int count = inStream.read(docBytes, pos, BUFFER_SIZE);
		if (count <= 0) {
		    isMore = false;
		} else {
		    pos += count;
		}
	    }
	} catch (IndexOutOfBoundsException re) {
	    re.printStackTrace();
	}

	inStream.close();
	return new String(docBytes, 0, pos, "utf-8");
    }

    /**
     * Encode a Java String for use in XML text nodes
     * 
     * @param str
     * @return
     */
    public static String encodeXMLLiteral(String str) {
	String result = new String(str);
	result = result.replace("&", "&amp;");
	result = result.replace("<", "&lt;");
	result = result.replace(">", "&gt;");
	result = result.replace("\"", "&quot;");
	result = result.replace("'", "&apos;");
	return result;
    }

    /**
     * Decode an XML text node
     * 
     * @param str
     * @return
     */
    public static String decodeXMLLiteral(String str) {
	String result = new String(str);
	result = result.replaceAll("&amp;", "&");
	result = result.replaceAll("&lt;", "<");
	result = result.replaceAll("&gt;", ">");
	result = result.replaceAll("&quot;", "\"");
	result = result.replaceAll("&apos;", "'");
	return result;
    }

    public static String DateToRFC3339(Date date) {
	String result = XSD_DATETIME_FORMAT.format(date);
	// Java gives time offset as -0800 we need -08:00
	result = result.substring(0, result.length() - 2) + ":" + result.substring(result.length() - 2, result.length());
	return result;
    }

    public static void logRequest(Logger logger, HttpServletRequest request) {
	logger.info("IP=" + request.getRemoteAddr());
	logger.info("Host=" + request.getRemoteHost());
	logger.info("Headers:");
	Enumeration<?> headers = request.getHeaderNames();
	while (headers.hasMoreElements()) {
	    String nextName = (String) headers.nextElement();
	    logger.info(nextName + '=' + request.getHeader(nextName));
	}
	logger.info("Params:");
	Enumeration<?> params = request.getParameterNames();
	while (params.hasMoreElements()) {
	    String nextKey = (String) params.nextElement();
	    logger.info(nextKey + "=" + request.getParameterValues(nextKey)[0]);
	}
    }

    private static String convertToHex(byte[] data) {
	StringBuffer buf = new StringBuffer();
	for (int i = 0; i < data.length; i++) {
	    int halfbyte = (data[i] >>> 4) & 0x0F;
	    int two_halfs = 0;
	    do {
		if ((0 <= halfbyte) && (halfbyte <= 9))
		    buf.append((char) ('0' + halfbyte));
		else
		    buf.append((char) ('a' + (halfbyte - 10)));
		halfbyte = data[i] & 0x0F;
	    } while (two_halfs++ < 1);
	}
	return buf.toString();
    }

    public static String MD5(String text) throws NoSuchAlgorithmException, UnsupportedEncodingException {
	MessageDigest md;
	md = MessageDigest.getInstance("MD5");
	byte[] md5hash = new byte[32];
	md.update(text.getBytes("iso-8859-1"), 0, text.length());
	md5hash = md.digest();
	return convertToHex(md5hash);
    }

    public static Map<String, String> loadPrefix2NamespaceMap(InputStream in) {
	Map<String, String> result = new HashMap<String, String>();
	// Load namespaces table
	BufferedReader reader;
	try {
	    // System.out.println("Loading namespaces table");
	    reader = new BufferedReader(new InputStreamReader(in));
	    String nextLine = "";
	    while ((nextLine = reader.readLine()) != null) {
		if (nextLine.indexOf("<!--") >= 0) {

		    // Skip until end of comment
		    boolean found = nextLine.indexOf("-->") >= 0;
		    while ((nextLine = reader.readLine()) != null && !found) {
			if (nextLine.indexOf("-->") >= 0)
			    found = true;
		    }
		}

		if (nextLine != null) {
		    Matcher m = NAMESPACES_PATTERN.matcher(nextLine);
		    if (m.find()) {
			result.put(m.group(1), m.group(3));
			// System.out.println("Adding prefix: '" + m.group(1) +
			// "' for '" + m.group(3) + "'");
		    }
		}
	    }
	    reader.close();
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return result;
    }

    public static Map<String, String> loadNamespaceToPrefixMap(InputStream in) {
	Map<String, String> result = new HashMap<String, String>();
	// Load namespaces table
	BufferedReader reader;
	try {
	    System.out.println("Loading namespaces table");
	    reader = new BufferedReader(new InputStreamReader(in));
	    String nextLine = "";
	    while ((nextLine = reader.readLine()) != null) {
		if (nextLine.indexOf("<!--") >= 0) {

		    // Skip until end of comment
		    boolean found = nextLine.indexOf("-->") >= 0;
		    while ((nextLine = reader.readLine()) != null && !found) {
			if (nextLine.indexOf("-->") >= 0)
			    found = true;
		    }
		}

		Matcher m = NAMESPACES_PATTERN.matcher(nextLine);
		if (m.find()) {
		    result.put(m.group(3), m.group(1));
		    System.out.println("Adding prefix: '" + m.group(1) + "' for '" + m.group(3) + "'");
		}
	    }
	    reader.close();
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return result;
    }

    public static Map<String, String> loadPrefixToNamespaceMap() {
	return loadPrefix2NamespaceMap(Util.class.getClassLoader().getResourceAsStream(NAMESPACES_TABLE));
    }

    public static Map<String, String> loadNamespaceToPrefixMap() {
	return loadNamespaceToPrefixMap(Util.class.getClassLoader().getResourceAsStream(NAMESPACES_TABLE));
    }

    /**
     * Parse an NQuad string and convert it to Sesame's Statement object
     * 
     * @throws ParseException
     * 
     */
    public final static Statement parseStatement(String line) throws ParseException {
	Node[] nodes = NxParser.parseNodes(line);
	return parseStatement(nodes);

    }

    /**
     * Convert an NQuad to Sesame's Statement object
     * 
     * @throws ParseException
     * 
     */
    public final static Statement parseStatement(Node[] nodes) throws ParseException {
	try {
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
	    // Check if there is context
	    Resource context = null;
	    if (nodes.length > 3) {
		if (nodes[3] instanceof org.semanticweb.yars.nx.Resource) {
		    context = new URIImpl(nodes[3].toString());
		} else if (nodes[3] instanceof org.semanticweb.yars.nx.BNode) {
		    String nodeID = nodes[3].toString().substring(org.semanticweb.yars.nx.BNode.PREFIX.length());
		    context = new BNodeImpl(nodeID);
		}
	    }
	    if (context != null) {
		return new ContextStatementImpl(subject, predicate, object, context);
	    } else {
		return new StatementImpl(subject, predicate, object);
	    }
	} catch (IllegalArgumentException iae) {
	    throw new ParseException(iae);
	}
    }

}