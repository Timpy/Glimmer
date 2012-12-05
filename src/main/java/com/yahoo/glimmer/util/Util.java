package com.yahoo.glimmer.util;

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

/**
 * Utility methods for microsearch
 * 
 * @author Peter Mika (pmika@yahoo-inc.com)
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;

public class Util {

    public final static SimpleDateFormat XSD_DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZ");

    private final static int BUFFER_SIZE = 2048;

    protected static Logger _logger = Logger.getLogger(Util.class);


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

    public static String encodeFieldName(String name) {
        return name.replaceAll("[^a-zA-Z0-9]+", "_");
    }
    
    public static <T extends Comparable<T>> int nullSafeCompareTo(T a, T b, boolean nullsFirst) {
	if (a == null) {
	    if (b == null) {
		return 0;
	    }
	    return nullsFirst ? -1 : 1;
	} else if (b == null) {
	    return nullsFirst ? 1 : -1;
	}
	return a.compareTo(b);
    }
    
    public static List<String> generateShortNames(List<String> names, Set<String> exclude, char delimiter) {
	if (names == null || names.isEmpty()) {
	    return Collections.emptyList();
	}
	if (exclude == null) {
	    exclude = Collections.emptySet();
	}
	
	ArrayList<String> shortNames = new ArrayList<String>(names.size());
	HashSet<String> used = new HashSet<String>(names.size() + exclude.size());
	used.addAll(exclude);
	
	for (String name : names) {
	    int i = name.length();
	    String shortName;
	    do {
		i = name.lastIndexOf(delimiter, i);
		if (i == -1) {
		    shortName = name;
		    break;
		}
		shortName = name.substring(i + 1);
		i--;
	    } while (used.contains(shortName));
	    if (used.contains(shortName)) {
		throw new IllegalArgumentException("None unique name " + name);
	    }
	    shortNames.add(shortName);
	    used.add(shortName);
	}

	return shortNames;
    }
}