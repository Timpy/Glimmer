package com.yahoo.glimmer.util;
/*  Sesame - Storage and Querying architecture for RDF and RDF Schema
 *  Copyright (C) 2001-2005 Aduna
 *
 *  Contact: 
 *  	Aduna
 *  	Prinses Julianaplein 14 b
 *  	3817 CS Amersfoort
 *  	The Netherlands
 *  	tel. +33 (0)33 465 99 87
 *  	fax. +33 (0)33 465 99 87
 *
 *  	http://aduna.biz/
 *  	http://www.openrdf.org/
 *  
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

/**
 * Utility methods for N-Triples encoding/decoding.
 **/
public class NTriplesUtil {

/*------------------------------------------------------+
| Methods                                               |
+------------------------------------------------------*/

	/**
	 * Parses an N-Triples value, creates an object for it using the
	 * supplied ValueFactory and returns this object.
	 *
	 * @param nTriplesValue The N-Triples value to parse.
	 * @param valueFactory The ValueFactory to use for creating the
	 * object.
	 * @return An object representing the parsed value.
	 * @exception IllegalArgumentException If the supplied value could not be
	 * parsed correctly.
	 **/
	public static Value parseValue(String nTriplesValue, ValueFactory valueFactory)
		throws IllegalArgumentException
	{
		if (nTriplesValue.startsWith("<")) {
			return parseURI(nTriplesValue, valueFactory);
		}
		else if (nTriplesValue.startsWith("_:")) {
			return parseBNode(nTriplesValue, valueFactory);
		}
		else if (nTriplesValue.startsWith("\"")) {
			return parseLiteral(nTriplesValue, valueFactory);
		}
		else {
			throw new IllegalArgumentException(
					"Not a legal N-Triples value: " + nTriplesValue);
		}
	}

	/**
	 * Parses an N-Triples resource, creates an object for it using
	 * the supplied ValueFactory and returns this object.
	 *
	 * @param nTriplesResource The N-Triples resource to parse.
	 * @param valueFactory The ValueFactory to use for creating the
	 * object.
	 * @return An object representing the parsed resource.
	 * @exception IllegalArgumentException If the supplied resource could not be
	 * parsed correctly.
	 **/
	public static Resource parseResource(String nTriplesResource, ValueFactory valueFactory)
		throws IllegalArgumentException
	{
		if (nTriplesResource.startsWith("<")) {
			return parseURI(nTriplesResource, valueFactory);
		}
		else if (nTriplesResource.startsWith("_:")) {
			return parseBNode(nTriplesResource, valueFactory);
		}
		else {
			throw new IllegalArgumentException(
					"Not a legal N-Triples resource: " + nTriplesResource);
		}
	}

	/**
	 * Parses an N-Triples URI, creates an object for it using the
	 * supplied ValueFactory and returns this object.
	 *
	 * @param nTriplesURI The N-Triples URI to parse.
	 * @param valueFactory The ValueFactory to use for creating the
	 * object.
	 * @return An object representing the parsed URI.
	 * @exception IllegalArgumentException If the supplied URI could not be
	 * parsed correctly.
	 **/
	public static URI parseURI(String nTriplesURI, ValueFactory valueFactory)
		throws IllegalArgumentException
	{
		if (nTriplesURI.startsWith("<") && nTriplesURI.endsWith(">")) {
			String uri = nTriplesURI.substring(1, nTriplesURI.length() - 1);
			uri = unescapeString(uri);
			return valueFactory.createURI(uri);
		}
		else {
			throw new IllegalArgumentException("Not a legal N-Triples URI: " + nTriplesURI);
		}
	}

	/**
	 * Parses an N-Triples bNode, creates an object for it using the
	 * supplied ValueFactory and returns this object.
	 *
	 * @param nTriplesBNode The N-Triples bNode to parse.
	 * @param valueFactory The ValueFactory to use for creating the
	 * object.
	 * @return An object representing the parsed bNode.
	 * @exception IllegalArgumentException If the supplied bNode could not be
	 * parsed correctly.
	 **/
	public static BNode parseBNode(String nTriplesBNode, ValueFactory valueFactory)
		throws IllegalArgumentException
	{
		if (nTriplesBNode.startsWith("_:")) {
			return valueFactory.createBNode(nTriplesBNode.substring(2));
		}
		else {
			throw new IllegalArgumentException(
					"Not a legal N-Triples URI: " + nTriplesBNode);
		}
	}

	/**
	 * Parses an N-Triples literal, creates an object for it using the
	 * supplied ValueFactory and returns this object.
	 *
	 * @param nTriplesLiteral The N-Triples literal to parse.
	 * @param valueFactory The ValueFactory to use for creating the
	 * object.
	 * @return An object representing the parsed literal.
	 * @exception IllegalArgumentException If the supplied literal could not be
	 * parsed correctly.
	 **/
	public static Literal parseLiteral(String nTriplesLiteral, ValueFactory valueFactory)
		throws IllegalArgumentException
	{
		if (nTriplesLiteral.startsWith("\"")) {
			// Find string separation points
			int endLabelIdx = _findEndOfLabel(nTriplesLiteral);

			if (endLabelIdx != -1) {
				int startLangIdx = nTriplesLiteral.indexOf("@", endLabelIdx);
				int startDtIdx = nTriplesLiteral.indexOf("^^", endLabelIdx);

				if (startLangIdx != -1 && startDtIdx != -1) {
					throw new IllegalArgumentException("Literals can not have both a language and a datatype");
				}

				// Get label
				String label = nTriplesLiteral.substring(1, endLabelIdx);
				label = unescapeString(label);

				if (startLangIdx != -1) {
					// Get language
					String language = nTriplesLiteral.substring(startLangIdx + 1);
					return valueFactory.createLiteral(label, language);
				}
				else if (startDtIdx != -1) {
					// Get datatype
					String datatype = nTriplesLiteral.substring(startDtIdx + 2);
					URI dtURI = parseURI(datatype, valueFactory);
					return valueFactory.createLiteral(label, dtURI);
				}
				else {
					return valueFactory.createLiteral(label);
				}
			}
		}

		throw new IllegalArgumentException(
				"Not a legal N-Triples literal: " + nTriplesLiteral);
	}

	/**
	 * Finds the end of the label in a literal string. This method
	 * takes into account that characters can be escaped using
	 * backslashes.
	 *
	 * @return The index of the double quote ending the label, or
	 * <tt>-1</tt> if it could not be found.
	 **/
	private static int _findEndOfLabel(String nTriplesLiteral) {
		// First character of literal is guaranteed to be a double
		// quote, start search at second character.

		boolean previousWasBackslash = false;

		for (int i = 1; i < nTriplesLiteral.length(); i++) {
			char c = nTriplesLiteral.charAt(i);

			if (c == '"' && !previousWasBackslash) {
				return i;
			}
			else if (c == '\\' && !previousWasBackslash) {
				// start of escape
				previousWasBackslash = true;
			}
			else if (previousWasBackslash) {
				// c was escaped
				previousWasBackslash = false;
			}
		}

		return -1;
	}

	/**
	 * Creates an N-Triples string for the supplied value.
	 **/
	public static String toNTriplesString(Value value) {
		if (value instanceof Resource) {
			return toNTriplesString((Resource)value);
		}
		else {
			return toNTriplesString((Literal)value);
		}
	}

	/**
	 * Creates an N-Triples string for the supplied resource.
	 **/
	public static String toNTriplesString(Resource resource) {
		if (resource instanceof URI) {
			return toNTriplesString((URI)resource);
		}
		else {
			return toNTriplesString((BNode)resource);
		}
	}

	/**
	 * Creates an N-Triples string for the supplied URI.
	 **/
	public static String toNTriplesString(URI uri) {
		return "<" + escapeString(uri.toString()) + ">";
	}

	/**
	 * Creates an N-Triples string for the supplied bNode.
	 **/
	public static String toNTriplesString(BNode bNode) {
		return "_:" + bNode.getID();
	}

	/**
	 * Creates an N-Triples string for the supplied literal.
	 **/
	public static String toNTriplesString(Literal lit) {
		// Do some character escaping on the label:
		StringBuffer result = new StringBuffer(128);
		result.append("\"");
		result.append(escapeString(lit.getLabel()));
		result.append("\"");

		if (lit.getDatatype() != null) {
			// Append the literal's datatype
			result.append("^^");
			result.append(toNTriplesString(lit.getDatatype()));
		}
		else if (lit.getLanguage() != null) {
			// Append the literal's language
			result.append("@");
			result.append(lit.getLanguage());
		}

		return result.toString();
	}

	/**
	 * Checks whether the supplied character is a letter or number
	 * according to the N-Triples specification.
	 * @see #isLetter
	 * @see #isNumber
	 **/
	public static boolean isLetterOrNumber(int c) {
		return isLetter(c) || isNumber(c);
	}

	/**
	 * Checks whether the supplied character is a letter according to
	 * the N-Triples specification. N-Triples letters are A - Z and a - z.
	 **/
	public static boolean isLetter(int c) {
		return (c >= 65 && c <= 90) || // A - Z
		       (c >= 97 && c <= 122);  // a - z
	}

	/**
	 * Checks whether the supplied character is a number according to
	 * the N-Triples specification. N-Triples numbers are 0 - 9.
	 **/
	public static boolean isNumber(int c) {
		return (c >= 48 && c <= 57); // 0 - 9
	}

	/**
	 * Escapes a Unicode string to an all-ASCII character sequence. Any special
	 * characters are escaped using backslashes (<tt>"</tt> becomes <tt>\"</tt>,
	 * etc.), and non-ascii/non-printable characters are escaped using Unicode
	 * escapes (<tt>&#x5C;uxxxx</tt> and <tt>&#x5C;Uxxxxxxxx</tt>).
	 **/
	public static String escapeString(String label) {
		int labelLength = label.length();
		StringBuffer result = new StringBuffer(2 * labelLength);

		for (int i = 0; i < labelLength; i++) {
			char c = label.charAt(i);
			int cInt = (int)c;

			if (c == '\\') {
				result.append("\\\\");
			}
			else if (c == '"') {
				result.append("\\\"");
			}
			else if (c == '\n') {
				result.append("\\n");
			}
			else if (c == '\r') {
				result.append("\\r");
			}
			else if (c == '\t') {
				result.append("\\t");
			}
			else if (
				cInt >= 0x0 && cInt <= 0x8 ||
				cInt == 0xB || cInt == 0xC ||
				cInt >= 0xE && cInt <= 0x1F ||
				cInt >= 0x7F && cInt <= 0xFFFF)
			{
				result.append("\\u");
				result.append(toHexString(cInt, 4));
			}
			else if (cInt >= 0x10000 && cInt <= 0x10FFFF) {
				result.append("\\U");
				result.append(toHexString(cInt, 8));
			}
			else {
				result.append(c);
			}
		}

		return result.toString();
	}

	/**
	 * Unescapes an escaped Unicode string. Any Unicode sequences
	 * (<tt>&#x5C;uxxxx</tt> and <tt>&#x5C;Uxxxxxxxx</tt>) are restored to the
	 * value indicated by the hexadecimal argument and any backslash-escapes
	 * (<tt>\"</tt>, <tt>\\</tt>, etc.) are decoded to their original form.
	 *
	 * @param s An escaped Unicode string.
	 * @return The unescaped string.
	 * @exception IllegalArgumentException If the supplied string is not a
	 * correctly escaped N-Triples string.
	 **/
	public static String unescapeString(String s) {
		int backSlashIdx = s.indexOf('\\');

		if (backSlashIdx == -1) {
			// No escaped characters found
			return s;
		}

		int startIdx = 0;
		int sLength = s.length();
		StringBuffer buf = new StringBuffer(sLength);

		while (backSlashIdx != -1) {
			buf.append(s.substring(startIdx, backSlashIdx));

			if (backSlashIdx + 1 >= sLength) {
				throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			char c = s.charAt(backSlashIdx + 1);

			if (c == 't') {
				buf.append('\t');
				startIdx = backSlashIdx + 2;
			}
			else if (c == 'r') {
				buf.append('\r');
				startIdx = backSlashIdx + 2;
			}
			else if (c == 'n') {
				buf.append('\n');
				startIdx = backSlashIdx + 2;
			}
			else if (c == '"') {
				buf.append('"');
				startIdx = backSlashIdx + 2;
			}
			else if (c == '\\') {
				buf.append('\\');
				startIdx = backSlashIdx + 2;
			}
			else if (c == 'u') {
				// \\uxxxx
				if (backSlashIdx + 5 >= sLength) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				String xx = s.substring(backSlashIdx + 2, backSlashIdx + 6);

				try {
					c = (char)Integer.parseInt(xx, 16);
					buf.append( (char)c );

					startIdx = backSlashIdx + 6;
				}
				catch (NumberFormatException e) {
					throw new IllegalArgumentException("Illegal Unicode escape sequence '\\u" + xx + "' in: " + s);
				}
			}
			else if (c == 'U') {
				// \\Uxxxxxxxx
				if (backSlashIdx + 9 >= sLength) {
					throw new IllegalArgumentException("Incomplete Unicode escape sequence in: " + s);
				}
				String xx = s.substring(backSlashIdx + 2, backSlashIdx + 10);

				try {
					c = (char)Integer.parseInt(xx, 16);
					buf.append( (char)c );

					startIdx = backSlashIdx + 10;
				}
				catch (NumberFormatException e) {
					throw new IllegalArgumentException("Illegal Unicode escape sequence '\\U" + xx + "' in: " + s);
				}
			}
			else {
				throw new IllegalArgumentException("Unescaped backslash in: " + s);
			}

			backSlashIdx = s.indexOf('\\', startIdx);
		}

		buf.append( s.substring(startIdx) );

		return buf.toString();
	}

	/**
	 * Converts a decimal value to a hexadecimal string represention
	 * of the specified length.
	 *
	 * @param decimal A decimal value.
	 * @param stringLength The length of the resulting string.
	 **/
	public static String toHexString(int decimal, int stringLength) {
		StringBuffer result = new StringBuffer(stringLength);

		String hexVal = Integer.toHexString(decimal).toUpperCase();

		// insert zeros if hexVal has less than stringLength characters:
		int nofZeros = stringLength - hexVal.length();
		for (int i = 0; i < nofZeros; i++) {
			result.append('0');
		}

		result.append(hexVal);

		return result.toString();
	}
}
