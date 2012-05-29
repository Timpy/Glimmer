package com.yahoo.glimmer.util;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.WeakHashMap;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;


/**
 * This writer can write either NTriples or a more verbose format with URL and
 * adjunctID
 * 
 * @author pmika@yahoo-inc.com
 * 
 */
public class NTuplesWriter implements RDFHandler {

	private static WeakHashMap<String, String> sha1Cache = new WeakHashMap<String, String>();

	private URL url;
	private String id;
	private StringWriter writer = new StringWriter();
	private boolean nQuads = true;
	
	//We need the URL for bNode globalization even when writing out triples only
	public NTuplesWriter(URL url, boolean nQuads) {
		this.url = url;
		this.nQuads = nQuads;
	}
	
	public NTuplesWriter(URL url, String id) {

		this.url = url;
		this.id = id;
		nQuads = false;
	}

	public void handleStatement(Statement st) throws RDFHandlerException {

		Resource subj = st.getSubject();
		URI pred = st.getPredicate();
		Value obj = st.getObject();
		Resource ctx = null;
		
		if (st instanceof ContextStatementImpl) ctx = ((ContextStatementImpl)st).getContext();

		try {
			// SUBJECT
			writeResource(subj);
			writer.write("\t");

			// PREDICATE
			writeURI(pred);
			writer.write("\t");
			
			
			// OBJECT
			if (obj instanceof Resource) {
				writeResource((Resource) obj);
			} else if (obj instanceof Literal) {
				writeLiteral((Literal) obj);
			}
			writer.write("\t");
			
			// CONTEXT
			if (ctx != null) {
				writeResource((Resource) ctx);
				writer.write("\t");
			} 
			
			if (url != null && !nQuads)  {
				// URL
				writeURI(new URIImpl(url.toExternalForm()));
				writer.write("\t");
			}
			
			
			
			if (id != null && !nQuads) {
				// ADJUNCT ID
				writeLiteral(new LiteralImpl(id));
				writer.write("\t");
			}

			
			writer.write('.');
			

			writeNewLine();
		} catch (IOException e) {
			throw new RDFHandlerException(e);
		}
	}

	private void writeResource(Resource res) throws IOException {
		if (res instanceof BNode) {
			writeBNode((BNode) res);
		} else {
			writeURI((URI) res);
		}
	}

	private void writeURI(URI uri) throws IOException {
		writer.write(NTriplesUtil.toNTriplesString(uri));
	}

	/**
	 * Postfix BNodes with the SHA1 digest of the URL. Cache checksums for
	 * efficiency
	 * 
	 * @param bNode
	 * @throws IOException
	 */
	private void writeBNode(BNode bNode) throws IOException {
		String prefix = NTriplesUtil.toNTriplesString(bNode);
		if (!sha1Cache.containsKey(url.toExternalForm())) {
			try {
				sha1Cache.put(url.toExternalForm(), Util.createSHA1(url
						.toExternalForm()));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				throw new IOException(e.getMessage());
			}
		}
		writer.write(prefix + sha1Cache.get(url.toExternalForm()));
	}

	private void writeLiteral(Literal lit) throws IOException {
		writer.write(NTriplesUtil.toNTriplesString(lit));
	}

	private void writeNewLine() throws IOException {
		writer.write("\n");
	}

	public String getResult() {
		return writer.toString();
	}

	public void endRDF() throws RDFHandlerException {

	}

	public void handleComment(String arg0) throws RDFHandlerException {

	}

	public void handleNamespace(String arg0, String arg1)
			throws RDFHandlerException {

	}

	public void startRDF() throws RDFHandlerException {

	}
	

	public void clear() {
		writer.getBuffer().setLength(0);
	}

}
