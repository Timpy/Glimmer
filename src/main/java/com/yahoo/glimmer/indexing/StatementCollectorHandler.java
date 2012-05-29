package com.yahoo.glimmer.indexing;

import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * An RDFHandler that simply collects the parsed statements in a Set
 * 
 * Note: Using a Set means that the same statement from multiple contexts will
 * be only indexed once.
 * 
 * @author pmika
 * 
 */
public class StatementCollectorHandler implements RDFHandler {

    private HashSet<Statement> stmts = new HashSet<Statement>();

    public void endRDF() throws RDFHandlerException {
	// TODO Auto-generated method stub

    }

    public void handleComment(String arg0) throws RDFHandlerException {
	// TODO Auto-generated method stub

    }

    public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
	// TODO Auto-generated method stub

    }

    public void handleStatement(Statement stmt) throws RDFHandlerException {
	stmts.add(stmt);

    }

    public void startRDF() throws RDFHandlerException {
	// TODO Auto-generated method stub

    }

    public Set<Statement> getStatements() {
	return stmts;
    }

}
