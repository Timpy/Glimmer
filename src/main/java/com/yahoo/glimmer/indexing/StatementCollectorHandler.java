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
