package com.yahoo.glimmer.query;

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
import it.unimi.di.mg4j.document.Document;
import it.unimi.di.mg4j.index.BitStreamIndex;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import javax.servlet.ServletException;

public class SetDocumentPriors {
    // public static final int IMPORTANT = 2;
    // default
    // public static final int NEUTRAL = 1;
    // public static final int UNIMPORTANT = 0;

    // Change to a Constants class
    public static final String IMPORTANT = "2";
    public static final String NEUTRAL = "1";
    public static final String UNIMPORTANT = "0";
    private HashMap<String, Integer> rules = new HashMap<String, Integer>();

    public static void main(String args[]) {
	try {
	    Context context = new Context(args[0]);
	    SetDocumentPriors engine = new SetDocumentPriors(context);
	    engine.calculatePriors();
	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

    private RDFIndex index;
    private Context context;

    HashMap<String, String> hosts = new HashMap<String, String>();
    HashMap<String, String> nonhosts = new HashMap<String, String>();

    public SetDocumentPriors(Context context) throws ServletException {
	this.context = context;
	index = new RDFIndex(context);
    }

    public void calculatePriors() {
	readPriorRules();
	System.out.println("Read rules " + rules);
	HashMap<Integer, Integer> priors = new HashMap<Integer, Integer>();
	Document d;
	try {
	    // TODO THIS IS NOT WORKING
	    // BitStreamIndex index = (BitStreamIndex)
	    // indexMap.get(fieldNames[0]);
	    BitStreamIndex ostia = (BitStreamIndex) index.getField("ostia");
	    for (int i = 0; i < ostia.numberOfDocuments; i++) {

		d = (Document) index.getCollection().document(i);
		URI uri = null;
		String host = null;
		try {
		    uri = new URI(d.title().toString());
		    if (uri != null) {
			host = uri.getHost();
			// } else {
			// priors.put(i, NEUTRAL);
		    }
		} catch (URISyntaxException use) {
		}
		if (host != null) {
		    // System.out.println("HOST FOUND:"+host);
		    if (!hosts.containsKey(host))
			hosts.put(host, host);
		    for (String rule : rules.keySet()) {
			if (rule.contains(host))
			    priors.put(i, rules.get(rule));
		    }
		} else {
		    // System.out.println("NOT A HOST "+d.title());
		    if (!nonhosts.containsKey(d.title()))
			nonhosts.put(d.title().toString(), d.title().toString());
		}
		d.close();
	    }/*
	      * for(String hostS : hosts.keySet()){
	      * System.out.println("HOST	"+hostS); } for(String hostS :
	      * nonhosts.keySet()){ System.out.println("NON HOST	"+hostS); }
	      */
	    System.out.print("Serializing priors...");
	    BinIO.storeObject(priors, context.getPathToDocumentPriors());
	    System.out.println("done");
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void readPriorRules() {
	try {
	    BufferedReader br = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(context.getPathToPriorRules()))));
	    String line;
	    while ((line = br.readLine()) != null) {
		if (!line.trim().equals("")) {
		    String parts[] = line.split("=");
		    rules.put(parts[0], Integer.parseInt(parts[1]));
		}
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}