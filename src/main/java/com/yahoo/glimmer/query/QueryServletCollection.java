package com.yahoo.glimmer.query;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.mg4j.query.ResultItem;
import it.unimi.dsi.mg4j.query.SelectedInterval;
import it.unimi.dsi.mg4j.query.nodes.Query;
import it.unimi.dsi.mg4j.search.score.DocumentScoreInfo;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Triple;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.yahoo.glimmer.vocabulary.OwlUtils;
import com.yahoo.glimmer.web.IndexMap;

@Deprecated
public class QueryServletCollection extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private final static Logger LOGGER = Logger.getLogger(QueryServletCollection.class);
    /**
     * Standard maximum number of items to be displayed (may be altered with the
     * <samp>m</samp> query parameter).
     */
    private final static int DEFAULT_MAX_NUM_ITEMS = 10;

    /** GsonBuilder object */
    private static Gson gson = new GsonBuilder().setPrettyPrinting().setDateFormat(DateFormat.LONG)
	    .registerTypeAdapter(CharSequence.class, new JsonSerializer<Object>() {
		public JsonElement serialize(Object object, Type arg1, JsonSerializationContext arg2) {
		    if (object instanceof CharSequence) {
			return new JsonPrimitive(((CharSequence) object).toString());
		    }
		    return null;
		}
	    }).registerTypeAdapter(Node.class, new JsonSerializer<Object>() {
		public JsonElement serialize(Object object, Type arg1, JsonSerializationContext arg2) {
		    if (object instanceof Node) {
			return new JsonPrimitive(((Node) object).toString());
		    }
		    return null;
		}
	    }).registerTypeAdapter(Triple.class, new JsonSerializer<Object>() {
		public JsonElement serialize(Object object, Type arg1, JsonSerializationContext arg2) {
		    if (object instanceof Triple) {
			JsonObject jo = new JsonObject();
			jo.addProperty("subject", ((Triple) object).getSubject().toString());
			jo.addProperty("predicate", ((Triple) object).getPredicate().toString());
			jo.addProperty("object", ((Triple) object).getObject().toString());
			return jo;
		    }
		    return null;
		}
	    })

	    .create();

    protected Context context;
    protected Map<String, RDFIndex> indexMap;

    public void init() throws ServletException {
	indexMap = IndexMap.getInstance();
    }

    /**
     * Remove duplicates from a result list until at least max unique results
     * are found or the list is exhausted
     * 
     * @return
     */
//    private ObjectArrayList<RDFResultItem> dedupe(RdfDisambiguator dis, List<RDFResultItem> list, int max) {
//	ObjectArrayList<RDFResultItem> result = new ObjectArrayList<RDFResultItem>();
//	for (RDFResultItem item : list) {
//	    // Compare it with all existing items
//	    boolean found = false;
//	    for (RDFResultItem current : result) {
//		if (dis.compare(current, item)) {
//		    found = true;
//		    // System.out.println(current.getText() + "SAMEAS\n" +
//		    // item.getText());
//		    // Store decisions in the items
//		    current.addDuplicate(item);
//		    item.addDuplicate(current);
//		    break;
//		}
//	    }
//	    if (!found) {
//		result.add(item);
//		if (result.size() >= max) {
//		    break;
//		}
//	    }
//	}
//	return result;
//    }

    /**
     * There are the following operations:
     * 
     * q: execute a query in MG4J format yq: execute a query in Yahoo query
     * format id: return the document for the given id subject: return the
     * document for the given subject
     * 
     */
    @SuppressWarnings("unused")
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

	String rawQuery = "";
	Query query = null;
	ObjectArrayList<RDFResultItem> resultItems = new ObjectArrayList<RDFResultItem>();
	RDFQueryResult result = null;
	int numResults = 0;

	try {

	    // This string is URL-encoded, and with the wrong coding.
	    // String query = request.getParameter( "q" ) != null ? new String(
	    // request.getParameter( "q" ).getBytes( "ISO-8859-1" ), "UTF-8" ) :
	    // null;

	    // Figure out which index we are going to use
	    RDFIndex index = indexMap.values().iterator().next();
	    String indexString = request.getParameter("index");
	    if (indexString != null) {
		if (indexMap.containsKey(indexString)) {
		    index = indexMap.get(indexString);
		} else {
		    response.sendError(400, "No such index: " + indexString);
		}
	    }

	    // Request for a document
	    if (request.getParameter("id") != null || request.getParameter("subject") != null) {
//		String idString = Util.decodeEntities(request.getParameter("id"));
//		String subject = Util.decodeEntities(request.getParameter("subject"));
//		LOGGER.info("id=" + idString + " subject=" + subject);
//		Long id;
//		if (idString != null) {
//		    id = Long.parseLong(idString);
//		} else if (index.subjectsMPH != null) {
//		    id = index.subjectsMPH.get(subject);
//		} else {
//		    response.sendError(400, "mph needs to be loaded for subject to work.");
//		    return;
//		}
//		if (id == -1 || id >= index.subjectsMPH.size64()) {
//		    response.sendError(400, "subject not in collection.");
//		    return;
//		}
//
//		// HACK
//		final RDFResultItem resultItem = new RDFResultItem(index.getIndexedFields(), index.getCollection(), Integer.parseInt(id.toString()), 1.0d);
//		if (index.getCollection() != null && subject != null && !resultItem.uri().equals(subject)) {
//		    // Ignore the result if the MPH tricked us and returned a
//		    // result with a different URI
//		    numResults = 0;
//		} else {
//		    resultItems.add(resultItem);
//		    numResults = 1;
//		}
//		// Stop the timer
//		if (index.getQueryLogger() != null)
//		    index.getQueryLogger().endQuery(query, numResults);
//		result = new RDFQueryResult(rawQuery, query, numResults, resultItems, (index.getQueryLogger() != null) ? index.getQueryLogger().getTime() : 0l);
		response.sendError(400, "Old get document.");
	    } else if (request.getParameter("mq") != null) {
//		// We've got an MG4J query, use MG4J's SimpleParser
//		rawQuery = Util.decodeEntities(request.getParameter("mq"));
//		query = new SimpleParser().parse(rawQuery);
		response.sendError(400, "Old MG4J query.");
	    } else if (request.getParameter("q") != null || request.getParameter("yq") != null) {
//		// We've got a Yahoo query, use our own query parser
//		if (request.getParameter("q") != null) {
//		    rawQuery = Util.decodeEntities(request.getParameter("q"));
//		} else {
//		    rawQuery = Util.decodeEntities(request.getParameter("yq"));
//		}
//
//		/*
//		 * ArrayList<String> segments = null; if
//		 * (request.getParameter("noQLAS") != null) { segments = new
//		 * ArrayList<String>(); } else { segments =
//		 * QLASService.toStringSegments
//		 * (QLASService.segmentator(rawQuery)); }
//		 * segments.add(rawQuery);
//		 */
//		try {
//		    query = index.getParser().parse(rawQuery);
//		} catch (QueryParserException e) {
//		    response.sendError(400, "Query failed to parse");
//		    return;
//		}
		response.sendError(400, "Old Yahoo query.");
	    } else {
//		// No subject or query parameter
//		// Return statistics of the index and the collection
//		String callback = request.getParameter("callback");
//		if (callback != null && !callback.equals("")) {
//		    response.setContentType("text/javascript");
//		    response.setCharacterEncoding("UTF-8");
//		    PrintWriter out = response.getWriter();
//
//		    out.write(callback + "(" + gson.toJson(index.getStatistics()) + ");");
//		    out.close();
//		} else {
//		    response.sendError(400, "callback is required");
//		}
		response.sendError(400, "Old no action.");
	    }

	    if (query != null) {

		// Sanitize parameters.
		int start = 0, maxNumItems = DEFAULT_MAX_NUM_ITEMS;
		try {
		    maxNumItems = Integer.parseInt(request.getParameter("m"));
		} catch (NumberFormatException dontCare) {
		}
		try {
		    start = Integer.parseInt(request.getParameter("s"));
		} catch (NumberFormatException dontCare) {
		}

		if (maxNumItems < 0 || maxNumItems > 10000)
		    maxNumItems = DEFAULT_MAX_NUM_ITEMS;

		boolean dedupe = request.getParameter("dedupe") != null && !request.getParameter("dedupe").equals("false");
		// When deduping, we ask for twice as many results
		// We will later reduce this to the original amount
		if (dedupe) {
		    maxNumItems *= 2;
		}

		if (start < 0)
		    start = 0;

		try {
		    // Reconfigure scorer
		    if (context != null) {
			try {
			    // TODO do we need this? tep.
			    //context.reload();
			    //context.update(request);
			    LOGGER.info("Reconfiguring scorer");
			    index.reconfigure(context);
			} catch (Exception e) {
			    e.printStackTrace();
			}
		    }

		    ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>();

		    numResults = index.process(query, start, maxNumItems, results);

		    if (results.size() > maxNumItems)
			results.size(maxNumItems);

		    if (!results.isEmpty()) {
			for (int i = 0; i < results.size(); i++) {
			    DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi = results.get(i);
			    LOGGER.debug("Intervals for item " + i);
			    LOGGER.debug("score " + dsi.score);
			    final RDFResultItem resultItem = new RDFResultItem(index.getIndexedFields(), index.getCollection(), dsi.document, dsi.score);
			    resultItems.add(resultItem);

			}
			// Dedupe results if requested
//			if (dedupe) {
//			    // This would replace the result list with the
//			    // disambiguated list
//			    resultItems = dedupe(disambiguator, resultItems, maxNumItems / 2);
//			    // This just marks the duplicates
//			    // dedupe(disambiguator, resultItems, maxNumItems /
//			    // 2 );
//			}
		    }

		    // Stop the timer
		    if (index.getQueryLogger() != null)
			index.getQueryLogger().endQuery(query, numResults);

		    // Generating result items
		    long time = System.currentTimeMillis();
		    result = new RDFQueryResult(rawQuery, query, numResults, resultItems, (index.getQueryLogger() != null) ? index.getQueryLogger().getTime() : 0l);
		    LOGGER.info("Generating results took " + (System.currentTimeMillis() - time) + " ms");

		    // Dereferencing
		    if (request.getParameter("deref") != null && !request.getParameter("deref").equals("false")) {
			time = System.currentTimeMillis();
			result.dereference(index.subjectsMPH);
			LOGGER.info("Dereferencing took " + (System.currentTimeMillis() - time) + " ms");
		    }
		} catch (Exception e) {
		    LOGGER.error(e);
		    response.sendError(400, e.getMessage());
		    return;
		}
	    }

	    // Render result depending on format
	    String format = request.getParameter("format");
	    if (format != null && format.equals("micro")) {
		// Redirect to microsearch
		String mfQuery = "";
		for (int i = 0; i < resultItems.size(); i++) {
		    if (i != 0)
			mfQuery += " OR ";
		    mfQuery += "wwwurl:" + resultItems.get(i).uri;
		}

		// TODO ??
		response.sendRedirect("http://ybcn-svr6.barcelona.corp.yahoo.com:8080/mf-devel/search.do?p=" + URLEncoder.encode(mfQuery, "UTF-8"));

	    } else if (format != null && format.equals("xml")) {
		// FIXME: this is not XML!!!!
//		response.setContentType("text/plain");
//		response.setCharacterEncoding("UTF-8");
//		PrintWriter out = response.getWriter();
//		for (int i = 0; i < resultItems.size(); i++) {
//		    ResultItem r = resultItems.get(i);
//		    out.println("<result> " + (i + 1) + "</result>");
//		    if (index.index_idfs != null) {
//			out.println("<documentSize> " + index.index_idfs.sizes.getInt(r.doc) + "</documentSize>");
//		    }
//		    out.println("<score>" + r.score + "</score>");
//		    out.println("<uri>" + r.uri + "</uri>");
//
//		    Document d = index.getCollection().document(r.doc);
//		    out.println("<contents>" + Util.getText(d) + "</contents>");
//		    d.close();
//
//		}
//		out.close();
		return;
	    } else if (format != null && format.equals("txt")) {
		// Return URIs and docs separated by tab
		response.setContentType("text/plain");
		response.setCharacterEncoding("UTF-8");
		PrintWriter out = response.getWriter();
		for (int i = 0; i < resultItems.size(); i++) {
		    ResultItem r = resultItems.get(i);
		    Document d = index.getCollection().document(r.doc);
		    out.println(r.uri + "\t" + Util.getText(d));
		    d.close();
		}
		out.close();
		return;
	    } else if (format != null && format.equals("json")) {
		long time = System.currentTimeMillis();
		String json = gson.toJson(result);
		LOGGER.info("JSON serialization took " + (System.currentTimeMillis() - time) + " ms");
		time = System.currentTimeMillis();
		String callback = request.getParameter("callback");
		if (callback != null && !callback.equals("")) {
		    response.setContentType("text/javascript");
		    response.setCharacterEncoding("UTF-8");
		    PrintWriter out = response.getWriter();
		    out.write(callback + "(" + json + ");");
		    out.close();
		} else {
		    response.setContentType("text/json");
		    response.setCharacterEncoding("UTF-8");
		    PrintWriter out = response.getWriter();
		    out.write(json);
		    out.close();
		}
		LOGGER.info("Writing to output took " + (System.currentTimeMillis() - time) + " ms");
		return;
	    } else if (format != null && format.equals("doc")) {
		// Return document text from the collection
		response.setContentType("text/plain");
		response.setCharacterEncoding("UTF-8");
		PrintWriter writer = response.getWriter();
		for (int i = 0; i < resultItems.size(); i++) {
		    Document d = index.getCollection().document(resultItems.get(i).doc);
		    writer.write(Util.getText(d));
		    d.close();
		}
		writer.close();
		return;
	    } else if (format != null && format.equals("rb")) {
		// Hack: rewrite to recurse at arbitrary depth
		response.setContentType("text/xml");
		response.setCharacterEncoding("UTF-8");
		PrintWriter writer = response.getWriter();
		RDFResultItem focus = resultItems.get(0);
		// Get the labels of related entities
		focus.dereference(index.subjectsMPH);
		writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		writer.write("<RelationViewerData>");
		writer.write("<Settings appTitle=\"Relation browser demo\" startID=\"" + focus.uri + "\" defaultRadius=\"150\" maxRadius=\"180\">");
		writer.write("<RelationTypes><DirectedRelation color=\"0x85CDE4\" lineSize=\"4\" labelText=\"association\"/></RelationTypes>");
		writer.write("<NodeTypes><Node/><Comment/><Person/><Document/></NodeTypes>");
		writer.write("</Settings>");
		writer.write("<Nodes>");
		StringBuffer desc = new StringBuffer();
		for (RDFResultItem.Value value : focus.getValues()) {
		    Object object = value.triple.getObject();
		    if (object instanceof Literal) {
			String predLabel = OwlUtils.getLocalName(IRI.create(value.triple.getPredicate().toString()));
			desc.append(predLabel + " : " + object + "\n");
		    }
		}
		writer.write("<Node id=\"" + focus.uri + "\" name=\"" + (focus.getLabel() != null ? focus.getLabel() : focus.uri) + "\" dataURL=\""
			+ "http://dn002.sg.woo.gq1.yahoo.com:4080/woosearch-devel/query-yahookb.do?subject=" + focus.uri + "&amp;format=rb" + "\"><![CDATA["
			+ desc + "]]></Node>");

		for (RDFResultItem.Value value : focus.getValues()) {
		    Object object = value.triple.getObject();
		    if (object instanceof Resource) {
			writer.write("<Node id=\"" + object + "\" name=\"" + (value.label != null ? value.label : object) + "\" dataURL=\""
				+ "http://dn002.sg.woo.gq1.yahoo.com:4080/woosearch-devel/query-yahookb.do?subject=" + object + "&amp;format=rb" + "\" />");
			// Also print the related objects...
			RDFResultItem subItem = new RDFResultItem(index.getIndexedFields(), index.getCollection(),
				((int) (long) index.subjectsMPH.get(value.triple.getObject().toString())), 1.0d);
			subItem.dereference(index.subjectsMPH);
			for (RDFResultItem.Value subValue : subItem.getValues()) {
			    Object subObject = subValue.triple.getObject();
			    if (subObject instanceof Resource) {
				writer.write("<Node id=\"" + subObject + "\" name=\"" + (subValue.label != null ? subValue.label : subObject) + "\" dataURL=\""
					+ "http://dn002.sg.woo.gq1.yahoo.com:4080/woosearch-devel/query-yahookb.do?subject=" + subObject + "&amp;format=rb"
					+ "\" />");

			    }
			}

		    }
		}
		writer.write("</Nodes>");
		writer.write("<Relations>");
		for (RDFResultItem.Value value : focus.getValues()) {
		    if (value.triple.getObject() instanceof Resource) {
			String predLabel = OwlUtils.getLocalName(IRI.create(value.triple.getPredicate().toString()));
			writer.write("<DirectedRelation fromID=\"" + value.triple.getSubject() + "\" toID=\"" + value.triple.getObject() + "\" labelText=\""
				+ predLabel + "\"/>");
		    }
		    // Also print relations of related objects
		    long id = index.subjectsMPH.get(value.triple.getObject().toString());
		    if (id >= 0 && id < index.subjectsMPH.size64()) {
			RDFResultItem subItem = new RDFResultItem(index.getIndexedFields(), index.getCollection(), (int) id, 1.0d);
			for (RDFResultItem.Value subValue : subItem.getValues()) {
			    if (subValue.triple.getObject() instanceof Resource) {
				String predLabel = OwlUtils.getLocalName(IRI.create(subValue.triple.getPredicate().toString()));
				writer.write("<DirectedRelation fromID=\"" + subValue.triple.getSubject() + "\" toID=\"" + subValue.triple.getObject()
					+ "\" labelText=\"" + predLabel + "\"/>");
			    }
			}
		    }

		}
		writer.write("</Relations>");
		writer.write("</RelationViewerData>");
		writer.close();
		return;
	    } else {
		response.sendError(400, "Invalid or empty format");
		return;
		// Set field names even if there is no query
		// request.setAttribute("fieldNames",
		// index.getIndexedFieldNames());
		//
		// request.setAttribute("result", result);
		//
		// if (index.index_idfs != null) {
		// request.setAttribute("sizes", index.index_idfs.sizes);
		// }
		// String target="/search.jsp";
		// if (request.getParameter("target") != null) {
		// target = "/" + request.getParameter("target");
		// }
		// getServletContext().getRequestDispatcher(target).forward(request,
		// response);
	    }
	} catch (Exception e) {
	    LOGGER.error(e);
	    response.sendError(400, e.getMessage());
	    return;
	}
    }

    @Override
    public void destroy() {
	for (RDFIndex index : indexMap.values()) {
	    if (index != null)
		index.destroy();
	}
	super.destroy();
    }

}
