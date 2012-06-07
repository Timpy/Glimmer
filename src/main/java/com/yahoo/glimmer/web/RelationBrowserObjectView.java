package com.yahoo.glimmer.web;

import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Resource;
import org.springframework.web.servlet.View;

import com.yahoo.glimmer.query.RDFIndex;
import com.yahoo.glimmer.query.RDFQueryResult;
import com.yahoo.glimmer.query.RDFResultItem;
import com.yahoo.glimmer.vocabulary.OwlUtils;

public class RelationBrowserObjectView implements View {

    @Override
    public String getContentType() {
	// Setting charset= means that response.getWriter() will return a Writer
	// that writes in that charset.
	return "text/xml; charset=UTF-8";
    }

    @Override
    public void render(Map<String, ?> model, HttpServletRequest request, HttpServletResponse response) throws Exception {
	Object object = model.get(QueryController.OBJECT_KEY);
	if (object == null) {
	    throw new IllegalArgumentException("Model does not contain an object!");
	}

	RDFIndex index = (RDFIndex) model.get(QueryController.INDEX_KEY);
	if (index == null) {
	    throw new IllegalArgumentException("Model does not contain an index!");
	}

	response.setContentType(getContentType());
	PrintWriter writer = response.getWriter();

	if (object instanceof RDFQueryResult) {
	    RDFQueryResult result = (RDFQueryResult) object;
	    RDFResultItem focus = result.getResultItems().get(0);
	    // Get the labels of related entities
	    focus.dereference(index.getSubjectsMPH());
	    writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
	    writer.write("<RelationViewerData>");
	    writer.write("<Settings appTitle=\"Relation browser demo\" startID=\"" + focus.uri + "\" defaultRadius=\"150\" maxRadius=\"180\">");
	    writer.write("<RelationTypes><DirectedRelation color=\"0x85CDE4\" lineSize=\"4\" labelText=\"association\"/></RelationTypes>");
	    writer.write("<NodeTypes><Node/><Comment/><Person/><Document/></NodeTypes>");
	    writer.write("</Settings>");
	    writer.write("<Nodes>");
	    StringBuffer desc = new StringBuffer();
	    for (RDFResultItem.Value value : focus.getValues()) {
		Object obj = value.triple.getObject();
		if (obj instanceof Literal) {
		    String predLabel = OwlUtils.getLocalName(IRI.create(value.triple.getPredicate().toString()));
		    desc.append(predLabel + " : " + obj + "\n");
		}
	    }
	    writer.write("<Node id=\"" + focus.uri + "\" name=\"" + (focus.getLabel() != null ? focus.getLabel() : focus.uri) + "\" dataURL=\""
		    + "http://dn002.sg.woo.gq1.yahoo.com:4080/woosearch-devel/query-yahookb.do?subject=" + focus.uri + "&amp;format=rb" + "\"><![CDATA[" + desc
		    + "]]></Node>");

	    for (RDFResultItem.Value value : focus.getValues()) {
		Object obj = value.triple.getObject();
		if (obj instanceof Resource) {
		    writer.write("<Node id=\"" + obj + "\" name=\"" + (value.label != null ? value.label : obj) + "\" dataURL=\""
			    + "http://dn002.sg.woo.gq1.yahoo.com:4080/woosearch-devel/query-yahookb.do?subject=" + obj + "&amp;format=rb" + "\" />");
		    // Also print the related objects...
		    RDFResultItem subItem = new RDFResultItem(index.getIndexedFields(), index.getCollection(), ((int) (long) index.getSubjectsMPH().get(
			    value.triple.getObject().toString())), 1.0d);
		    subItem.dereference(index.getSubjectsMPH());
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
		long id = index.getSubjectsMPH().get(value.triple.getObject().toString());
		if (id >= 0 && id < index.getSubjectsMPH().size64()) {
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
	} else {
	    throw new IllegalArgumentException("Model object is not an RDFQueryResult!");
	}
    }
}
