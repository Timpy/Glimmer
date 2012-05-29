package com.yahoo.glimmer.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Split an NTuple into it's constituents and decode
 * 
 * @author pmika
 * 
 */
public class SplitTuple extends EvalFunc<Tuple> {

	// @Override
	public Tuple exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0)
			return null;

		Tuple result = TupleFactory.getInstance().newTuple();

		String tuple = (String) input.get(0);
		
		if (tuple == null || tuple.length() == 0) {
			return null;
		}

		Node[] nodes;
		try {
			nodes = NxParser.parseNodes(tuple);
			if (nodes.length < 3) {
				return null;
			}
			for (int i = 0; i < Math.min(nodes.length, 4); i++) {

				/**
				 * if (nodes[i] instanceof org.semanticweb.yars.nx.Resource) {
				 * result.set(i, nodes[i].toString()); } else if (nodes[i]
				 * instanceof org.semanticweb.yars.nx.BNode) { String nodeID =
				 * nodes[i].toString().substring(org.semanticweb.yars.nx.BNode.
				 * PREFIX.length()); result.set(i, nodeID); } else { if
				 * (((org.semanticweb.yars.nx.Literal)nodes[2]).getDatatype() !=
				 * null) { URI datatype = new
				 * URIImpl(((org.semanticweb.yars.nx.Literal
				 * )nodes[2]).getDatatype().toString()); object = new
				 * LiteralImpl(((org.semanticweb.yars.nx.Literal)nodes[2]).
				 * getUnescapedData(), datatype);
				 * 
				 * } else if
				 * (((org.semanticweb.yars.nx.Literal)nodes[2]).getLanguageTag()
				 * != null) { String language =
				 * ((org.semanticweb.yars.nx.Literal)nodes[2]).getLanguageTag();
				 * object = new
				 * LiteralImpl(((org.semanticweb.yars.nx.Literal)nodes
				 * [2]).getUnescapedData(), language); } else {
				 * 
				 * result.set(i, ((org.semanticweb.yars.nx.Literal)nodes[i]).
				 * getUnescapedData());
				 * 
				 * }
				 */

				result.append(nodes[i].toString());
			}
			
			//No context
			if (nodes.length < 4) {
				result.append("foo:bar");
			}
			
			if (nodes[2] instanceof org.semanticweb.yars.nx.Literal) {
				result.append("d");
			} else {
				result.append("o");
			}
			
		}  catch (Exception e) {
			System.err.println("Failed parsing tuple: " + input);
			e.printStackTrace();
		}

		return result;
	}

	public Schema outputSchema(Schema input) {
		try {
			Schema tupleSchema = new Schema();
			tupleSchema.add(new Schema.FieldSchema("s", DataType.CHARARRAY));
			tupleSchema.add(new Schema.FieldSchema("p", DataType.CHARARRAY));
			tupleSchema.add(new Schema.FieldSchema("o", DataType.CHARARRAY));
			tupleSchema.add(new Schema.FieldSchema("c", DataType.CHARARRAY));
			tupleSchema.add(new Schema.FieldSchema("type", DataType.CHARARRAY));
			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), input), tupleSchema,
					DataType.TUPLE));
		} catch (Exception e) {
			return null;
		}
	}

}
