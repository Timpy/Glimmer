String.prototype.startsWith = function(str) {
	return (this.match("^" + str) == str)
}

var stats = {};
var fieldShortNames;
var fieldLongNames;
var store = {};
var ns;
var webapp = "";
var RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
var ONTOLOGY_ROOT = "http://www.w3.org/2002/07/owl#Thing";
var contextsToColour = []; // TODO populate from server.

var argString = window.location.href.split('?')[1];
var args = []; // URL arguments
if (argString != undefined) {
	args = parse(argString);
}

function getProviderName(sstring) {
	if (contextsToColour[sstring] === undefined)
		return getLocalName(sstring);
	else
		return contextsToColour[sstring];
}

function getLocalName(uri) {
	if (uri.indexOf('#') > 0) {
		return uri.substring(uri.lastIndexOf('#') + 1);
	} else if (uri.indexOf('/') > 0) {
		return uri.substring(uri.lastIndexOf('/') + 1);
	} else
		return uri;
}

function stripVersion(uri) {
	return uri.replace(/[0-9]+\.[0-9]+\.[0-9]+\//, "");
}

function encode(uri) {
	return uri.replace(/[^a-zA-Z0-9]+/g, "_");
}

function parse(qs, sep, eq) {
	sep = sep || "&";
	eq = eq || "=";
	for ( var obj = {}, i = 0, pieces = qs.split(sep), l = pieces.length, tuple; i < l; i++) {
		tuple = pieces[i].split(eq);
		if (tuple.length > 0) {
			obj[unescape(tuple.shift())] = unescape(tuple.join(eq));
		}
	}
	return obj;
};

function unescape(s) {
	return decodeURIComponent(s.replace(/\+/g, ' '));
};

YUI({
	gallery : 'gallery-2011.01.03-18-30'
})
		.use(
				'gallery-yui3treeview',
				'history',
				'tabview',
				'node',
				'datatable-scroll',
				'event',
				'datasource',
				'querystring-parse-simple',
				'autocomplete',
				'autocomplete-filters',
				'autocomplete-highlighters',
				function(Y) {
					var dataSource = new Y.DataSource.Get({
						source : webapp + "ajax/"
					});

					// A value (URI or Literal) to be rendered. URI optionally
					// has anchortext
					function renderValue(ref, value) {
						if (ref.startsWith("urn:uuid:")) {
							ref = ref.substring(9);
							if (value == undefined) {
								value = ref;
							}
							var kbname = Y.one("#dataset").get('value');
							return '<a href="search.html?index=' + kbname + '&subject=' + ref + '">' + value + '</a>';
						}

						if (value == undefined) {
							value = ref;
						}
						if (ref.startsWith("http:")) {
							return '<a href="' + ref + '">' + value + '</a>';
						}
						return value;
					}

					function renderResult(result, node) {
						var li = Y.Node.create("<li class=\"result\"></li>");

						function predSort(a, b) {
							return (fieldLongNames.indexOf(encode(a.predicate)) - fieldLongNames.indexOf(encode(b.predicate)));
						}
						result.relations.sort(predSort);

						// Group the relations by predicate
						var map = [];
						var types = [];

						if (result.hasOwnProperty("relations")) {
							for (relationIndex in result.relations) {
								var relation = result.relations[relationIndex];
								if (map[relation.predicate] == null) {
									map[relation.predicate] = [ relation ];
								} else {
									map[relation.predicate].push(relation);
								}
								if (relation.predicate == RDF_TYPE) {
									types.push(stripVersion(relation.object));
								}
							}
						}
						function typeSort(a, b) {
							return stats.classes[b].count - stats.classes[a].count;
						}
						types.sort(typeSort);

						if (result.hasOwnProperty("label") && result.label != null) {
							li.append('<span class="label">' + result.label + '</span>');
						}
						li.append("<br/>");
						for (type in types) {
							li.append('&nbsp;<a class="type" href="' + types[type] + '">' + getLocalName(types[type]) + '</a>&nbsp;/');
						}
						var span = Y.Node.create('<span class="id"><a href=' + result.subject + '>' + result.subject + '</a></span>');
						if (result.subjectId != undefined) {
							var sourceLink = Y.Node.create('<a href="#">&nbsp;-&gt;</a>');
							var closure = closureCall(getDocumentByIdOrSubject, result.subjectId);
							sourceLink.on('click', closure);
							span.appendChild(sourceLink);
						}
						li.append(span);

						var table = Y.Node
								.create('<table class=\"result\"><col class=\"predicate-col\"/><col class=\"value-col\"/><th class="result-header">Property</th><th class=\"result-header\">Value</th></table>');
						var i = 0;
						for (var predicate in map) {
							if (predicate == RDF_TYPE)
								continue;
							
							var tdPredicate = Y.Node.create('<td class="predicate">' + getLocalName(predicate) + '</td>');
							
							var tdValues = Y.Node.create('<td></td>');
							for ( var relationIndex in map[predicate]) {
								var item = map[predicate][relationIndex];
								var providedName = getProviderName(item.context[0]);
								var div = Y.Node.create('<div title="' + providedName + '" class="source-' + providedName + '"></div>');
								div.appendChild(renderValue(item.object, item.label));
								if (item.subjectIdOfObject != undefined) {
									var sourceLink = Y.Node.create('<a href="#">&nbsp;-&gt;</a>');
									var closure = closureCall(getDocumentByIdOrSubject, item.subjectIdOfObject);
									sourceLink.on('click', closure);
									div.appendChild(sourceLink);
								}
								tdValues.appendChild(div);
							}
							
							var tr;
							if (i++ % 2 == 0) {
								tr = Y.Node.create('<tr class="even"></tr>');
							} else {
								tr = Y.Node.create('<tr class="odd"></tr>');
							}
							tr.appendChild(tdPredicate);
							tr.appendChild(tdValues);
							table.appendChild(tr);
						}

						li.append(table);
						node.append(li);

					}
					
					function closureCall(func, param) {
						var f = func;
						var p = param;
						return function () {
							f(p);
						}
					}

					function executeUnifiedSearch(e) {
						var query = Y.one("#ac-input").get('value');
						loadResults(query);
					}

					function executeSearchByClass(e) {
						var query = '';
						var params = Y.all(".class-property").each(function(thisNode) {
							if (thisNode.get('value') != '') {
								query = query + ' ';
								query = query + ns + thisNode.get('name') + ':' + thisNode.get('value');
							}
						});
						loadResults(query);
					}

					function executeSearchByID(e) {
						loadResults("subject=urn:uuid:" + Y.one('#id-text').get('value'));
					}
					
					function getDocumentByIdOrSubject(idOrSubject) {
						loadResults(idOrSubject, "DOCUMENT");
					}

					function loadResults(query, type) {
						type = typeof type !== 'undefined' ? type : "YAHOO";
						
						Y.one("#result-loader").show();

						dataSource.sendRequest({
							request : 'query?index=' + Y.one("#dataset").get('value') + '&query=' + query + '&pageSize=' + Y.one("#numresults").get('value')
									+ '&deref=' + Y.one("#dereference").get('checked') + "&type=" + type,
							callback : {
								success : function(e) {
									var result = e.response.results[0];
									
									Y.one("#result-loader").hide();
									Y.one("#resultContainer").show();
									Y.one("#result-stats").setContent("Found " + result.numResults + " results in " + result.time + " ms.");

									var ol = Y.Node.create("<ol></ol>");

									var markers = [];
									for ( var i in result.resultItems) {
										renderResult(result.resultItems[i], ol);
									}

									Y.one("#results").setContent("").append(ol);
								},
								failure : function(e) {
									alert(e.error.message);
								}
							}
						});
					}

					function changeProperties(clazz) {
						Y.one('#class-properties').setContent('');
						var frag = '<table>';
						for ( var i in stats.classes[clazz].properties) {
							frag = frag + '<tr><td>' + getLocalName(stats.classes[clazz].properties[i])
									+ '</td><td><input class="class-property yui3-hastooltip" type="text" title="' + /* TODO */'" name="'
									+ getLocalName(stats.classes[clazz].properties[i]) + '"/></td></tr>';
						}
						frag = frag + '</table>';
						Y.one('#class-properties').append(Y.Node.create(frag)).show();
					}

					function initDataSet() {

						fieldShortNames = [ "any" ];
						fieldLongNames = [ "any" ];
						Y.one("#results").setContent("");
						Y.one("#result-stats").setContent("");
						Y.one("#class-select").setContent("");
						Y.one("#resultContainer").hide();

						Y.one("#statistics-tree").setContent("");
						Y.one("#statistics-loader").show();

						dataSource.sendRequest({
							request : 'indexStatistics?index=' + Y.one("#dataset").get('value'),
							callback : {
								success : function(e) {
									stats = e.response.results[0];

									Y.one("#class-loader").hide();
									Y.one("#class-message").setContent("");

									Y.one("#statistics-loader").hide();

									var autocompletefields = [ 'OR' ];
									for ( var key in stats.fields) {
										fieldShortNames.push(key);
										fieldLongNames.push(stats.fields[key]);
										// fill up the autocomplete
										// suggestion box
										autocompletefields.push(key);
									}

									Y.one('#ac-input').plug(Y.Plugin.AutoComplete, {
										resultFilters : 'phraseMatch',
										queryDelimiter : ' ',
										tabSelect : 'true',
										typeAhead : 'true',
										source : autocompletefields
									});

									// Display class selection dropdown
									if (stats.classes.lenght > 0) {
										var frag = '<select>';
										var keys = [];
										for ( var key in stats.classes) {
											keys.push(key);
										}
										keys.sort();
										for (clazz in keys) {
											frag = frag + "<option value='" + keys[clazz] + "'>" + getLocalName(keys[clazz]) + "</option>";
										}

										frag = frag + '</select>';
										var fragNode = Y.Node.create(frag);
										Y.one('#class-select').append("I'm looking for a").append(fragNode).append("where");
										changeProperties(keys[0]);
										fragNode.on('change', function(e) {
											changeProperties(e.target.get('value'));
										});
									}

									var tree = [];
									function addClass(clazz) {
										var sub = [];
										var count = 0;
										if (stats.classes[clazz] != undefined) {
											count = stats.classes[clazz].count;
											for (uri in stats.classes[clazz].children) {
												var child = stats.classes[clazz].children[uri];
												if (stats.classes[child] != undefined)
													sub.push(addClass(child));
											}
										} else {
											// Class appears in the
											// ontology but no instances
											return [];
										}
										if (sub.length > 0) {
											return {
												label : getLocalName(clazz) + " (" + count + ") ",
												type : "TreeView",
												children : sub
											};
										} else {
											return {
												label : getLocalName(clazz) + " (" + count + ") "
											};
										}
									}

									// Render TreeView
									Y.one('#statistics-tree').setContent('<ul id="statisticsTreeList"></ul>');
									tree.push(addClass(ONTOLOGY_ROOT));
									var treeview = new Y.TreeView({
										srcNode : '#statisticsTreeList',
										contentBox : null,
										type : "TreeView",
										children : tree
									});

									treeview.render();
									// Expand the top level of the tree
									var statisticsTree = Y.one('#statistics-tree').removeClass('yui3-tree-collapsed');

									// Render statistics box
									/*
									 * Y.one("#statistics").setContent(''); var
									 * sidestats = [];a for (clazz in keys) {
									 * sidestats.push({"Entity" :
									 * getLocalName(keys[clazz]), "Count":
									 * stats.classes[keys[clazz]].count}); } var
									 * dtScrollingY = new Y.DataTable.Base({
									 * columnset : [{ key : "Entity", label :
									 * "Entity" }, { key : "Count", label :
									 * "Count" }], recordset : sidestats,
									 * summary : "Y axis scrolling table" });
									 * dtScrollingY.plug(Y.Plugin.DataTableScroll, {
									 * height : "400px" });
									 * dtScrollingY.render("#statistics");
									 */

									// On load, check if there were any
									// query parameters
									if (args['subject'] != undefined && args['subject'] != null) {
										var queryNode = Y.one("#id-text");
										queryNode.set('value', args['subject']);
										executeSearchByID();
									}

								},
								failure : function(e) {
									alert("Failed to preload fields : " + e.error.message);
								}
							}
						});
					}

					function initDataSetList(selectedDataSetName) {
						dataSource.sendRequest({
							request : 'dataSetList?',
							callback : {
								success : function(e) {
									dataSetNames = e.response.results;

									for ( var i in dataSetNames) {
										var dataSetName = dataSetNames[i];
										Y.one("#dataset").append('<option value=\"' + dataSetName + '\">' + dataSetName + '</option>');
										if ((selectedDataSetName == null && i == 0) || dataSetName == selectedDataSetName) {
											Y.one("#dataset").set('value', dataSetName);
											// Load the fields for the dataset
											initDataSet();
										}
									}
									Y.one('#dataset').on('change', initDataSet);
								},
								failure : function(e) {
									alert("Failed to load data set list : " + e.error.message);
								}
							}
						});
					}

					// Everything below is run automatically upon loading the
					// page

					initDataSetList(args['kb']);

					// Add handlers

					// On submit, retrieve and display search results
					Y.one('#search-by-class').on('submit', executeSearchByClass);

					// On submit, retrieve and display search results
					Y.one('#search-by-id').on('submit', executeSearchByID);

					// On submit, retrieve and display search results
					Y.one('#unifiedsearchform').on('submit', executeUnifiedSearch);

					// Toggle ontology search box
					Y.one('#toggleonto').on('click', function(e) {
						var node = Y.one('#onto-hider');
						var link = Y.one('#toggleonto');
						if (link.getContent() == "hide") {
							node.hide();
							link.setContent("show");
						} else {
							node.show();
							link.setContent("hide");
						}
					});

					var tabview = new Y.TabView({
						srcNode : '#resultContainer'
					});
					tabview.render();

				});
