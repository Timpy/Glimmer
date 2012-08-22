String.prototype.startsWith = function(str) {
	return (this.match("^" + str) == str)
}

var stats = {};
var fields = [ "any" ];
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

function removeNameSpace(sstring) {
	if (sstring === "any")
		return "any";
	return sstring.substr(sstring.toString().lastIndexOf("_") + 1, sstring
			.toString().length)
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

					// Add one more parameter
					function addMoreField() {

						var frag = '<div class="param"><input class="query" type="text" name="query"/><select class="field">';
						for (field in fields) {
							frag = frag + "<option value='" + fields[field]
									+ "'>" + removeNameSpace(fields[field])
									+ "</option>";
						}
						frag = frag + '</select></div>';
						Y.one('#params').append(Y.Node.create(frag));
					}
					;

					// A value (URI or Literal) to be rendered. URI optionally
					// has anchortext
					function renderValue(value, anchor) {

						if (value.startsWith("urn:uuid:")) {
							var label = value.substring(9);
							if (anchor != undefined && anchor != null)
								label = anchor;
							if (typeof (pubby) != "undefined" && pubby != null) {
								return '<span class="id"><a href="' + pubby
										+ value.substring(9) + '">' + label
										+ '</a></span>';
							} else {
								var kbname = Y.one("#dataset").get('value');
								return '<span class="id"><a href="search.html?kb='
										+ kbname
										+ '&subject='
										+ value.substring(9)
										+ '">'
										+ label
										+ '</a></span>';
							}
						} else {
							return '<span class="id">' + value + '</span>';
						}
					}

					// SingleLocationBusiness specific
					function getMarker(result) {
						var lat, lon;
						for (qid in result.quads) {
							if (getLocalName(result.quads[qid].triple.predicate) === "latitude") {
								lat = result.quads[qid].triple.object;
							} else if (getLocalName(result.quads[qid].triple.predicate) === "longitude") {
								lon = result.quads[qid].triple.object;
							}
						}
						return ({
							"id" : result.title,
							"lat" : lat,
							"lon" : lon
						});
					}

					function renderResult(result, node) {

						var li = Y.Node.create("<li class=\"result\"></li>");
						var title = result.uri;
						if (title == null)
							title = result.title;
						// Sort the quads

						function predSort(a, b) {
							return (fields.indexOf(encode(a.triple.predicate)) - fields
									.indexOf(encode(b.triple.predicate)));
						}
						result.quads.sort(predSort);

						// Group the quads by predicate
						var map = [];
						var labels = [];
						var types = [];

						if (result.hasOwnProperty("quads")) {
							for (qid in result.quads) {
								if (map[result.quads[qid].triple.predicate] == null) {
									map[result.quads[qid].triple.predicate] = [ result.quads[qid] ];
								} else {
									map[result.quads[qid].triple.predicate]
											.push(result.quads[qid]);
								}
								if (result.quads[qid].hasOwnProperty("label")
										&& result.quads[qid].label != null) {
									labels[result.quads[qid].triple.object] = result.quads[qid].label;
								}
								if (result.quads[qid].triple.predicate == RDF_TYPE) {
									types
											.push(stripVersion(result.quads[qid].triple.object));
								}
							}
						}
						function typeSort(a, b) {
							return stats.classes[b].count
									- stats.classes[a].count;
						}
						types.sort(typeSort);

						if (result.hasOwnProperty("label")
								&& result.label != null) {
							li.append('<span class="label">'
									+ renderValue(result.label) + '</span>');
						}
						li.append("<br/>");
						for (type in types) {
							li
									.append('&nbsp;<a class="type" href="'
											+ types[type] + '">'
											+ getLocalName(types[type])
											+ '</a>&nbsp;/');
						}
						li.append('<span class="id">' + renderValue(title)
								+ '</span>');

						var table = Y.Node
								.create('<table class=\"result\"><col class=\"predicate-col\"/><col class=\"value-col\"/><th class="result-header">Property</th><th class=\"result-header\">Value</th></table>');
						var i = 0;
						for (pred in map) {
							// alert(Object.getOwnPropertyNames(result.quads[qid]));
							if (pred == RDF_TYPE)
								continue;
							var row = "";
							if (i++ % 2 == 0) {
								row = row + '<tr class="even">';
							} else {
								row = row + '<tr class="odd">';
							}
							row = row + "<td class=\"predicate\">"
									+ getLocalName(pred) + "</td>" + "<td>";
							for (qid in map[pred]) {
								if (labels[map[pred][qid].triple.object] != null) {
									row = row
											+ '<span title="'
											+ getProviderName(map[pred][qid].source[0])
											+ '" class="source-'
											+ getProviderName(map[pred][qid].source[0])
											+ '">'
											+ renderValue(
													map[pred][qid].triple.object,
													labels[map[pred][qid].triple.object])
											+ '</span><br/>';
								} else {
									row = row
											+ '<span title="'
											+ getProviderName(map[pred][qid].source[0])
											+ '" class="source-'
											+ getProviderName(map[pred][qid].source[0])
											+ '">'
											+ renderValue(map[pred][qid].triple.object)
											+ '</span><br/>';
								}
							}
							row = row + '</td></tr>';
							table.append(row);
						}

						li.append(table);
						node.append(li);

					}

					function executeUnifiedSearch(e) {

						// FIXME need handling for UUID and RDFTYPES (currently
						// I assume all searches are on WOO attributes)

						var query = Y.one("#ac-input").get('value');

						var re2 = /(\w+:)/g;
						query = query.replace(re2, '' + ns + '$1');
						query = query
								.replace(
										'http_woo_corp_yahoo_com_1_7_1_ns_type:',
										'http_www_w3_org_1999_02_22_rdf_syntax_ns_type:');

						loadResults(query);
					}

					function executeSearchByField(e) {
						var query = '';

						var params = Y
								.all(".param")
								.each(
										function(thisNode, index) {
											query = query + ' ';
											field = thisNode.one(".field").get(
													'value');
											if (!Y.one("#match-mode").get(
													'checked')
													&& index > 0)
												query = query + 'OR';
											if (field != "any")
												query = query + field + ':';
											query = query
													+ '('
													+ thisNode.one(".query")
															.get('value') + ')';

										});

						if (field === "uuid")
							loadResults("subject=urn:uuid:" + uuid);
						else
							loadResults(query);
					}

					function executeSearchByClass(e) {
						var query = '';
						var params = Y.all(".class-property").each(
								function(thisNode) {
									if (thisNode.get('value') != '') {
										query = query + ' ';
										query = query + ns
												+ thisNode.get('name') + ':'
												+ thisNode.get('value');
									}
								});
						loadResults(query);
					}

					function executeSearchByID(e) {
						loadResults("subject=urn:uuid:"
								+ Y.one('#id-text').get('value'));
					}

					function loadResults(query) {
						Y.one("#result-loader").show();

						dataSource
								.sendRequest({
									request : 'query?index=' + Y.one("#dataset").get('value')
											+ '&query=' + query
											+ '&pageSize=' + Y.one("#numresults").get('value')
											+ '&deref='	+ Y.one("#dereference").get('checked'),
									callback : {

										success : function(e) {
											Y.one("#result-loader").hide();
											Y.one("#resultContainer").show();
											Y.one("#result-stats").setContent(
													"Rendering...");

											var ol = Y.Node.create("<ol></ol>");

											var results = e.response.results[0].resultItems;
											var markers = [];
											for (result in results) {
												renderResult(results[result],
														ol);

												var marker = getMarker(results[result]);
												if (marker != null) {
													marker["label"] = result; // label
													// the
													// marker
													// with
													// index
													markers.push(marker);
												}
											}

											Y.one("#results").setContent("")
													.append(ol);
											Y
													.one("#result-stats")
													.setContent(
															"Found "
																	+ e.response.results[0].numResults
																	+ " results in "
																	+ e.response.results[0].time
																	+ " ms.");

											// render map if anything has
											// coordinates
											if (markers.length > 0) {
												showMap('map', markers);
												Y.one("#result-map-tab").show();
											} else {
												// TODO: fix me... this doesn't
												// hide the tab
												Y.one("#result-map-tab")
														.setContent("");
											}

										},
										failure : function(e) {
											alert(e.error.message);
										}
									}
								});

					}
					;

					function changeProperties(clazz) {
						Y.one('#class-properties').setContent('');
						var frag = '<table>';
						for (i in stats.classes[clazz].properties) {
							frag = frag
									+ '<tr><td>'
									+ getLocalName(stats.classes[clazz].properties[i])
									+ '</td><td><input class="class-property yui3-hastooltip" type="text" title="'
									+ /* TODO */'" name="'
									+ getLocalName(stats.classes[clazz].properties[i])
									+ '"/></td></tr>';
						}
						frag = frag + '</table>';
						Y.one('#class-properties').append(Y.Node.create(frag))
								.show();
					}

					function initDataSet() {

						fields = [ "any", "uuid" ];
						Y.one("#params").setContent("");
						Y.one("#results").setContent("");
						Y.one("#result-stats").setContent("");
						Y.one("#class-select").setContent("");
						Y.one("#resultContainer").hide();

						Y.one("#statistics-tree").setContent("");
						Y.one("#statistics-loader").show();

						dataSource
								.sendRequest({
									request : 'indexStatistics?index=' + Y.one("#dataset").get('value'),
									callback : {
										success : function(e) {
											stats = e.response.results[0];

											Y.one("#field-loader").hide();
											Y.one("#field-message").setContent(
													"");

											Y.one("#class-loader").hide();
											Y.one("#class-message").setContent(
													"");

											Y.one("#statistics-loader").hide();

											var autocompletefields = [ 'OR' ];
											for (field in stats.fields) {
												fields
														.push(stats.fields[field]);
												// fill up the autocomplete
												// suggestion box
												autocompletefields
														.push(removeNameSpace(stats.fields[field]));
											}

											Y
													.one('#ac-input')
													.plug(
															Y.Plugin.AutoComplete,
															{
																resultFilters : 'phraseMatch',
																queryDelimiter : ' ',
																tabSelect : 'true',
																typeAhead : 'true',
																source : autocompletefields
															});

											// Add the first field
											addMoreField();

											// Display class selection dropdown
											if (stats.classes.lenght > 0) {
												var frag = '<select>';
												var keys = [];
												for ( var key in stats.classes) {
													keys.push(key);
												}
												keys.sort();
												for (clazz in keys) {
													frag = frag
														+ "<option value='"
														+ keys[clazz]
														+ "'>"
														+ getLocalName(keys[clazz])
														+ "</option>";
												}

												frag = frag + '</select>';
												var fragNode = Y.Node.create(frag);
												Y.one('#class-select').append(
													"I'm looking for a")
													.append(fragNode).append(
															"where");
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
															sub
																	.push(addClass(child));
													}
												} else {
													// Class appears in the
													// ontology but no instances
													return [];
												}
												if (sub.length > 0) {
													return {
														label : getLocalName(clazz)
																+ " ("
																+ count
																+ ") ",
														type : "TreeView",
														children : sub
													};
												} else {
													return {
														label : getLocalName(clazz)
																+ " ("
																+ count
																+ ") "
													};
												}
											}

											// Render TreeView
											Y
													.one('#statistics-tree')
													.setContent(
															'<ul id="statisticsTreeList"></ul>');
											tree.push(addClass(ONTOLOGY_ROOT));
											var treeview = new Y.TreeView(
													{
														srcNode : '#statisticsTreeList',
														contentBox : null,
														type : "TreeView",
														children : tree
													});

											treeview.render();
											// Expand the top level of the tree
											Y
													.one(
															'#statistics-tree .yui3-tree-collapsed')
													.toggleClass(
															'yui3-tree-collapsed');

											// Render statistics box
											/*
											 * Y.one("#statistics").setContent('');
											 * var sidestats = [];a for (clazz
											 * in keys) {
											 * sidestats.push({"Entity" :
											 * getLocalName(keys[clazz]),
											 * "Count":
											 * stats.classes[keys[clazz]].count}); }
											 * var dtScrollingY = new
											 * Y.DataTable.Base({ columnset : [{
											 * key : "Entity", label : "Entity" }, {
											 * key : "Count", label : "Count"
											 * }], recordset : sidestats,
											 * summary : "Y axis scrolling
											 * table" });
											 * dtScrollingY.plug(Y.Plugin.DataTableScroll, {
											 * height : "400px" });
											 * dtScrollingY.render("#statistics");
											 */

											// On load, check if there were any
											// query parameters
											if (args['q'] != undefined
													&& args['q'] != null) {
												var queryNode = Y.one(".query");
												queryNode.set('value',
														args['q']);
												executeSearchByField();
											} else if (args['subject'] != undefined
													&& args['subject'] != null) {
												var queryNode = Y
														.one("#id-text");
												queryNode.set('value',
														args['subject']);
												executeSearchByID();
											}

										},
										failure : function(e) {
											alert("Failed to preload fields : "
													+ e.error.message);
											addMoreField();
										}
									}
								});
					}

					function showMap(mapid, markers) {

						Y.one('#' + mapid).setContent('');

						if (mapid == undefined || markers == undefined
								|| markers.length == 0
								|| markers[0] == undefined)
							return;
						var myOptions = {
							center : [ parseFloat(markers[0].lat),
									parseFloat(markers[0].lon) ],
							mapTypeId : 0,
							zoom : 4
						};

						// create the map
						var mapContainer = document.getElementById(mapid);
						var bubbleContainer = new ovi.mapsapi.map.component.InfoBubbles();
						var components = [
								new ovi.mapsapi.search.component.RightClick(),
								new ovi.mapsapi.map.component.Behavior(),
								new ovi.mapsapi.map.component.TypeSelector(),
								new ovi.mapsapi.map.component.ZoomBar(),
								new ovi.mapsapi.map.component.ScaleBar(),
								new ovi.mapsapi.map.component.Overview(),
								new ovi.mapsapi.map.component.ViewControl(),
								new ovi.mapsapi.map.component.RightClick(),
								bubbleContainer ];

						if (ovi.mapsapi.map && ovi.mapsapi.map.Display) {
							var map = (window.display = new ovi.mapsapi.map.Display(
									mapContainer, {
										components : components,
										zoomLevel : myOptions['zoom'],
										fading : 250, // fading duration of
										// tiles in miliseconds
										center : myOptions['center']
									}));
						}

						var titleBubble = [];

						for ( var i = 0; i < markers.length; i++) {
							// alert(markers[i].label +" " + markers[i].id + " "
							// + markers[i].lat + markers[i].lon);
							var gp = new ovi.mapsapi.geo.Coordinate(
									parseFloat(markers[i].lat),
									parseFloat(markers[i].lon));
							var marker = new ovi.mapsapi.map.StandardMarker(gp,
									{
										text : markers[i].label
									});

							marker.addListener("mouseover", function() {
								titleBubble[i] = bubbleContainer.addBubble(
										markers[i].label, gp);
							});
							marker.addListener("mouseleave", function() {
								bubbleContainer.removeBubble(titleBubble[i]);
							});

							marker.addListener("click", function() {
								Y.one('#' + mapid).setContent('');
								loadResults("subject=" + markers[i].id);
							});

							map.objects.add(marker);
						}
					}
					
					function initDataSetList(selectedDataSetName) {
						dataSource
						.sendRequest({
							request : 'dataSetList?',
							callback : {
								success : function(e) {
									dataSetNames = e.response.results;
									
									for (var i in dataSetNames) {
										var dataSetName = dataSetNames[i];
										Y.one("#dataset").append(
												'<option value=\"' + dataSetName + '\">'
												+ dataSetName + '</option>');
										if ((selectedDataSetName == null && i == 0) || dataSetName == selectedDataSetName) {
											Y.one("#dataset").set('value', dataSetName);
											// Load the fields for the dataset
											initDataSet();
										}
									}
									Y.one('#dataset').on('change', initDataSet);
								},
								failure : function(e) {
									alert("Failed to load data set list : "	+ e.error.message);
									addMoreField();
								}
							}
						});
					}

					// Everything below is run automatically upon loading the
					// page

					initDataSetList(args['kb']);


					// Add handlers

					// Add more fields when clicking Add more
					Y.one('#add').on('click', addMoreField);

					// On submit, retrieve and display search results
					Y.one('#search-by-field')
							.on('submit', executeSearchByField);

					// On submit, retrieve and display search results
					Y.one('#search-by-class')
							.on('submit', executeSearchByClass);

					// On submit, retrieve and display search results
					Y.one('#search-by-id').on('submit', executeSearchByID);

					// On submit, retrieve and display search results
					Y.one('#unifiedsearchform').on('submit',
							executeUnifiedSearch);

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

					// Toggle field search box
					Y.one('#togglefields').on('click', function(e) {
						var node = Y.one('#field-hider');
						var link = Y.one('#togglefields');
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
