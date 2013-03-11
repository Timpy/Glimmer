YUI.add('yui-pager', function (Y){
	
	function Pager (config) {
		Pager.superclass.constructor.apply(this, arguments);
	}
	
	Pager.Name = "Pager";
	
	var state = {
		callback: undefined,
		pageSize: 10,
		pages: 0,
		page: 1,
		windowSize: 10,
		anchorClass: "pager-link",
		anchorClassCurrent: "pager-link-current",
		first: "&lt;&lt;first",
		previous: "&lt;previous",
		next: "next&gt;",
		last: "last&gt;&gt;",
		elementIds: undefined
	}
	
	function appendElement(element, innerHtml, targetPage, current) {
		var anchor = Y.Node.create("<a href='#'></a>");
		anchor.addClass(state.anchorClass);
		anchor.appendChild(innerHtml);
		if (current) {
			anchor.addClass(state.anchorClassCurrent)
		} else {
			anchor.on('click', function (e) {
				e.preventDefault();
				
				if (state.callback != undefined) {
					state.callback(targetPage, state);
				} else {
					alert(targetPage);
					this.setState({
						page: targetPage
					});
				} 
			});
		}
		
		element.appendChild(anchor);
	}
	
	function getPagesInWindow() {
		var a = new Array();
		a.push(state.page);
		
		var i = 1;
		while (i < state.windowSize/2) {
			if (state.page + i <= state.pages) {
				a.push(state.page + i);
			}
			if (state.page - i >= 1) {
				a.unshift(state.page - i);
			}
			i++;
		}
		i = state.windowSize
		while (i < state.pages) {
			if (state.page + i <= state.pages) {
				a.push(state.page + i);
			}
			if (state.page - i >= 1) {
				a.unshift(state.page - i);
			}
			i = i * 2;
		}
		
		return a;
	}
	
	Y.extend(Pager, Y.Base, {
		initializer: function(config) { },
		destructor: function() { },
		
		setCallback: function(newCallback) {
			state.callback = newCallback;
		},
		
		setState: function(newState) {
			var changed = false;
			for (var key in state) {
				if (newState.hasOwnProperty(key)) {
					if (state[key] != newState[key]) {
						state[key] = newState[key];
						changed = true;
					}
				}
			}
			if (changed && state.elementIds != undefined) {
				for (var i in state.elementIds) {
					this.render(state.elementIds[i]);
				}
			}
		},
		
		getState: function() {
			return state;
		},
		
		render: function(elementId) {
			var rootElement = Y.one(elementId);
			rootElement.setContent("");
			if (state.pages > 0) {
				if (state.page > 1) {
					appendElement(rootElement, state.first, 1);
					appendElement(rootElement, state.previous, state.page - 1);
				}
				var pagesToShow = getPagesInWindow();
				for (var i in pagesToShow) {
					var p = pagesToShow[i];
					appendElement(rootElement, p, p, p == state.page);
				}
				
				if (state.page < state.pages) {
					appendElement(rootElement, state.next, state.page + 1);
					appendElement(rootElement, state.last, state.pages);
				}
			}
		}
	});
	
	Y.Pager = Pager;
}, '0.0.1', { requires:['node']});