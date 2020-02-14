var bounds = new mapboxgl.LngLatBounds();
var selected_routes = []

function buildRouteList(routes, stops, transp_mapping){
	for ( var trans_id in routes) {
		//console.log(child);
		buildRouteListMode(trans_id, routes[trans_id], transp_mapping[trans_id], stops);
	}
	addEventsRoutes();
	addEventsStops();
}

function addEventsRoutes(){
    var toggler = document.getElementsByClassName("caret");
    var i;

    for (i = 0; i < toggler.length; i++) {
      //console.log(toggler[i])
      toggler[i].addEventListener("click", function() {
        this.parentElement.querySelector(".nested").classList.toggle("active_route");
        this.classList.toggle("caret-down");
      });
    }
    
    addEventRouteChildren();
    addEventRouteParent()
}


function addEventRouteParent(){
	const checkboxes = document.getElementsByClassName("checkbox_list_parent");
    //console.log(checkboxes);
    for (i = 0; i < checkboxes.length; i++) {
    	checkboxes[i].addEventListener("change", function() {
    		var route_id = this.id;
    		var route = route_id.split("_");
    		var transp_id = route[1];
        	if(this.checked) {
                console.log(this.id + " checked")
                markChildren(this.id, true);
            } else {
                // Checkbox is not checked..
            	console.log(this.id + " unchecked")
            	markChildren(this.id, false);
            }
        });
    }
}

function markChildren(parent_marker_id, marked){
	var parent_id = "n"+ parent_marker_id.slice(1)
	var children_list = document.getElementById(parent_id).getElementsByTagName("li");
	for (const child of children_list) {
		const child_id = child.id;
		var route = child_id.split("_");
		var route_name = route[1];
		var transp_id = route[2];
		//console.log(child_id)
		const child_checkbox = document.getElementById(route_name + "_" + transp_id);
		child_checkbox.checked = marked;
	}
}

function addEventsStops(){
	const checkboxes = document.getElementsByClassName("checkbox_stop_list");
    //console.log(checkboxes);
    for (i = 0; i < checkboxes.length; i++) {
    	checkboxes[i].addEventListener("change", function() {
    		var route_stop_id = this.id;
    		var route = route_stop_id.split("_");
    		var route_name = route[0];
    		var transp_id = route[1];
    		var parent_id = route_name + "_" + transp_id
    		console.log(parent_id)
        	if(this.checked) {
                console.log(this.id + " checked")
                var check_parent = document.getElementById(parent_id);
            	check_parent.checked = true
            } else {
                // Checkbox is not checked..
            	console.log(this.id + " unchecked")
            	checkStopParent(parent_id);
            }
    		var sr_checkbox = document.getElementById("sr_" + this.id);
    		if(sr_checkbox){
    			console.log("updating stop checkbox")
    			sr_checkbox.checked = this.checked;
    		}
    		
        });
    }
}



function addEventRouteChildren(){
	const checkboxes = document.getElementsByClassName("checkbox_list");
    //console.log(checkboxes);
    for (i = 0; i < checkboxes.length; i++) {
    	checkboxes[i].addEventListener("change", function() {
    		var route_id = this.id;
    		var route = route_id.split("_");
    		var transp_id = route[1];
        	if(this.checked) {
                console.log(this.id + " checked")
                checkParent(transp_id);
            } else {
                // Checkbox is not checked..
            	console.log(this.id + " unchecked")
            	var check_parent = document.getElementById("c" + transp_mapping[transp_id]);
            	check_parent.checked = false
            }
        });
    }
}

function checkParent(transp_id){
	console.log(transp_mapping[transp_id])
	var check_parent = document.getElementById("c" + transp_mapping[transp_id]);
	var list = document.getElementById("n" + transp_mapping[transp_id]).getElementsByTagName("li");
	allMarked = checkChildrenMarked(list)
	if (allMarked){
		check_parent.checked = true
	}
		
}

function checkStopParent(parent_id){
	console.log(parent_id)
	var check_parent = document.getElementById(parent_id);
	var list = document.getElementById("l_" + parent_id).getElementsByTagName("li");
	allUnmarked = checkStopsUnmarked(list)
	if (allUnmarked){
		check_parent.checked = false
	}
		
}

function checkStopsUnmarked(children_list){
	for (const child of children_list) {
		const child_id = child.id
		//console.log(child_id)
		var route = child_id.split("_");
		var route_name = route[1];
		var transp_id = route[2];
		var stop_id = route[3];
		const child_checkbox = document.getElementById(route_name + "_" + transp_id + "_" + stop_id);
		if(child_checkbox.checked){
			return false;
		}
	}
	return true;
}

function checkChildrenMarked(children_list){
	for (const child of children_list) {
		const child_id = child.id
		var route = child_id.split("_");
		var route_name = route[1];
		var transp_id = route[2];
		//console.log(child_id)
		const child_checkbox = document.getElementById(route_name + "_" + transp_id);
		if(!child_checkbox || !child_checkbox.checked){
			//console.log(child_id + " NOT marked")
			return false;
		}
	}
	return true;
}

function buildRouteListMode(transp_id, routes, transp_type, stops) {
	
	if (!document.getElementById("c" + transp_type)){
		var list = document.getElementById('list_routes');

		var entry = document.createElement('li');
		entry.style.overflow = "hidden";
		var span = document.createElement('span');
		span.classList.add('caret');
		entry.appendChild(span);
		entry.appendChild(document.createTextNode(transp_type));
		var checkbox = document.createElement("INPUT");
		checkbox.setAttribute("type", "checkbox");
		checkbox.setAttribute("id", "c" + transp_type);
		checkbox.classList.add('checkbox_list_parent');
		checkbox.checked = true;
		entry.appendChild(checkbox);
		entry.setAttribute("id", "n" + transp_type);
		list.appendChild(entry);

		var entry_ul = document.createElement('ul');
		entry_ul.classList.add('nested');
		entry_ul.setAttribute("id", "ul_" + transp_type);
		entry_ul.style.width = "230px";
		//entry_ul.style.maxHeight = "70%";
		//entry_ul.style.overflow = "auto";
		entry_ul.style.padding = "30px";
		entry_ul.style.margin = "-10px";
		//entry_ul.style.float = "right";
		entry.appendChild(entry_ul);
	}

	if(!entry_ul){
		console.log("List already exists")
		entry_ul = document.getElementById("ul_" + transp_type);
	}
	
	for ( var route in routes) {
		var entry_child = document.createElement('li');
		var span_child = document.createElement('span');
		span_child.classList.add('caret');
		entry_child.appendChild(span_child);
		var image = document.createElement("img");
		image.src =  '../../static/images/map-blue2-16.png';
		image.style.cursor = "pointer";
		image.style.marginRight = "5px";
		image.style.position = "absolute";
		image.style.left = "20px";
		image.style.verticalAlign = "-3px";
		image.setAttribute("id", "img_" + route + "_" + transp_id);
		image.addEventListener("click", function() { showRoute(this.parentElement.id); })
		entry_child.appendChild(image);
		
		var text = document.createElement("input");
		text.setAttribute("type", "text");
		text.setAttribute("value", route);
		text.setAttribute("class", "route-text");
		text.setAttribute("id", "text_" + route + "_" + transp_id);
		text.readOnly = "true";
		text.style.position = "absolute";
		text.style.left = "50px";
		entry_child.appendChild(text);
		
		var checkbox = document.createElement("INPUT");
		checkbox.setAttribute("type", "checkbox");
		checkbox.setAttribute("id", route + "_" + transp_id);
		checkbox.classList.add('checkbox_list');
		checkbox.checked = true;
		entry_child.appendChild(checkbox);
		entry_child.setAttribute("id", "l_" + route + "_" + transp_id);
		entry_child.style.overflow = "auto";
		entry_child.classList.add('list_child');
		
		entry_child.style.boxSizing = "border-box";
		entry_child.style.whiteSpace = "nowrap";
		entry_child.style.position = "relative";
		//entry_child.style.display="inline-block";
		entry_ul.appendChild(entry_child);
		
		var stops_route = stops[route + "_" + transp_id];
		var entry_ul_child = document.createElement('ul');
		entry_ul_child.classList.add('nested');
		entry_ul_child.setAttribute("id", "ul_" + route + "_" + transp_id);
		entry_ul_child.style.width = "185px";
		entry_ul_child.style.paddingLeft = "25px";
		entry_ul_child.style.paddingTop = "15px";
		entry_ul_child.style.paddingBottom = "15px";
		//entry_ul_child.style.marginLeft = "-20px";
		//entry_ul_child.style.marginTop = "-50px";
		//entry_ul_child.style.marginBottom = "-10px";
		entry_ul_child.style.overflow = "hidden";
		entry_child.appendChild(entry_ul_child);
		for ( var stop of stops_route) {
			//console.log(stop)
			var entry_stop = document.createElement('li');
			var new_name = formatStopName(stop[1])
			//console.log(stop + "-" +  new_name);
			entry_stop.appendChild(document.createTextNode(new_name));
			entry_stop.style.overflow = "hidden";
			var checkbox = document.createElement("INPUT");
			checkbox.setAttribute("type", "checkbox");
			checkbox.setAttribute("id", route + "_" + transp_id + "_" + stop[0]);
			checkbox.classList.add('checkbox_stop_list');
			checkbox.checked = true;
			checkbox.style.float = "right";
			//entry_stop.style.fontSize = "14px";
			entry_stop.appendChild(checkbox);
			entry_stop.setAttribute("id", "s_"+ route + "_" + transp_id + "_" + stop[0]);
			entry_ul_child.appendChild(entry_stop);
		}
		
	}

}

function showRoute(route_id){
	var route = route_id.split("_");
	var route_name = route[1];
	var transp_id = route[2]
	var img = document.getElementById("img_" + route_name + "_" + transp_id)
	var text = document.getElementById("text_" + route_name + "_" + transp_id);
	console.log(route_name, transp_id)
	if (!map.getLayer("route_" + route_name + "_" + transp_id)) {
		img.src = '../../static/images/map-blue3-16.png';
		text.style.fontWeight="bold";
		var route_data = {"route_name": route_name, "transp_id": transp_id};
		var color = getRandomColor();
		$.post("route", JSON.stringify(route_data) , function(res){
			console.log(res)
			route_geom = res["route_geom"]
			console.log(route_geom)
			addRouteLayer(route_name, transp_id, route_geom, color);
			
			// Pass the first coordinates in the LineString to `lngLatBounds` &
			// wrap each coordinate pair in `extend` to include them in the bounds
			// result. A variation of this technique could be applied to zooming
			// to the bounds of multiple Points or Polygon geomteries - it just
			// requires wrapping all the coordinates with the extend method.
			/*bounds = coordinates.reduce(function(bounds, coord) {
			return bounds.extend(coord);
			}, new mapboxgl.LngLatBounds(coordinates[0], coordinates[0]));*/
			route_geom.features.forEach(function(feature) {
				
				var coordinates = feature.geometry.coordinates;
				for(var i = 0; i < coordinates.length; i++){
					var llBound = new mapboxgl.LngLatBounds(coordinates[i], coordinates[i]);
					//console.log(llBound);
					bounds.extend(llBound);
				}
			});
			
			console.log(bounds)
			map.fitBounds(bounds, {
				padding: 20
			});
			
			 
		});
		var parent = document.getElementById("l_" + route_name + "_" + transp_id);
		
		/*var button = document.createElement("button");
		button.classList.add('button-close');
		button.appendChild(document.createTextNode('x'));
		button.style.float = "left";
		button.addEventListener("click", function() { clearRoute(this, this.parentElement.id); })
		//var image = document.getElementById("img_"+ route_name + "_" + transp_type);
		parent.appendChild(button);*/
		
		var checkbox = document.getElementById(route_name + "_" + transp_id);
		var text = document.createElement("input");
    	//Assign different attributes to the element.
		text.setAttribute("type", "text");
		text.setAttribute("value", "");
		text.setAttribute("id", "line_" + route_name + "_" + transp_id);
    	//element.setAttribute("class", "input-text");
		text.style.backgroundColor = color
		text.style.width = "20px"
		text.style.height = "5px"
		text.style.borderRadius = "6px";
		text.style.borderWidth = "0px";
		text.style.border = "none";
		text.style.whiteSpace = "normal";
		//text.style.margin = "auto"
		//text.style.marginRight = "10px"
		//text.style.marginTop = "15px";
		text.style.overflow = "hidden";
		//text.style.float = "right";
		
		text.style.position = "absolute";
		//text.style.right = "-10px";
		text.style.left = "65%";
		text.style.top = "5px";
		text.style.display = "inline-block"
		parent.insertBefore(text, checkbox);
		
		
	}else{
		//alert("Route is already shown on the map.");
		clearRoute(route_name + "_" + transp_id);
	}
}

function addRouteLayer(route_name, transp_id, geom, color){
    console.log("Adding layer:" + route_name + "_" + transp_id)
    console.log(selected_routes)
    selected_routes.push(route_name+"_" + transp_id)
	map.addLayer({
        "id": "route_"+route_name+"_" + transp_id,
        "type": "line",
        "source": {
            "type": "geojson",
            "data": geom
        },
        "layout": {
            "line-join": "round",
            "line-cap": "round"
        },
        'paint': {
        	"line-width": 6,
        	// color routes by type
        	// https://docs.mapbox.com/mapbox-gl-js/style-spec/#expressions-match
        	'line-color': color
        }
    });
}

function getRemovedRoutes(routes){
	var not_checked = document.querySelectorAll('input[class="checkbox_list"]:not(:checked)');
	var removed_routes = []               		
	for(var i = 0; i < not_checked.length; i+=1){
		if (typeof(routes) == 'undefined') {
			  console.log("routes is not defined!")
		}
   		var rid = not_checked[i].id
   		var route = rid.split("_");
   		var route_name = route[0];
   		var transp_id = route[1];
   		var route_map = routes[transp_id][route_name]
   		route_map.forEach(function(route_id) {
   			removed_routes.push(route_id)
		});
   	}
   	console.log(removed_routes)
   	return removed_routes
}

function getRemovedStops(routes, stops){
	var not_checked = document.querySelectorAll('input[class="checkbox_stop_list"]:not(:checked)');
	var removed_stops = {}             		
	for(var i = 0; i < not_checked.length; i+=1){
   		var rid = not_checked[i].id
   		console.log(rid)
   		var route = rid.split("_");
   		var route_name = route[0];
   		var transp_id = route[1];
   		var stop_id = route[2];
   		var route_list = routes[transp_id][route_name]
   		
   		if(stop_id in removed_stops){
   			list_stop_routes = removed_stops[stop_id]
   			Array.prototype.push.apply(list_stop_routes, route_list);
   			console.log(list_stop_routes)
   			
   		}else{
   			removed_stops[stop_id] = route_list
   			console.log(route_list)
   		}
   	}
   	console.log(removed_stops)
   	return removed_stops
}

function clearRoute(route_id){
	
	var route = route_id.split("_");
	var route_name = route[0];
	var transp_id = route[1]
	
	if (map.getLayer("route_" + route_name + "_" + transp_id)) {
		console.log("Clearing route:" + route_name + '_' + transp_id)
		map.removeLayer("route_"+ route_name + '_' + transp_id);
		map.removeSource("route_"+ route_name + '_' + transp_id);
		line = document.getElementById("line_" + route_name + "_" + transp_id)
		line.parentNode.removeChild(line);
	}
	var img = document.getElementById("img_" + route_name + "_" + transp_id)
	var text = document.getElementById("text_" + route_name + "_" + transp_id);
	img.src = '../../static/images/map-blue2-16.png';
	text.style.fontWeight="normal";
	
}

function formatStopName(stop_name){
	var new_name = stop_name
	var pos = stop_name.indexOf("(")
	if(pos != -1){
		var pos2 = stop_name.indexOf(")")
		var len = pos2-pos + 1
		sub_string = stop_name.substr(pos, len)
		new_name = stop_name.replace(sub_string, "")
	}
	/*var pos_comma = stop_name.indexOf(",")
	if(pos_comma != -1){
		new_name = new_name.substr(pos_comma+1)
	}*/
	if (new_name.length > 20){
		new_name = stop_name.slice(0, 20)
		new_name += "."
	}
	return new_name
}

function getRandomColor() {
	  var letters = '0123456789ABCDEF';
	  var color = '#';
	  for (var i = 0; i < 6; i++) {
	    color += letters[Math.floor(Math.random() * 16)];
	  }
	  return color;
}

function clearSelectedRoutes(){
	$(".caret-down").removeClass("caret-down");
	$(".active_route").removeClass("active_route");

	list = document.getElementById ('list_routes');
    var checkboxes = list.querySelectorAll ('input[type=checkbox]:not(:checked)');
    for (checkbox of checkboxes){
    	checkbox.checked = true;
    }
    
    for (route of selected_routes){
    	clearRoute(route);
    }
    selected_routes = []
}