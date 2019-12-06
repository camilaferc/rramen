var bounds = null;

function buildRouteList(routes){
	for ( var trans_type in routes) {
		//console.log(child);
		buildRouteListMode(trans_type, routes[trans_type]);
	}
}

function buildRouteListMode(trans_type, routes) {

	var list = document.getElementById('list_routes');

	var entry = document.createElement('li');
	var span = document.createElement('span');
	span.classList.add('caret');
	entry.appendChild(span);
	entry.appendChild(document.createTextNode(trans_type));
	var checkbox = document.createElement("INPUT");
	checkbox.setAttribute("type", "checkbox");
	checkbox.setAttribute("id", "c" + trans_type);
	checkbox.classList.add('checkbox_list_parent');
	checkbox.checked = true;
	entry.appendChild(checkbox);
	entry.setAttribute("id", "n" + trans_type);
	list.appendChild(entry);

	var entry_ul = document.createElement('ul');
	entry_ul.classList.add('nested');
	entry_ul.setAttribute("id", "ul_" + trans_type);
	entry_ul.style.width = "120px";
	//entry_ul.style.maxHeight = "70%";
	//entry_ul.style.overflow = "auto";
	entry_ul.style.padding = "30px";
	entry_ul.style.margin = "-10px";
	//entry_ul.style.float = "right";
	entry.appendChild(entry_ul);
	
	for ( var route in routes) {
		var entry_child = document.createElement('li');
		var image = document.createElement("img");
		image.src =  '../../static/images/map-blue2-16.png';
		image.style.cursor = "pointer";
		image.style.marginRight = "5px";
		//image.style.display="inline-block";
		image.style.verticalAlign = "-3px";
		image.setAttribute("id", "img_" + routes[route]);
		//image.style.height = "16px";
		//image.style.width = "auto";
		image.addEventListener("click", function() { showRoute(this.parentElement.id); })
		entry_child.appendChild(image);
		entry_child.appendChild(document.createTextNode(route));
		
		var checkbox = document.createElement("INPUT");
		checkbox.setAttribute("type", "checkbox");
		checkbox.setAttribute("id", routes[route]);
		checkbox.classList.add('checkbox_list');
		checkbox.checked = true;
		//checkbox.style.display="inline";
		//checkbox.style.overflow = "hidden";
		//checkbox.style.verticalAlign = "middle";
		//checkbox.style.marginTop = "5px";
		entry_child.appendChild(checkbox);
		entry_child.setAttribute("id", "l" + routes[route]);
		entry_child.style.overflow = "auto";
		entry_child.classList.add('list_child');
		
		entry_child.style.boxSizing = "border-box";
		entry_child.style.whiteSpace = "nowrap";
		entry_child.style.position = "relative";
		//entry_child.style.display="inline-block";
		entry_ul.appendChild(entry_child);
	}

}

function showRoute(route_id){
	var route_id = route_id.slice(1);
	console.log(route_id)
	if (!map.getLayer("route_" + route_id)) {
		var route_data = {"route_id": route_id};
		var color = getRandomColor();
		$.post("route", JSON.stringify(route_data) , function(res){
			console.log(res)
			route_geom = res["route_geom"]
			console.log(route_geom)
			addRouteLayer(route_id, route_geom, color);
			
			// Geographic coordinates of the LineString
			var coordinates = route_geom.features[0].geometry.coordinates;
			console.log(coordinates)
			
			if(!bounds){
				bounds = new mapboxgl.LngLatBounds(coordinates[0], coordinates[0]);
			}
			 
			// Pass the first coordinates in the LineString to `lngLatBounds` &
			// wrap each coordinate pair in `extend` to include them in the bounds
			// result. A variation of this technique could be applied to zooming
			// to the bounds of multiple Points or Polygon geomteries - it just
			// requires wrapping all the coordinates with the extend method.
			for(var i = 0; i < coordinates.length; i++){
				var llBound = new mapboxgl.LngLatBounds(coordinates[i], coordinates[i]);
				console.log(llBound);
				bounds.extend(llBound);
				console.log(bounds)
			}
			/*bounds = coordinates.reduce(function(bounds, coord) {
				return bounds.extend(coord);
			}, new mapboxgl.LngLatBounds(coordinates[0], coordinates[0]));*/
			 
			map.fitBounds(bounds, {
				padding: 20
			});
		});
		var parent = document.getElementById("l" + route_id);
		var checkbox = document.getElementById(route_id);
		var text = document.createElement("input");
    	//Assign different attributes to the element.
		text.setAttribute("type", "text");
		text.setAttribute("value", "");
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
		text.style.left = "55%";
		text.style.top = "30%";
		text.style.display = "inline-block"
		parent.insertBefore(text, checkbox);
		
		
	}else{
		alert("Route is already shown on the map.");
	}
}

function addRouteLayer(route_id, geom, color){
    console.log("Adding layer:" + route_id)
	map.addLayer({
        "id": "route_"+route_id,
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

function getRandomColor() {
	  var letters = '0123456789ABCDEF';
	  var color = '#';
	  for (var i = 0; i < 6; i++) {
	    color += letters[Math.floor(Math.random() * 16)];
	  }
	  return color;
}