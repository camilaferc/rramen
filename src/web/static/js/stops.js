function addStopsLayer(stop_locations){
	for(level in LEVEL_IMAGE){
    	var layer_name = LEVEL_IMAGE[level].split(".")[0]
    	map.addSource("stops-" + layer_name, {
    	    type: "geojson",
    	    data: stop_locations[level],
    	});
    	const zoom = 11 + parseInt(level, 10);
    	map.addLayer({
    	    "type": "symbol",
    	    "id": "stops-" + layer_name,
    	    "source": "stops-" + layer_name,
    	    "minzoom": zoom,
    	    "layout": {
    	        "icon-image": layer_name,
    	        "icon-offset": [0,-8],
    	        "text-field": "{name}",
    	        "text-anchor": "top",
    	        "text-size": {
    	            "stops": [
    	                [0, 0],
    	                [3, 0],
    	                [15, 0],
    	                [15.1, 10],
    	            ]
    	        }
    	    }
    	});
	}
	
}

function stopRouteSelection(button){
	if(view == 1){
		route_name_split = button.id.split("_")
	    route_name = route_name_split[1] + "_" + route_name_split[2] + "_" + route_name_split[3]
	    var list_checkbox = document.getElementById(route_name);
	    //console.log(list_checkbox)
	    if (button.classList.contains("button_unselected")){
	    	button.className = button.className.replace(" button_unselected", "");
	    	list_checkbox.checked = true;
	    }else{
	    	button.className += " button_unselected";
	    	list_checkbox.checked = false;
	    }
		/*console.log(checkbox.checked)
	    console.log(checkbox)
	    console.log(checkbox.id)
	    route_name_split = checkbox.id.split("_")
	    route_name = route_name_split[1] + "_" + route_name_split[2] + "_" + route_name_split[3]
	    console.log(route_name)
	    var list_checkbox = document.getElementById(route_name);
	    console.log(list_checkbox)
	    list_checkbox.checked = checkbox.checked;*/
	}else{
		console.log("Individual user!")
	}
    
}

