function addStopsLayer(stop_locations){
	map.addSource("stops", {
	    type: "geojson",
	    data: stop_locations,
	});

	map.addLayer({
	    "type": "symbol",
	    "id": "stops",
	    "source": "stops",
	    "minzoom": 14,
	    "layout": {
	        "icon-image": "bus-icon",
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

function stopRouteSelection(checkbox){
    console.log(checkbox.checked)
    console.log(checkbox)
    console.log(checkbox.id)
    route_name_split = checkbox.id.split("_")
    route_name = route_name_split[1] + "_" + route_name_split[2] + "_" + route_name_split[3]
    console.log(route_name)
    var list_checkbox = document.getElementById(route_name);
    console.log(list_checkbox)
    list_checkbox.checked = checkbox.checked;
}

