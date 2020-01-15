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