var draw = null;
var polygon_source_coords = null;
var polygon_target_coords = null;

const STYLES_DRAW = [ {
	"id" : "gl-draw-polygon-fill",
	"type" : "fill",
	"filter" : [ "all", [ "==", "$type", "Polygon" ],
			[ "!=", "mode", "static" ] ],
	"paint" : {
		"fill-color" : "#FFD300",
		"fill-outline-color" : "#FDA50F",
		"fill-opacity" : 0.2
	}
} ]

function addPolygonLocation(location, map) {
	currentPolygon = location
	if (location == "source" && polygon_source_coords != null) {
		alert("Source polygon has already been drawn!")
		return

	}
	if (location == "target" && polygon_target_coords != null) {
		alert("Target polygon has already been drawn!")
		return

	}
	if (!draw) {
		draw = new MapboxDraw({
			displayControlsDefault : false,
			controls : {}
		});
		map.addControl(draw);

		draw.changeMode('draw_polygon');

		map.on('draw.create', function (e) {
			  updateRegion(currentPolygon);
			});
		map.on('draw.update', function (e) {
			  updateRegion(currentPolygon);
			});
		map.on('draw.delete', function (e) {
			deleteRegion();
		});
		//map.on('draw.create', updateArea('draw.create', polygon_source_coords, polygon_target_coords, currentPolygon, draw));
		//map.on('draw.delete', updateArea('draw.delete', polygon_source_coords, polygon_target_coords, currentPolygon, draw));
		//map.on('draw.update', updateArea('draw.update', polygon_source_coords, polygon_target_coords, currentPolygon, draw));
	} else {
		draw.changeMode('draw_polygon');
	}

}

function updateRegion(currentPolygon) {
	console.log("Updating region")
	var data = draw.getAll();
	console.log(data);
	console.log(data.features);
	var len = data.features.length;
	if (currentPolygon == "source") {
		polygon_source_coords = data.features[len - 1].geometry.coordinates;
		console.log(polygon_source_coords);
		if (document.getElementById('inputSource').value == "") {
			document.getElementById('inputSource').value = "Polygon drawn"
		}
	} else {
		polygon_target_coords = data.features[len - 1].geometry.coordinates;
		console.log(polygon_target_coords);
		if (document.getElementById('inputTarget').value == "") {
			document.getElementById('inputTarget').value = "Polygon drawn"
		}
	}
}

function deleteRegion(){
	polygon_source_coords = null;
	polygon_target_coords = null;
}

function updateArea(type, polygon_source_coords, polygon_target_coords, currentPolygon, draw) {
	console.log(type)
	if (type == 'draw.create' || type == 'draw.update') {
		console.log("Updating source")
		var data = draw.getAll();
		console.log(data);
		console.log(data.features);
		//console.log(data.features[0].geometry.coordinates);
		var len = data.features.length;
		if (currentPolygon == "source") {
			polygon_source_coords = data.features[len - 1].geometry.coordinates;
			console.log(polygon_source_coords);
			if (document.getElementById('inputSource').value == "") {
				document.getElementById('inputSource').value = "Polygon drawn"
			}
		} else {
			polygon_target_coords = data.features[len - 1].geometry.coordinates;
			console.log(polygon_target_coords);
			if (document.getElementById('inputTarget').value == "") {
				document.getElementById('inputTarget').value = "Polygon drawn"
			}
		}

	} else if (type == 'draw.delete') {
		polygon_source_coords = null;
		polygon_target_coords = null;
	}

}

function updateAreaTarget(e) {
	if (e.type == 'draw.create' || e.type == 'draw.update') {
		console.log("Updating target")
		var data = draw_target.getAll();
		console.log(data);
		console.log(data.features);
		//console.log(data.features[0].geometry.coordinates);
		polygon_target_coords = data.features[0].geometry.coordinates;
		console.log(polygon_target_coords);
	} else if (e.type == 'draw.delete') {
		polygon_target_coords = null;
	}

}