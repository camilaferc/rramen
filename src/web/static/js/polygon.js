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
	if (location == "source") {
		alert("Region as source currently not supported.")
		return

	}
	if (location == "target" && it > 0 && Object.keys(targets).length > 0) {
		alert("Mixed destination types not supported (point already selected).")
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
	} else {
		draw.changeMode('draw_polygon');
	}

}

function removePolygonTarget(){
	deleteRegion();
	if (draw){
		var data = draw.getAll();
		if (data.features[0]){
			var first_id = data.features[0].id
			draw.delete(first_id)
		}
    }
}

function updateRegion(currentPolygon) {
	var data = draw.getAll();
	var len = data.features.length;
	if (currentPolygon == "source") {
		polygon_source_coords = data.features[len - 1].geometry.coordinates;
		//console.log(polygon_source_coords);
		if (document.getElementById('sourceNode' + is).value == "") {
			document.getElementById('sourceNode' + is).value = "Polygon drawn"
		}
	} else {
		if (polygon_target_coords != null){
			removePolygonTarget()
		}
		var targetNode = document.getElementById('targetNode'+it)
		removeMarkerTarget(targetNode)
		removeNeighborhoodTarget()
		var new_target = document.getElementById ("new_target") ;
		new_target.style.display = "none" ;
		polygon_target_coords = data.features[len - 1].geometry.coordinates;
		targetNode.value = "Polygon drawn"
	}
}

function deleteRegion(){
	polygon_source_coords = null;
	polygon_target_coords = null;
}

function updateArea(type, polygon_source_coords, polygon_target_coords, currentPolygon, draw) {
	//console.log(type)
	if (type == 'draw.create' || type == 'draw.update') {
		var data = draw.getAll();
		var len = data.features.length;
		if (currentPolygon == "source") {
			polygon_source_coords = data.features[len - 1].geometry.coordinates;
			//console.log(polygon_source_coords);
			if (document.getElementById('inputSource').value == "") {
				document.getElementById('inputSource').value = "Polygon drawn"
			}
		} else {
			polygon_target_coords = data.features[len - 1].geometry.coordinates;
			//console.log(polygon_target_coords);
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
		//console.log("Updating target")
		var data = draw_target.getAll();
		console.log(data);
		console.log(data.features);
		polygon_target_coords = data.features[0].geometry.coordinates;
		//console.log(polygon_target_coords);
	} else if (e.type == 'draw.delete') {
		polygon_target_coords = null;
	}

}