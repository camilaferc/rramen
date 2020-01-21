function showRoutes(button) {
	var list = document.getElementById("routes_list_div");
	if (list.style.display == "none") {
		button.className += " active_edit";
		list.style.display = "inline";
	} else {
		list.style.display = "none"
		button.className = button.className.replace(" active_edit", "");
	}
}

function selectSegments(button) {
	var list = document.getElementById("segments_div");
	if (list.style.display == "none") {
		button.className += " active_edit";
		list.style.display = "inline";
	} else {
		list.style.display = "none"
		button.className = button.className.replace(" active_edit", "");
	}
}