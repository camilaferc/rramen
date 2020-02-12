function removeSegment(remove, segment_id) {
	//console.log(remove, segment_id)
	popup = popup_seg.get(segment_id)
	popup.remove()
	if (!remove) {
		map.removeLayer("seg_" + segment_id);
		map.removeSource("seg_" + segment_id);
	} else {
		var div = document.getElementById("segments_div");
		var button = document.getElementById("button-edit-segments");
		button.className += " active_edit";
		div.style.display = "inline";

		var list = document.getElementById("list_segments");
		var entry = document.createElement('li');
		entry.style.overflow = "auto";
		entry.appendChild(document.createTextNode(segment_id));
		var checkbox = document.createElement("INPUT");
		checkbox.setAttribute("type", "checkbox");
		checkbox.setAttribute("id", "cseg_" + segment_id);
		checkbox.setAttribute("title", "check to add segment back to network");
		title="Pepperoni"
		checkbox.classList.add('checkbox_list_segment');
		checkbox.checked = false;
		entry.appendChild(checkbox);
		//entry.setAttribute("id", "n" + transp_type);
		list.appendChild(entry);
	}
}