function removeSegment(remove, segment_id, source, target) {
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
		checkbox.setAttribute("source", source);
		checkbox.setAttribute("target", target);
		checkbox.classList.add('checkbox_list_segment');
		checkbox.checked = false;
		entry.appendChild(checkbox);
		list.appendChild(entry);
	}
}

function getRemovedSegments(){
	var not_checked = document.querySelectorAll('input[class="checkbox_list_segment"]:not(:checked)');
	var removed_segments = {}             		
	for(var i = 0; i < not_checked.length; i+=1){
   		var segment = not_checked[i].id
   		//console.log(segment)
   		//var sid = segment.split("_")[1];
   		var source = parseInt(not_checked[i].getAttribute("source"), 10)
   		var target = parseInt(not_checked[i].getAttribute("target"), 10)
   		//console.log(source, target)
   		if(source in removed_segments){
   			removed_segments[source].push(target)
   		}else{
   			removed_segments[source] = [target]
   		}
   	}
   	console.log(removed_segments)
   	return removed_segments
}