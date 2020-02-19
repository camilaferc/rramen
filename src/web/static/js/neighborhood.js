var parentMap = new Map();
var childrenMap = new Map();
var minLevelNeig = Number.MAX_SAFE_INTEGER;
var minLevel = Number.MAX_SAFE_INTEGER;
var parentSet = new Set();
var childrenSet = new Set();
var childrenFirstLevel = new Set();

function createNeighborhoodList(polygon){
	polygons.features.forEach(function(feature) {
		var id = feature.id;
		var name = feature.properties.name;
		var level = feature.properties.level;
		var parent = feature.properties.parent;
		childrenMap.set(id, parent);

		if (level < minLevel) {
			minLevel = level;
			minLevelNeig = id;
		}

		if (parentMap.get(parent)) {
			listChildren = parentMap.get(parent);
			listChildren.push({
				"id" : id,
				"name" : name,
				"level" : level,
				"parent" : parent
			});
			parentMap.set(parent, listChildren);

		} else {
			parentMap.set(parent, [ {
				"id" : id,
				"name" : name,
				"level" : level,
				"parent" : parent
			} ]);
		}

	});
	
	var list = document.getElementById('list_neighborhoods');
	parentMap.get(minLevelNeig).forEach(
			function(child) {
				childrenFirstLevel.add(child["id"])
				buildNeighborhoodList(child["id"], child["name"], child["level"],
						child["parent"], list);
	});
	
	addEventsNeighborhood();
}

function addEventsNeighborhood(){
	var toggler = document.getElementsByClassName("caret_neig");
	var i;

	for (i = 0; i < toggler.length; i++) {
		toggler[i].addEventListener("click", function() {
			this.parentElement.querySelector(".nested_neig").classList.toggle("active");
			this.classList.toggle("caret_neig-down");
		});
	}
	
	for (let parent of parentSet) {
		  if(childrenSet.has(parent)){
			  addEventsParentChildNeig(parent)
		  }else{
			  addEventsParentNeig(parent)
		  }
	}
	
	
	for (let child of childrenSet) {
		  if(!parentSet.has(child)){
			  addEventsChildNeig(child)
		  }
	}
	
	for (let child of childrenFirstLevel) {
		  if(!parentSet.has(child)){
			  addEventsChildNeig(child)
		  }
	}
}

function addEventsChildNeig(id){
	checkbox = document.getElementById(id)
	checkbox.addEventListener("change", function() {
		parent = childrenMap.get(parseInt(this.id, 10))
		parent_checkbox = document.getElementById(parent)
		if (this.checked){
			if(!parent_checkbox || !parent_checkbox.checked) {
				document.getElementById('targetNode'+it).value = "Neighborhood selected"
				map.setFeatureState({
					source : 'polygons',
					id : this.id
				}, {
					hover : true
				});
				if(parent_checkbox){
					markNeigParent(parent, parent_checkbox);
				}
			}
		} else {
			// Checkbox is not checked..
			map.setFeatureState({
				source : 'polygons',
				id : this.id
			}, {
				hover : false
			});

			if(parent_checkbox){
				updateParentColor(this.id, parent, parent_checkbox)
			}

		}
	});
}

function updateParentColor(child_id, parent_id, parent_checkbox){
	if (parent_checkbox.checked) {
		parent_checkbox.checked = false;
		map.setFeatureState({
			source : 'polygons',
			id : parent_id
		}, {
			hover : false
		});
		colorSiblings(child_id, parent)
		
		if(childrenMap.has(parent_id)){
			parent = childrenMap.get(parent_id)
			parent_checkbox = document.getElementById(parent)
			if(parent_checkbox){
				updateParentColor(parent_id, parent, parent_checkbox)
			}
		}
	}
}

function colorSiblings(id, parent){
	parentMap.get(parent).forEach(function(child) {
		const child_id = child['id']
		if(child_id != id){
			map.setFeatureState({
				source : 'polygons',
				id : child_id
			}, {
				hover : true
			});
			
		}
		
	});
}

function addEventsParentNeig(id){
	const checkbox = document.getElementById(id);
	checkbox.addEventListener("change", function() {
		if (this.checked) {
			document.getElementById('targetNode'+it).value = "Neighborhood selected"
			map.setFeatureState({
				source : 'polygons',
				id : this.id
			}, {
				hover : true
			});
			markNeigChildren(this.id, true)
		} else {
			// Checkbox is not checked..
			map.setFeatureState({
				source : 'polygons',
				id : this.id
			}, {
				hover : false
			});
			markNeigChildren(this.id, false)
		}
	});
}

function addEventsParentChildNeig(id) {
	checkbox = document.getElementById(id)
	checkbox.addEventListener("change", function() {
		parent = childrenMap.get(parseInt(this.id, 10))
		parent_checkbox = document.getElementById(parent)
		if (this.checked) {
			if (!parent_checkbox.checked) {
				document.getElementById('targetNode'+it).value = "Neighborhood selected"
				map.setFeatureState({
					source : 'polygons',
					id : this.id
				}, {
					hover : true
				});
				markNeigParent(parent, parent_checkbox);
			}
			markNeigChildren(this.id, true)
		} else {
			// Checkbox is not checked..
			map.setFeatureState({
				source : 'polygons',
				id : this.id
			}, {
				hover : false
			});
			updateParentColor(this.id, parent, parent_checkbox)
			markNeigChildren(this.id, false)

		}
	});
}

function markNeigChildren(parent_id, marked){
	parent_id = parseInt(parent_id, 10)
	if(parentMap.has(parent_id)){
		parentMap.get(parent_id).forEach(function (child) {
			const child_id = child['id']
			const child_checkbox = document.getElementById(child_id);
			child_checkbox.checked = marked;
			map.setFeatureState({
				source : 'polygons',
				id : child_id
			}, {
				hover : false
			});
			if(parentMap.has(child_id)){
				markNeigChildren(child_id, marked);
			}
    	});
	}
}


function markNeigParent(parent_id, parent_checkbox){
	parent_id = parseInt(parent_id, 10)
	if(parent_checkbox && parentMap.has(parent_id)){
		allMarked = checkChildrenMarkedNeig(parent_id);
		if(allMarked){
			map.setFeatureState({
				source : 'polygons',
				id : parent_id
			}, {
				hover : true
			});
			parent_checkbox.checked = true;
			
			
			parentMap.get(parent_id).forEach(function (child) {
				const child_id = child['id']
				const child_checkbox = document.getElementById(child_id);
				map.setFeatureState({
					source : 'polygons',
					id : child_id
				}, {
					hover : false
				});
	    	});
			if(childrenMap.has(parent_id)){
				parent_parent = childrenMap.get(parent_id);
				const parent_parent_checkbox = document.getElementById(parent_parent);
				markNeigParent(parent_parent, parent_parent_checkbox);
			}
		}
	}
}

function checkChildrenMarkedNeig(parent_id){
	for (const child of parentMap.get(parent_id)) {
		const child_id = child['id']
		const child_checkbox = document.getElementById(child_id);
		if(!child_checkbox || !child_checkbox.checked){
			return false;
		}
	}
	return true;
}


function buildNeighborhoodList(nid, name, level, parent, list){
	if(!parentMap.get(nid)){
		var entry = document.createElement('li');
		if (name.length > 20) {
			name = trunc(name, 20);
		}
	    entry.appendChild(document.createTextNode(name));
	    entry.style.overflow="hidden"
	    var checkbox = document.createElement("INPUT");
	    checkbox.setAttribute("type", "checkbox");
	    checkbox.setAttribute("id", nid);
	    checkbox.classList.add('checkbox_list_neig');
	    entry.appendChild(checkbox);
	    entry.style.overflow = "hidden";
	    entry.style.marginLeft = "18px";
	    entry.setAttribute("id", "n"+nid);
	    list.appendChild(entry);
    }else{
    	var entry = document.createElement('li');
	    var span = document.createElement('span');
	    span.classList.add('caret_neig');
	    entry.appendChild(span);
	    entry.appendChild(document.createTextNode(name));
	    var checkbox = document.createElement("INPUT");
	    checkbox.setAttribute("type", "checkbox");
	    checkbox.setAttribute("id", nid);
	    checkbox.classList.add('checkbox_list_neig_parent');
	    entry.appendChild(checkbox);
	    entry.setAttribute("id", "n"+nid);
	    entry.style.overflow="hidden"
	    list.appendChild(entry);

	    var listNeig = document.getElementById("n"+nid);
	    var entry1 = document.createElement('ul');
	    entry1.style.width = "180px";
	    entry1.style.padding = "12px";
	    entry1.classList.add('nested_neig');
	    entry1.setAttribute("id", "ul_n"+nid);
	    listNeig.appendChild(entry1);

	    var listUl = document.getElementById("ul_n"+nid);

	    parentSet.add(nid)
	    parentMap.get(nid).forEach(function (child) {
	    	childrenSet.add(child["id"])
        	buildNeighborhoodList(child["id"], child["name"], child["level"], child["parent"], listUl);
    	});
    }
}

function getSelectedNeighborhoods(){
	var checked = document.querySelectorAll('input[class="checkbox_list_neig"]:checked');
	var checked_neig = []               		
	for(var i = 0; i < checked.length; i+=1){
   		n = checked[i]
   		checked_neig.push(n.id)
   		
   	}
   	return checked_neig;
}

function clearSelectedNeighborhoods(){
	var checked = document.querySelectorAll('input[class="checkbox_list_neig"]:checked');
	for(var i = 0; i < checked.length; i+=1){
   		n = checked[i]
   		n.checked = false;
   		map.setFeatureState({
			source : 'polygons',
			id : n.id
		}, {
			hover : false
		});
   		
   	}
	
	checked = document.querySelectorAll('input[class="checkbox_list_neig_parent"]:checked');
	for(var i = 0; i < checked.length; i+=1){
   		n = checked[i]
   		n.checked = false;
   		map.setFeatureState({
			source : 'polygons',
			id : n.id
		}, {
			hover : false
		});
   	}
	
	var toggler = document.getElementsByClassName("caret_neig-down");
	for (var i = 0; i < toggler.length; i++) {
		toggler[i].parentElement.querySelector(".nested_neig").classList.toggle("active");
		toggler[i].classList.toggle("caret_neig-down");
	}
}

function trunc(str, n) {
	 return str.slice(0, n) + "."
}

function addNeighborhoodLocation(location){
    if(location == "source"){
    	alert("Neighborhood as source currently not supported.")
    	return
    }
    if (location == "target" && it > 0 && Object.keys(targets).length > 0) {
    	var parent = document.getElementById("input_target");
		var textbox = parent.getElementsByTagName('input');
		if(textbox.length > 1){
			alert("Mixed destination types not supported (point already selected).")
			return
		}

	}
    else{
    	var divList = document.getElementById("neig_div");
    	divList.style.display = "block";
    	document.getElementById('targetNode' + it).value = "Select neighborhoods"
    	var targetNode = document.getElementById('targetNode'+it)
        if (targetNode.value != ""){
        	removeMarkerTarget(targetNode)
        	removePolygonTarget()
        }
    		
    }
}

function removeNeighborhoodTarget(){
	closeNeighborhoodList()
	clearSelectedNeighborhoods();
}

function closeNeighborhoodList(){
	var divList = document.getElementById("neig_div");
	divList.style.display = "none";
}