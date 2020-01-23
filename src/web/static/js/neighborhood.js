var parentMap = new Map();
var childrenMap = new Map();
var minLevelNeig = Number.MAX_SAFE_INTEGER;
var minLevel = Number.MAX_SAFE_INTEGER;
var parentSet = new Set();
var childrenSet = new Set();

function createNeighborhoodList(polygon){
	polygons.features.forEach(function(feature) {
		var id = feature.id;
		var name = feature.properties.name;
		var level = feature.properties.level;
		var parent = feature.properties.parent;
		//console.log(id, name, level, parent);
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
	//console.log(childrenMap)
	//console.log(minLevel)
	
	var list = document.getElementById('list_neighborhoods');
	parentMap.get(minLevelNeig).forEach(
			function(child) {
				//console.log(child);
				buildNeighborhoodList(child["id"], child["name"], child["level"],
						child["parent"], list);
	});
	
	addEventsNeighborhood();
}

function addEventsNeighborhood(){
	var toggler = document.getElementsByClassName("caret_neig");
	var i;

	for (i = 0; i < toggler.length; i++) {
		//console.log(toggler[i])
		toggler[i].addEventListener("click", function() {
			//console.log("clicking!")
			//console.log(this)
			this.parentElement.querySelector(".nested").classList.toggle("active");
			this.classList.toggle("caret_neig-down");
		});
	}
	
	//addEventsParents()
	//addEventsChildren()
	//console.log(parentSet)
	//console.log(childrenSet)
	
	for (let parent of parentSet) {
		  //console.log(parent);
		  if(childrenSet.has(parent)){
			  addEventsParentChildNeig(parent)
		  }else{
			  addEventsParentNeig(parent)
		  }
	}
	
	
	for (let child of childrenSet) {
		  //console.log(child);
		  if(!parentSet.has(child)){
			  addEventsChildNeig(child)
		  }
	}
}

function addEventsChildNeig(id){
	checkbox = document.getElementById(id)
	checkbox.addEventListener("change", function() {
		//console.log(this.id)
		parent = childrenMap.get(parseInt(this.id, 10))
		//console.log(parent)
		parent_checkbox = document.getElementById(parent)
		if (this.checked){
			console.log(this.id + " checked")
			if(!parent_checkbox.checked) {
				//console.log(parent + " parent checked")
				document.getElementById('inputTarget').value = "Neighborhood selected"
				map.setFeatureState({
					source : 'polygons',
					id : this.id
				}, {
					hover : true
				});
				markNeigParent(parent, parent_checkbox);
			}
		} else {
			// Checkbox is not checked..
			//console.log(this.id + " NOT checked")
			map.setFeatureState({
				source : 'polygons',
				id : this.id
			}, {
				hover : false
			});

			updateParentColor(this.id, parent, parent_checkbox)

		}
	});
}

function updateParentColor(child_id, parent_id, parent_checkbox){
	if (parent_checkbox.checked) {
		//console.log(parent + " parent checked")
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
			//console.log(parent)
			parent_checkbox = document.getElementById(parent)
			if(parent_checkbox){
				updateParentColor(parent_id, parent, parent_checkbox)
			}
		}
	}
}

/*
 * function addEventsChildren(){ const checkboxes =
 * document.getElementsByClassName("checkbox_list_neig"); //console.log(checkboxes);
 * //console.log(childrenMap) for (i = 0; i < checkboxes.length; i++) {
 * //console.log(checkboxes[i]) //console.log(checkboxes[i].id)
 * checkboxes[i].addEventListener("change", function() { console.log(this.id)
 * parent = childrenMap.get(parseInt(this.id, 10)) console.log(parent)
 * parent_checkbox = document.getElementById(parent) if (this.checked &&
 * !parent_checkbox.checked) { console.log(this.id + " checked")
 * map.setFeatureState({ source : 'polygons', id : this.id }, { hover : true }); }
 * else { // Checkbox is not checked.. map.setFeatureState({ source :
 * 'polygons', id : this.id }, { hover : false });
 * 
 * if(parent_checkbox.checked){ parent_checkbox.checked = false;
 * map.setFeatureState({ source : 'polygons', id : parent }, { hover : false });
 * colorSiblings(this.id, parent) }
 *  } }); } }
 */

function colorSiblings(id, parent){
	parentMap.get(parent).forEach(function(child) {
		const child_id = child['id']
		if(child_id != id){
			//console.log(child_id + " colored, parent:" + parent)
			map.setFeatureState({
				source : 'polygons',
				id : child_id
			}, {
				hover : true
			});
			/*if(parentMap.has(child_id)){
				colorSiblings(-1, child_id)
			}*/
		}/*else{
			console.log(child_id + " will not be colored")
		}*/
		
	});
}

function addEventsParentNeig(id){
	const checkbox = document.getElementById(id);
	checkbox.addEventListener("change", function() {
		if (this.checked) {
			document.getElementById('inputTarget').value = "Neighborhood selected"
			console.log(this.id + " checked")
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
		//console.log(this.id)
		parent = childrenMap.get(parseInt(this.id, 10))
		//console.log(parent)
		parent_checkbox = document.getElementById(parent)
		if (this.checked) {
			console.log(this.id + " checked")
			if (!parent_checkbox.checked) {
				document.getElementById('inputTarget').value = "Neighborhood selected"
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

/*
 * function addEventsParents(){ const checkboxes =
 * document.getElementsByClassName("checkbox_list_neig_parent");
 * //console.log(checkboxes); for (i = 0; i < checkboxes.length; i++) {
 * //console.log(checkboxes[i].id) checkboxes[i].addEventListener("change",
 * function() { if (this.checked) { console.log(this.id + " checked")
 * map.setFeatureState({ source : 'polygons', id : this.id }, { hover : true });
 * markNeigChildren(this.id, true) } else { // Checkbox is not checked..
 * map.setFeatureState({ source : 'polygons', id : this.id }, { hover : false
 * }); markNeigChildren(this.id, false) } }); } }
 */

function markNeigChildren(parent_id, marked){
	parent_id = parseInt(parent_id, 10)
	if(parentMap.has(parent_id)){
		parentMap.get(parent_id).forEach(function (child) {
			const child_id = child['id']
			//console.log(child_id)
			const child_checkbox = document.getElementById(child_id);
			child_checkbox.checked = marked;
			map.setFeatureState({
				source : 'polygons',
				id : child_id
			}, {
				hover : false
			});
			if(parentMap.has(child_id)){
				//console.log(child_id + " has children");
				markNeigChildren(child_id, marked);
			}
    	});
	}
}


function markNeigParent(parent_id, parent_checkbox){
	parent_id = parseInt(parent_id, 10)
	//console.log(parent_id)
	if(parent_checkbox && parentMap.has(parent_id)){
		allMarked = checkChildrenMarkedNeig(parent_id);
		if(allMarked){
			//console.log("all children are marked:" + parent_id)
			map.setFeatureState({
				source : 'polygons',
				id : parent_id
			}, {
				hover : true
			});
			parent_checkbox.checked = true;
			
			
			parentMap.get(parent_id).forEach(function (child) {
				const child_id = child['id']
				//console.log(child_id)
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
				//console.log("parent_parent:" + parent_parent)
				const parent_parent_checkbox = document.getElementById(parent_parent);
				markNeigParent(parent_parent, parent_parent_checkbox);
			}
		}
	}
}

function checkChildrenMarkedNeig(parent_id){
	for (const child of parentMap.get(parent_id)) {
		const child_id = child['id']
		//console.log(child_id)
		const child_checkbox = document.getElementById(child_id);
		if(!child_checkbox || !child_checkbox.checked){
			//console.log(child_id + " NOT marked")
			return false;
		}
	}
	return true;
}


function buildNeighborhoodList(nid, name, level, parent, list){
	//console.log(nid, name);
	
	if(!parentMap.get(nid)){
    	//console.log("leaf reached:" + nid);
		var entry = document.createElement('li');
		//console.log(name.length)
		if (name.length > 20) {
			//console.log(name)
			name = trunc(name, 20);
			//console.log(name)
		}
	    entry.appendChild(document.createTextNode(name));
	    entry.style.overflow="hidden"
	    //entry.style.width="80%"
	    var checkbox = document.createElement("INPUT");
	    checkbox.setAttribute("type", "checkbox");
	    checkbox.setAttribute("id", nid);
	    checkbox.classList.add('checkbox_list_neig');
	    entry.appendChild(checkbox);
	    entry.style.overflow = "hidden";
	    entry.setAttribute("id", "n"+nid);
	    list.appendChild(entry);
    }else{
    	//console.log("node has children:" + nid);
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
	    entry1.classList.add('nested');
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
		console.log(toggler[i])
		toggler[i].parentElement.querySelector(".nested").classList.toggle("active");
		toggler[i].classList.toggle("caret_neig-down");
		//toggler[i].parentElement.querySelector("active").classList.toggle(".nested");
		//toggler[i].classList.remove('caret-down');
	}
}

function trunc(str, n) {
	 return str.slice(0, n) + "."
}