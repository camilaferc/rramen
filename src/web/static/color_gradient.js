function hexToRgb(hex) {
	  var result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
	  return result
	    ? {
	        r: parseInt(result[1], 16),
	        g: parseInt(result[2], 16),
	        b: parseInt(result[3], 16)
	      }
	    : null;
	}

	// returns an array of startColor, colors between according to steps, and endColor
	function getColorGradient(startColor, endColor, p) {

	  var startColorRgb = hexToRgb(startColor);
	  var endColorRgb = hexToRgb(endColor);

	  var rInc = Math.round((endColorRgb.r - startColorRgb.r) * p);
	  var gInc = Math.round((endColorRgb.g - startColorRgb.g) * p);
	  var bInc = Math.round((endColorRgb.b - startColorRgb.b) * p);

	  startColorRgb.r += rInc;
	  startColorRgb.g += gInc;
	  startColorRgb.b += bInc;

	    //console.log(startColorRgb)
	    
	    rgb = "#" + rgbToHex(startColorRgb.r) + rgbToHex(startColorRgb.g) + rgbToHex(startColorRgb.b)
	    //console.log(rgb)
	  return rgb;
	}
	  
	function rgbToHex(rgb) { 
		  var hex = Number(Math.round(rgb)).toString(16);
		  if (hex.length < 2) {
		       hex = "0" + hex;
		  }
		  return hex;
	};
