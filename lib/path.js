/**
 * Module for looking up objects by path-based identifiers
 */
var all = require("promised-io/promise").all,
	when = require("promised-io/promise").when,
	getLink = require("commonjs-utils/json-schema").getLink;

exports.resolver = function resolver(storeGet, storeQuery, getDataModel){
	// Creates a function for resolving ids that have dot-delimited paths,
	// and resolves any links in those paths
	// 		storeGet: The store get method to use to resolve ids
	// 		storeQuery: The store query method to use to resolve ids
	//		getDataModel: Optional parameter for resolving cross-model references 
	return function(id, metadata){
		metadata = metadata || {};
		id = '' + id;
		var schema = this;
		if(id.indexOf('.') > -1 && (id.indexOf('?') == -1 || id.indexOf('.') < id.indexOf('?'))){
			var parts = id.split('.');
			var value = storeGet(parts.shift());
			parts.forEach(function(part){
				value = when(value, function(value){
					var linkTarget = schema != this && getLink(part, value, schema);
					if(!linkTarget){
						value = value && value[part];
						linkTarget = value && value.$ref;
					}
					if(linkTarget){
						if((linkTarget.charAt(0) == '/' || linkTarget.substring(0,3) == '../') && getDataModel){
							value = getFromDataModel(getDataModel(), linkTarget.substring(linkTarget.charAt(0) == '/' ? 1 : 3));
						}else{
							value = storeGet(linkTarget);
						}
					}
					return value;
				}); 
			});
			return value;
		}
		if(id === '' || id.charAt(0) == "?"){
			return storeQuery(id.substring(1), metadata);
		}
		var parts = id.match(/[\.#][^\.#]+/g);
		if(parts){
			var value = storeGet(id.match(/^([^\.#]*)[\.#]/)[0]);
			for(var i = 0; i < parts.length; i++){
				var part = parts[i];
				value = value[part.substring(1)];
			}
			return value;
		}
		if(id.match(/^\(.*\)$/)){
			// handle paranthesis embedded, comma separated ids
			if(id.length == 2){ // empty array
				return [];
			}
			var parts = id.substring(1, id.length -1).split(',');
			return all(parts.map(function(part){
				return storeGet(part, metadata);
			}));
		}
		return storeGet(id, metadata);
	};
};
exports.resolve = function(id, metadata, storeGet, storeQuery, getDataModel){
	return exports.resolver(storeGet, storeQuery, getDataModel)(id, metadata);
}

function getFromDataModel(dataModel, path){
	var model = dataModel;
	var part; 
	do{
		var proceed = false;
		var slashIndex = path.indexOf("/");
		if(slashIndex > -1){
			part = path.substring(0, slashIndex);
			if(model[part]){
				model = model[part];
				path = path.substring(slashIndex + 1);
				proceed = true;
			}
		}
	}while(proceed);
			//require("sys").puts("get from " + model + " path: " + path);
	
	return exports.resolver(model, function(){;
		return dataModel;
	})(path);
}	