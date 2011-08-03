/**
 * A very simple filesystem based storage
 */
var fs = require("promised-io/fs"),
	MIME_TYPES = require("jack/mime").MIME_TYPES,
	when = require("promised-io/promise").when,
	AutoTransaction = require("../transaction").AutoTransaction;

function BinaryFile(){
}

exports.defaultContentType = "application/octet-stream";

var FileSystem = exports.FileSystem = function(options){
	var fsRoot = require("commonjs-utils/settings").dataFolder || "data"
	if(options.dataFolder){
		fsRoot = options.dataFolder.charAt(0) == '/' ? options.dataFolder : fsRoot + '/' + options.dataFolder;
	}
	var defaultContentType = options.defaultContentType || exports.defaultContentType;
	var storePath;

	var store = AutoTransaction({
		get: function(id, metadata){
			var filename;
			var parts = getFilePathAndMetadata(id);
			var extraParts = parts.extra;
			var fp = parts.file;
			try{
				if (fs.statSync(fp).isFile()){
					filename = fp;
				}
			}catch(e){
				if (!options.defaultExtension) return;

				fp += options.defaultExtension ? ("."+options.defaultExtension):"";
				try {
					if (fs.statSync(fp).isFile()) {
						filename = fp;
					}
				}catch(e){
					return;
				}
			}

			var extension = filename.match(/\.[^\.]+$/);
			var f = new BinaryFile();

			f.forEach = function(callback){
				var file = fs.open(filename, "br");
				return file.forEach(callback);
			};
			f.forEach.binary = true;

			// FIXME we can find a better way
			f.getFileHandle = function() {
				return fs.open(filename, "br");
			};
			f.getFileHandleChars = function() {
				return fs.open(filename, "br"); // FIXME just "r"?
			};

			f.path = filename;

			f.getMetadata = function(){
				return f;
			};
			var pathParts = filename.split("/")
			var fname = pathParts[pathParts.length-1];
			Object.defineProperty(f,"alternates",{
				value: [f]
			});
			f.id = id;

			var explicitType = extraParts && extraParts[0];
			var explicitDisposition = extraParts && extraParts[1];
			if(!explicitDisposition && explicitType && explicitType.indexOf("/") == -1){
				explicitDisposition = explicitType;
				explicitType = false;
			}

			f['content-type'] = explicitType || MIME_TYPES[extension] || defaultContentType;
			f['content-disposition'] = ((explicitDisposition && (explicitDisposition.charAt(0)=="a")) ? "attachment" : "inline") + "; filename=" + fname;
			f["content-length"] = fs.statSync(filename).size;

			return f;
		},
		put: function(object, directives){
			if (object.id){
				return object.id; // FIXME WTF?
			}
			var id = object.id = directives.id || generateId(object);
			var filename = getFilePathAndMetadata(id).file;
			return when(fs.stat(filename),
				function(){
					if(directives.overwrite === false){
						throw new Error("Cannot overwrite existing file");
					}
					return writeFile();
				},
				function(){
					if(directives.overwrite === true){
						throw new Error("No existing file to overwrite");
					}
					return writeFile();
				});
			function writeFile(){
				var path = object.path || object.tempfile
				var forEach = object.forEach;
				// FIXME when object is passed as a native array the forEach gets dropped in facet.js
				if(path || forEach){
					store.addToTransactionQueue(function(){
						if(object.__stored__){
							return;
						}
						Object.defineProperty(object, "__stored__",{
							value: true,
							enumerable: false
						});
						fs.makeTree(filename.substring(0, filename.lastIndexOf("/")));
						if(path){
							var fromFile = fs.open(path, "b");
							fs.open(filename, "wb").write(fromFile.read()).close();
							return fromFile.close();
							//return fs.move(path, filename);
						}
						var file = fs.open(filename, "wb");
						return when(forEach.call(object, function(buffer){
							file.write(buffer);
						}), function(){
							return file.close();
						});
					});
					return id;
				}
			}
		},
		"delete": function(id, directives){
			var path = getFilePathAndMetadata(id).file;
			store.addToTransactionQueue(function(){
				fs.remove(path);
			});
		},
		setPath: function(path) {
			return storePath = path;
		},
		getPath: function() {
			return storePath;
		}
	});
	return store;
	function getFilePathAndMetadata(id){
		var extra = id.split("$");
		var fp = id;
		if (extra[1]){
			var extraParts = extra[1].split(",");
			fp = extra[0];
		}

		var fp= [fsRoot, fp].join('/');
		return {
			file: fp,
			extra: extraParts
		};
	}

}
var REVERSE_MIME_TYPES = {};
for(var i in MIME_TYPES){
	REVERSE_MIME_TYPES[MIME_TYPES[i]] = i;
}
exports.folderDepth = 3; // depth of file directory paths to use
exports.folderLength = 2; // length of initial file directory paths to use

function generateId(object){
	var id = [];
	for(var i = 0; i < exports.folderDepth; i++){
		id.push(Math.random().toString().substring(2, 2 + exports.folderLength));
	}
	var sourceFn = object.filename || object.path || "";
	var extension = sourceFn.match(/\.[^\.]+$/);
	var mime = MIME_TYPES[extension && extension[0]];
	var filename = object.filename || Math.random().toString().substring(2);
	id.push(filename);
	id = id.join("/");

	var checkedAttachment;
	if(object["content-type"] && object["content-type"] !== mime){
		if(object.filename || !REVERSE_MIME_TYPES[object["content-type"]]){
			id += "$" + object["content-type"];
			checkedAttachment = true;
			if(object["content-disposition"] == "attachment"){
				id += ",attachment";
			}
		}else{
			id += REVERSE_MIME_TYPES[object["content-type"]];
		}
	}
	else if (mime) {
		id += extension[0];
	}
	if(!checkedAttachment && object["content-disposition"] == "attachment"){
		id += "$attachment";
	}
	return id;
}
