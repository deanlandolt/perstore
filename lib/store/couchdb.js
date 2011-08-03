/*
 * CouchDB store
 */

var http = require("promised-io/http-client");
var defer = require("promised-io/promise").defer;
var when = require("promised-io/promise").when;
var LazyArray = require("promised-io/lazy-array").LazyArray;
var streamToString = require("promised-io/lazy-array").streamToString;
var settings = require("commonjs-utils/settings");
var NotFoundError = require("../errors").NotFoundError;
//var getIdentityProperty = require("commonjs-utils/json-schema").getIdentityProperty;


// KEEP
function streamToObject(body) {
    return when(streamToString(body), function(string) {
        return JSON.parse(string);
    });
};



function streamToDocument(body, metadata, identityProperty) {
    /* converts a jsgi response body from couch to a document object */
    return when(streamToObject(body), function(object) {
	return objectToDocument(object, metadata, identityProperty);
    });
};

function responseToObject(response) {
    return when(response, function(response) {
	return streamToObject(response.body);
    });
};

function responseToDocument(response, identityProperty) {
    /* converts a jsgi response from couch to a document object */
    return when(response, function(response) {
	if (response.status === 404) return;
	return when(streamToObject(response.body), function(object) {
	    return objectToDocument(object, response.headers, identityProperty);
	});
    });
};

function objectToDocument(object, metadata, identityProperty) {
    metadata = metadata || {};
    var id = identityProperty || "id";
    if (id && id !== "_id") {
	object[id] = object._id;
	delete object._id;
    }
    if ("_rev" in object) {
	metadata.etag = object._rev;
	delete object._rev;
    }
    object.getMetadata = function() {
	return metadata;
    };
    return object;
};

function streamToNumber(stream) {
    return when(streamToString(stream), function(string) {
	return Number(string);
    });
};

function streamToArray(body, metadata, expectedLength) {
    /* takes advantage of couch's consistent view formatting to stream json */
    metadata = metadata || {};
    var array = LazyArray({
        some: function(write) {
	    var remainder = "";
            var header;
            return body.forEach(function(chunk) {
                chunk = remainder + chunk;
                if (!header) {
                    // first capture the header object
                    header = chunk.match(/^(.*?),"rows":\[/);
                    if (!header) throw new Error("invalid response array");
                    chunk = chunk.substring(header[0].length);
                    header = JSON.parse(header[1] + "}");
		    // TODO add these to body before returning
                    array.totalCount = header.total_rows; // FIXME is this legit?
                    if (typeof expectedLength === "number") {
                        array.length = Math.min(expectedLength, totalCount);
                    }
                }
                var rows = chunk.split("\n").filter(function(row) {
                    return row.trim();
                }).map(function(row) {
                    return row.replace(/,\s*$/, "");
                });
                remainder = rows.pop();
                rows.forEach(function(row) {
                    write(JSON.parse(row));
                });
                // TODO do we ever need to handle the trailing "]}"?
            });
        }
    });
    array.getMetadata = function() {
	return metadata;
    };
    return array;
};

function responseToArray(response, expectedLength) {
    return when(response, function(response) {
	if (response.status === 404) throw new NotFoundError("not found");
	return streamToArray(response.body, response.headers, expectedLength);
    });
};

function objectToStream(object) {
    return [JSON.stringify(object)];
};

function documentToStream(object, directives, identityProperty) {
    directives = directives || {};
    // FIXME where should identityProperty come from?
    var id = identityProperty || "id";
    if (id !== "_id") {
	object._id = object[id];
	delete object[id];
    }
    if (directives["if-match"]) {
	object._rev = directives["if-match"];
    }
    return objectToStream(object);
};

exports.defaultHost = "http://localhost:5984/";

exports.Server = function(server) {
    server = server || {};
    var host = server.host || (settings.couchdb && settings.couchdb.host) || exports.defaultHost;
    
    // this method binds server host information to the rest of the url
    server.request = function(method, path, headers, body) {
	var request = {
	    method: method,
	    url: host + path
	};
	if (headers) request.headers = headers;
	if (body) request.body = body;
	return http.request(request);
    };
    
    // add simple method helpers
    ["GET", "POST", "PUT", "DELETE", "COPY"].forEach(function(method) {
	server[method] = function(path, headers, body) {
	    return server.request(method, path, headers, body);
	}
    });
    
    server.getInfo = function() {
        return responseToObject(server.GET("_config"));
    };
    
    server.createDatabase = function(name) {
	// A database must be named with all lowercase letters (a-z), digits (0-9), or any of the _$()+-/ characters and must end with a slash in the URL. The name has to start with a lowercase letter (a-z).
	return when(server.PUT(name + "/"), function(response) {
	    if (response.status === 201) {
		return true;
	    }
	    else if (response.status === 412) {
		return false;
	    }
	    // FIXME what other errors? auth? name errors?
	    // TODO throw perstore-specific errors?
	    throw new Error("database create failed");
	});
    };
    
    server.deleteDatabase = function(name) {
	return when(server.DELETE(name + "/"), function(response) {
	    if (response.status === 200) return true;
	});
    };
    
    return server;
};

exports.defaultServer = exports.Server();
exports.defaultDatabaseName = "perstore";

exports.Database = function(config) {
    config = config || {};
    var db = config.db || {};
    db.name = config.name || exports.defaultDatabaseName;
    var server = db.server || exports.defaultServer;
    var path;
    
    db.get = function(id, directives) {
        return responseToDocument(server.GET(db.name + "/" + id, directives));
    };
    
    db.query = function(query, directives) {
	if (!query) return db.getAllDocuments();
        // TODO parse and inspect query
	return responseToArray(server.GET(db.name + "/_someview" + query, directives)).map(function(object) {
	    function(object) {
		return objectToDocument(response);
	    }
	});
    };
    
    function mvccOverride(directives) {
	if (!db.mvcc && !directives["if-match"]) {
	    var previous = directives.previous;
	    var ifMatch = previous && previous.getMetadata && previous.getMetadata().etag;
	    if (ifMatch) {
		directives["if-match"] = ifMatch;
	    }
	}
    };
    
    db.put = function(object, directives) {
	directives = directives || {};
	var id = object.id || directives.id;
	mvccOverride(directives);
	
	var document = JSON.parse(JSON.stringify(object)); // FIXME use a better copy op
	var idProp = db.identityProperty || "id";
	if (idProp !== "_id") {
	    document._id = document[idProp];
	    delete document[idProp];
	}
	var rev = directives["if-match"];
	if (rev) document._rev = rev;
	
	var response = server.PUT(db.name + "/" + id, directives, [JSON.stringify(document)]);
	return when(response, function(response) {
	    return when(streamToObject(response.body), function(result) {
		if (!result.ok) {
		    if (response.status === 400) {
			throw new URIError(result.reason);
		    }
		    throw new require("../errors").DatabaseError(result.reason);
		}
		var metadata = {
		    etag: result.rev
		}
		object.getMetadata = function() {
		    return metadata;
		}
		return result.id;
	    });
	});
	
	return responseToDocument(server.PUT(db.name + "/" + id, directives, documentToStream(object, directives)));
    };
    
    db["delete"] = function(id, directives) {
	directives = directives || {};
	mvccOverride(directives);
	// FIXME couch not respecting if-match header per the docs:
	// return when(server.DELETE(db.name + "/" + id, directives), function(response) {
	return when(server.DELETE(db.name + "/" + id + "?rev=" + directives["if-match"], directives), function(response) {
	    if (response.status === 200) return true;
	});
    };
    
    var schema;
    db.setSchema = function(s) {
        return schema = s;
    };
    
    db.setPath = function(p) {
	path = p;
    };
    
    db.getPath = function() {
	return path;
    }
    
    /*
     * CouchDB-specific API extensions
     */
    
    db.copy = function(id, directives) {
	directives = directives || {};
	if (!directives.destination)
	    throw new Error("A destination directive must be supplied");
        if (directives["if-match"]) {
	    // TODO file a couch issue re: supporting if-match with destination
	    directives.destination += "?rev=" + directives["if-match"];
	    delete directives["if-match"];
	}
        
        // TODO handle * by subverting MVCC?
	return when(server.COPY(db.name + "/" + id, directives), function(response) {
	    // TODO
	    // is it 201 created?
	    // get old object and add getMetadata fn w/ etag
	    var getMetadata = function() {
		return {
		    etag: response.headers.etag
		}
	    }
	    return true;
	});
    };
    
    db.getAllDocuments = function(options) {
        options = options || {};
        var results = responseToArray(server.GET(db.name + "/_all_docs?include_docs=true")).map(function(object) {
            return object.doc;
        });
        if (!options.includeDesigns) {
            results = results.filter(function(object) {
                return !object._id || object._id.indexOf("_design/") !== 0;
            });
        }
        return results.map(function(object) {
            return objectToDocument(object);
        });
    }
    
    db.getAllDesigns = function() {
	var path = db.name + "/_all_docs?include_docs=true&startkey=%22_design%2F%22&endkey=%22_design0%22";
        return responseToArray(server.GET(path)).map(function(object) {
	    return objectToDocument(object.doc);
        });
    };
    
    db.getDesign = function(name) {
        if (!name) return db.getAllDesigns();
        return responseToDocument(server.GET(db.name + "_design/" + name));
    };
    
    db.viewCleanup = function() {
        return responseToObject(server.POST(db.name + "_view_cleanup"));
    };
    
    db.compactView = function(name) {
        return responseToObject(server.POST(db.name + "_compact/" + name));
    };
    
    db.compact = function() {
        return responseToObject(server.POST(db.name + "_compact"));
    };
    
    db.create = function() {
	return server.createDatabase(db.name);
    };
    
    db.drop = function() {
	return server.dropDatabase(db.name)
    };
    
    db.clear = function() {
	return when(server.dropDatabase(db.name), function() {
	    return server.createDatabase(db.name);
	});
    };
    
    db.getInfo = function() {
        return responseToObject(server.GET(db.name + "/"));
    };
    
    db.getRevisionLimit = function() {
	return when(server.GET(db.name + "/_revs_limit"), function(response) {
	    return streamToNumber(response.body);
	});
    };
    
    db.setRevisionLimit = function(value) {
	return when(server.PUT(db.name + "/_revs_limit"), function(response) {
	    return streamToNumber(response.body);
	});
    };
    
    return db;
}

var defaultDatabase = exports.Server(exports.defaultDatabaseName);


exports.Design = function(name, config) {
    if (!name) throw new Error("No name defined for design document");
    config = config || {}
    
    var design = {};
    return design;
};

var defaultDesign;


exports.View = function(name, config) {
    if (!name) throw new Error("No name defined for view");
    config = config || {}
    
    var db = config.database || defaultDatabase;
    var schema;
    
    return {
        get: function(id) {
            
        },
        query: function(query, directives) {
            query = parseQuery(query);
            function parse(terms) {
                
            }
            
        },
        setSchema: function(s) {
            return schema = s;
        }
    }
}

exports.Class = function(config) {
    /** 
     ** Extends the read-only View store to add write methods for a class
     **/
    config = config || {};
    var db = config.database || defaultDatabase;
    var store = exports.View(config);
    var schema;
    
    store.put = function(object, directives) {
        
    };
    store["delete"] = function(id) {
        
    };
    store.setSchema = function(s) {
        store.setSchema(s);
        return schema = s;
    };
    
    return store;
};



exports.Managed = function(config) {
    config = config || {};
    var db = config.database || defaultDatabase;
    var backingStore = exports.Document(config);
    var schema;
    var store = {};
    store.query = function(query, directives) {
        
        
        var q = {
            couch: {
                include_docs: true,
                reduce: false
            },
            setDimension: function(dimension) {
                if (store.dimension) {
                    if (dimension !== store.dimension) {
                        // can't do multidimensional queries in couch
                        throw new Error("Unsatisfiable query");
                    }
                    // check existing store.couch to make sure it's sane
                }
                store.dimension = dimension;
            }
        }
        
        if (directives.stale) q.couch.stale = "ok"; //FIXME what's the right header?
        
        var compile = {
            eq: function(args) {
                q.setDimension(args[0]);
                q.couch.key = args[1];
            },
            ge: function(args) {
                q.setDimension(args[0]);
                q.couch.startkey = args[1];
            },
            gt: function(args) {
                q.setDimension(args[0]);
                q.couch.startkey = args[1];
                q.excludeStart = args[1]; // TODO
            },
            le: function(args) {
                q.setDimension(args[0]);
                q.couch.startkey = args[1];
            },
            lt: function(args) {
                q.setDimension(args[0]);
                q.couch.startkey = args[1];
                q.excludeEnd = args[1];
                q.couch.inclusive_end = false;
            },
            limit: function(args) {
                q.couch.limit = args[0];
                if (args[1]) q.couch.skip = args[1];
            },
            reverse: function(args) {
                q.couch.descending = true;
                var startkey = q.couch.startkey;
                q.couch.startkey = q.couch.endkey;
                q.couch.endkey = startkey;
                // reset inclusive start/end
                var excludeStart = q.excludeStart;
                q.excludeStart = q.excludeEnd;
                q.excludeEnd = excludeStart;
                q.couch.inclusive_end = typeof q.excludeEnd === "undefined";
            },
            select: function(args) {
                // postprocess, break
            },
            values: function(args) {
                // postprocess, break
            },
            distinct: function(args) {
                // postprocess, break
            }
        };
        
        // add the reduce ops
        ["sum", "count", "min", "max", "sumsqr"].forEach(function(op) {
            compile[op] = function(args) {
                q.couch.include_docs = false;
                q.couch.reduce = true;
                q.returnProperty = op;
                throw new Error("Cannot continue");
            }
        });
        
        if (typeof query === "string") query = parseQuery(query);
        
        // if user has privs and query starts with "or" submit q.couch and apply whole query to lazy-array
        // if query starts with "and" term loop over
        //   try {
        //     compile[term]
        //     toplevelargs.shift()
        //   catch (e) {
        //      // if BadRequestError
        //      if priveledged, submit q.couch as is and apply the rest of the query to lazy-array
        // } else {
        //      compile[query.name](query.args);
        // }
        // 
        
        compile[query.name](query.args);
    }
    
    store.setSchema = function(s) {
        store.setSchema(s);
        var indexedProperties = [];
        for (var name in s.properties) {
            var prop = s.properties[name];
            if (s.properties[name].index) {
                indexedProperties.push(name);
            }
        }
        if (indexedProperties.length) {
            var design = db.getDesign();
            indexedProperties.forEach(function(name) {
                design.views = design.views || {};
                design.views[schema.id + "." + name] = {
                    map: "function(doc) { if (doc.perstore_class == '" + schema.id + "') emit(doc." + name + ", 1) }",
                    reduce: "_stats"
                }
            });
            db.setDesign(design);
        }
        return schema = s;
    }
    return store;
}

/*exports.errorHandler = function(response) {
    // translate couch json error messages into js errors
    if (response.status >= 400) {
        if (response.body) {
            var message = JSON.parse(response.body);
            if (message.error === "not_found") throw new error.status[404];
        }
    }
}*/

// NOTES
/*
    _rev: The current revision of this document
    _attachments: If the document has attachments, _attachments holds a data structure, which can also be mapped
    TODO _attachements: create $refs to attachments?
    TODO _deleted: api


  should be available as methods:
    revisions (_revisions, _rev_infos)
    conflicts (_conflicts, _deleted_conflicts) 
*/



//Couch uses multiversion concurrency control by default. This can be overridden on initialization
//directives.previous can be used to attempt to override mvcc effeciently


// couch doc ids cannot begin with an underscore
// top level keys for objects stored in couch cannot begin with an underscore