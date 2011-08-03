var mongodb = require('narwhal-mongodb/mongodb');
var rql = require("rql/parser");
var JSONExt = require("commonjs-utils/json-ext");
var LazyArray = require('promised-io/lazy-array').LazyArray;


function mongoEncode(s) { return s.replace(/%/g, '%25').replace(/\$/g, '%24') }
function mongoDecode(s) { return s.replace(/%24/g, '$').replace(/%25/g, '%') }

function patchKeys(target, patch, schema) {
    if (!target || typeof target != 'object') return target;
    if (target instanceof Date || target instanceof RegExp) return target;
    if (Array.isArray(target)) {
        return target.map(function(i) {
            return patchKeys(i, patch, schema);
        });
    }
    var object = {};
    Object.keys(target).forEach(function(key) {
        object[patch(key)] = patchKeys(target[key], patch, schema);
    });
    return object;
}


if (typeof MONGO_SERVER === 'undefined') {
    MONGO_SERVER = new mongodb.Mongo();
    MONGO_SERVER._mongo.setWriteConcern(com.mongodb.WriteConcern.SAFE);
}


exports.Store = function(config) {
    config = config || {};
    var _dbName = config.db || 'persvr';
    var _path;
    var _schema;
    var _collection;
    var _db = MONGO_SERVER.getDB(_dbName);
    return {
        getPath: function() {
            return _path;
        },
        setPath: function(path) {
            _path = path;
            //_collection = Collection(_dbName, path);
            _collection = _db.getCollection(path);
            return path;
        },
        setSchema: function(schema) {
            return _schema = schema;
        },
        get: function(id, directives) {
            var result = _collection.findOne(id);
            if (!result) return;
            result = JSONExt.parse(_collection._stringify(result));

            result.id = result._id;
            delete result._id;
            Object.keys(result).forEach(function(key) {
                if (key.indexOf('_') == 0) delete result[key];
            });
            return patchKeys(result, mongoDecode);
        },
        // TODO _collection.ensureIndex({timestamp:-1})
        query: function(query, directives) {
            // HACK WTF is going on with query encoding?
            query = query.toString()
                .replace(/epoch%3A/gi, 'epoch:')
                .replace(/sort\(%2B/gi, 'sort(+');

            if (typeof query == "string") query = rql.parseQuery(query);
            //print('QUERY:' + query)
            // change id to _id
            var _parsed = parse(query, directives);
            var options = _parsed[0];
            var search = _parsed[1];

            if (options.limit <= 0) return [];
            //print('SEARCH:' + search.toSource())
            // HACK for dates and id
            Object.keys(search).forEach(function(key) {
                var val = search[key];
                if (val instanceof Date) search[key] = val.toISOString();
                if (key === 'id') {
                    search._id = search.id;
                    delete search.id;
                }
            });




            // TODO options
            var cursor = _collection.find(search);

            if (options.sort) cursor.sort(options.sort);
            if (options.skip > 0) cursor.skip(options.skip);
            if (options.limit < Infinity) cursor.limit(options.limit);

            if (options.scalar) return cursor[options.scalar]();


            /*return {
                totalCount: cursor.count(),
                //length: cursor.size(),
                forEach: function(write) {
                    while (cursor.hasNext()) {
                        var result = JSONExt.parse(_collection._stringify(cursor.next()));
                        result.id = result._id;
                        delete result._id;
                        Object.keys(result).forEach(function(key) {
                            if (key.indexOf('_') == 0) delete result[key];
                        });
                        write(patchKeys(result, mongoDecode));
                    }
                    //cursor.close(); FIXME!!
                }
            };*/

            var count = cursor.count();
            var array = cursor.toArray();
            var len = array.length;
            return LazyArray({
                some: function(write) {
                    for (var i = 0; i < len; i++) {
                        var result = JSONExt.parse(_collection._stringify(array[i]));
                        result.id = result._id;
                        delete result._id;
                        Object.keys(result).forEach(function(key) {
                            if (key.indexOf('_') == 0) delete result[key];
                        });
                        write(patchKeys(result, mongoDecode));
                    }
                    cursor.close();
                },
                totalCount: count,
                length: len,
                schema: function() { return _schema }
            });

        },
        put: function(object, directives){
            directives = directives || {};
            var id = object.id || directives.id || generateId();
            object = JSON.parse(JSON.stringify(object));
            object = patchKeys(object, mongoEncode);
            delete object.id;
            object._id = id;

            // TODO capture result somehow?
            //if (directives.overwrite === true) {
            //    _collection.insert(object);
            //}
            //else if (directives.overwrite === false) {
            //    _collection.update(id, object);
            //}
            //else {
                var updatedExisting = _collection.save(object);
            //}

            return !updatedExisting && id;
            // TODO bulk update based on query?
        },
        'delete': function(id, directives) {
            // TODO query-based delete
            return _collection.remove(id);
        },
        // TODO drop
        getTotal: function() {
            _collection.getCount();
        }
    }
};

function generateId() {
    return Math.random().toString().substring(2);
}

function parse(query, directives){
    var options = {
        skip: 0,
        limit: Infinity,
        lastSkip: 0,
        lastLimit: Infinity
    };

    var search = {};
    function walk(name, terms) {
        // valid funcs
        var valid_funcs = ['lt','lte','gt','gte','ne','in','nin','not','mod','all','size','exists','type','elemMatch'];
        // funcs which definitely require array arguments
        var requires_array = ['in','nin','all','mod'];
        // funcs acting as operators
        var valid_operators = ['or', 'and'];//, 'xor'];
        // compiled search conditions
        var search = {};
        // iterate over terms
        terms.forEach(function(term){
            var func = term.name;
            var args = term.args;
            // ignore bad terms
            // N.B. this filters quirky terms such as for ?or(1,2) -- term here is a plain value
            if (!func || !args) return;
            //dir(['W:', func, args]);
            // process well-known functions
            // http://www.mongodb.org/display/DOCS/Querying
            if (func == 'sort' && args.length > 0) {
                options.sort = {};
                args.forEach(function(sortAttribute){
                    var firstChar = sortAttribute.charAt(0);
                    var orderDir = 1;
                    if (firstChar == '-' || firstChar == '+') {
                        if (firstChar == '-') orderDir = -1;
                        sortAttribute = sortAttribute.substring(1);
                    }
                    options.sort[sortAttribute] = orderDir;
                });
            }
            else if (func == 'select') {
                options.fields = args;
            }
            else if (func == 'values') {
                options.unhash = true;
                options.fields = args;
            // N.B. mongo has $slice but so far we don't allow it
            /*} else if (func == 'slice') {
                //options[args.shift()] = {'$slice': args.length > 1 ? args : args[0]};*/
            }
            else if (func == 'limit') {
                // we calculate limit(s) combination
                options.lastSkip = options.skip;
                options.lastLimit = options.limit;
                // TODO: validate args, negative args
                var l = args[0] || Infinity, s = args[1] || 0;
                // N.B: so far the last seen limit() contains Infinity
                options.totalCount = args[2];
                if (l <= 0) l = 0;
                if (s > 0) options.skip += s, options.limit -= s;
                if (l < options.limit) options.limit = l;
//dir('LIMIT', options);
            // grouping
            }
            else if (func == 'group') {
                // TODO:
            // nested terms? -> recurse
            }
            else if (func == 'count') {
                options.scalar = 'count';
            }
            else if (args[0] && typeof args[0] === 'object') {
                if (valid_operators.indexOf(func) > -1)
                    search['$'+func] = walk(func, args);
                // N.B. here we encountered a custom function
                // ...
            // structured query syntax
            // http://www.mongodb.org/display/DOCS/Advanced+Queries
            }
            else {
                //dir(['F:', func, args]);
                // mongo specialty
                if (func == 'le') func = 'lte';
                else if (func == 'ge') func = 'gte';
                // the args[0] is the name of the property
                var key = args.shift();
                // the rest args are parameters to func()
                // FIXME: do we really need to .join()?!
                if (requires_array.indexOf(func) == -1)
                    args = args.length == 1 ? args[0] : args.join();
                // regexps:
                if (typeof args === 'string' && args.indexOf('re:') === 0)
                    args = new RegExp(args.substr(3), 'i');
                // regexp inequality means negation of equality
                if (func == 'ne' && args instanceof RegExp) {
                    func = 'not';
                }
                // TODO: contains() can be used as poorman regexp
                // E.g. contains(prop,a,bb,ccc) means prop.indexOf('a') >= 0 || prop.indexOf('bb') >= 0 || prop.indexOf('ccc') >= 0
                //if (func == 'contains') {
                //  // ...
                //}
                // valid functions are prepended with $
                if (valid_funcs.indexOf(func) > -1) {
                    func = '$'+func;
                }
                // $or requires an array of conditions
                // N.B. $or is said available for mongodb >= 1.5.1
                if (name == 'or') {
                    if (!(search instanceof Array))
                        search = [];
                    var x = {};
                    x[func == 'eq' ? key : func] = args;
                    search.push(x);
                // other functions pack conditions into object
                } else {
                    // several conditions on the same property is merged into one object condition
                    if (search[key] === undefined)
                        search[key] = {};
                    if (search[key] instanceof Object && !(search[key] instanceof Array))
                        search[key][func] = args;
                    // equality cancels all other conditions
                    if (func == 'eq')
                        search[key] = args;
                }
            }
        // TODO: add support for query expressions as Javascript
        });
        return search;
    }
    //print(['Q:',query]);
    search = walk(query.name, query.args);
    //print(['S:',search.toSource()]);
    return [options, search];
}



/*

use persvr

var wf = db['workflow/process'];
wfh.ensureIndex({timestamp: -1});
wfh.ensureIndex({workflowType: 1, profileId: 1, state: 1});

wfh.ensureIndex({assigned: 1});
wfh.ensureIndex({nextRun: 1});
wfh.ensureIndex({timestamp: -1});


var wfh = db['workflow/history'];
wfh.ensureIndex({timestamp: -1});
wfh.ensureIndex({workflowType: 1, profileId: 1, state: 1});

wfh.ensureIndex({pid: 1});


var cf = db['collection/file'];
cf.ensureIndex({filename: 1});
cf.ensureIndex({profileId: 1});
cf.ensureIndex({sourceDate: 1});
cf.ensureIndex({size: 1});
cf.ensureIndex({type: 1});


*/
