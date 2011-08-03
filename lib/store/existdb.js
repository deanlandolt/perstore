/*

For now this library requires an instance of the eXist XML database, but should
be easy to adapt to any XML database that supports XQuery, XPath Full Text, and
the XQuery Update Facility.

This library depends on the promised-xml library for xml building and parsing.
*/


//var when = require('promised-io/lib/promise').when;
//var request = require('promised-io/lib/http-client').request;
//var settings = require('commonjs-utils/lib/settings');
var when = require('promised-io/promise').when;
var request = require('promised-io/http-client').request;
var streamToString = require('promised-io/lazy-array').streamToString;
var LazyArray = require('promised-io/lazy-array').LazyArray;
var settings = require('commonjs-utils/settings');
var parseQuery = require("rql/parser").parseQuery;
var getModelByPath = require("perstore/model").getModelByPath;


var encodePath = encodeURIComponent;
var decodePath = decodeURIComponent;

// database branding object
function Database(){}

/**
 * The Database object is the primarily an organizational construct for
 * configuration. If unspecified the specified `defaultDatabase` will be used. A
 * database may inherit from another -- if a parent database is unspecified the
 * root Database will be used. Among other configuration options, indexes are
 * specified at this level.
 */
exports.Database = function(config) {
    var db = new Database;
    db.url = config.url || config.parent.url + encodePath(config.name) + '/';
    Object.defineProperty(db, 'collection', {
        get: function() {
            return db.url.match(/\/db\/.*/)[0];
        }
    });
    db.request = function(method, id, body) {
        method = method.toUpperCase();
        id = encodePath(id || '');
        var url = db.url + id;

        if (typeof body == 'string') {
            body = [body];
        }
        else if (typeof body == 'xml') {
            body = [body.toXMLString()];
        }
        else {
            body = body || [];
        }

        if (method == 'GET') {
            // WTF eXist? double-escaping? really?
            url = config.parent.url + encodePath(encodePath(config.name)) + '/' + id;
            body = null;
        }

        // TODO handle response status here?
        return request({
            method: method,
            headers: { 'content-type': 'application/xml; charset=UTF-8' },
            url: url,
            body: body
        });
    };
    return db;
};


var databaseConfig = settings.xmldb && settings.xmldb['default'] || {
    url: 'http://127.0.0.1:8899/exist/rest/db/'
};
exports.defaultDatabase = exports.Database(databaseConfig);

exports.Store = function(config) {
    config = config || {};
    var store = {};
    var _db, _path, _schema;
    var _parent = config.database || exports.defaultDatabase;
    store.setPath = function(path) {
        _path = path;
        _db = exports.Database({parent: _parent, name: _path});
    };
    store.getPath = function() {
        return _path;
    };
    store.get = function(id, directives) {

        /*
        // HACK cannot GET objects where collection has encoded slashes
        var xquery = 'declare namespace obj="http://persvr.org/xmldb/object";';
        xquery += 'collection("' + _db.collection + '")/obj:object[obj:id="' + id + '"]'

        default xml namespace = "http://exist.sourceforge.net/NS/exist";
        var body = <query max="1"><text>{xquery}</text></query>;
        return when(_db.request('post', '', body), function(response) {
            if (response.status === 404) return;
            if (response.status !== 200) throw new Error('db error: get');
            return when(streamToString(response.body), function(body) {
                var result = XML(body);
                for each (var child in result.children()) {
                    return decodeObject(child);
                }
            });
        });
        */

        // FIXME WTF eXist? double-encode collection slashes and encoding case-sensitivity? really?
        return when(_db.request('get', id), function(response) {
            if (response.status === 404) return;
            if (response.status !== 200)
                throw new Error('db error: get '+ response.body.toSource());
            return when(streamToString(response.body), function(body) {
                //print(body.substring(500))
                return decodeObject(body);
            });
        });
    };
    store.put = function(object, directives) {
        directives = directives || {};
        var id = object.id = object.id || directives.id || generateId(id);
        var fullId = typeof directives.getId === 'function' ? directives.getId() : object.id;

        // TODO set id and collections in separate namespace
        var body = encodeObject(object);

        if (false && _schema.properties) for (var key in _schema.properties) {
            var descriptor = _schema.properties[key];
            var content = object[key];
            if (!content || !descriptor.content) continue;
            var format = descriptor.content.type || descriptor.content;

            // if File buffer contents
            // FIXME this is absolutely tragic -- needs Model.resolve
            if (content.$ref && content.$ref.substring(0, 6) === "/File/") {
                var f = File.get(content.$ref.substring(6));
                content = f.getFileHandleChars().read();
            }


            if (format === "xml") {
                // FIXME do we need to decode bytestring?
                if (typeof content.decodeToString === "function") {
                    content = content.decodeToString("utf-8");
                }
            }
            else if (format === "text") {
                // escape
                if (typeof content.decodeToString === "function") {
                    content = content.decodeToString("utf-8");
                }
                content = xmlEscape(content);
            }
            else if (format === "binary") {
                // TODO ... eventually
                // content = base64(content);
            }
            else {
                if (!format)
                    throw new Error("No xmldb content type available");
                throw new Error("Unknown xmldb content type: " + format);
            }

            // TODO add content to `body` xml at <Ref> obj
            //contentContainer[key] = content;
        }

        store.getAllClasses().forEach(function(path) {
            body.appendChild(<sys:class xmlns:sys={sysUri}>{path}</sys:class>);
        });

        return when(_db.request('put', fullId, body), function(response) {
            if (response.status >= 400) {
                throw new Error('PUT fail: ' + status + ', ' + store.getPath() + '/' + id);
            }
            var isNew = !(directives.previous && Object.keys(directives.previous).length > 0);
            return isNew && fullId;
        });
    };
    store.add = function(object, directives) {
        directives = directives || {};
        directives.overwrite = false;
        return store.put(object, directives);
    };
    store['delete'] = function(id) {
        return when(_db.request('delete', id), function(response) {
            if (response.status === 404) return;
            // TODO account for other conditions
            if (response.status !== 200) throw new Error('db error');
        });
    };
    store.query = function(query, directives) {
        directives = directives || {};

        function compile(args) {
            return args.map(function(arg) {
                if (arg.name && arg.args) {
                    return compile[arg.name](arg.args);
                }
                return context(arg);
            }).join(",");
        }
        compile.and = function(args) {
            var ops = [];
            args.forEach(function(arg) {
                var result = arg && compile[arg.name](arg.args);
                if (result) ops.push(result);
            });
            return ops.length && "(" + ops.join(" and ") + ")" || "";
        };
        compile.or = function(args) {
            var ops = [];
            args.forEach(function(arg) {
                var result = arg && compile[arg.name](arg.args);
                if (result) ops.push(result);
            });
            return '(' + ops.join(' or ') + ')';
        };
        compile.eq = function(args) {
            if (args[1] === null || typeof args[1] === 'undefined') {
                return compile.not([{name: 'exists', args: [args[0]]}]);
            }
            return compare('=', args);
        };
        compile.ne = function(args) {
            if (args[1] === null || typeof args[1] === 'undefined') {
                return compile.exists([args[0]]);
            }
            return compare('!=', args);
        };
        compile.lt = function(args) {
            return compare('<', args);
        };
        compile.le = function(args) {
            return compare('<=', args);
        };
        compile.gt = function(args) {
            return compare('>', args);
        };
        compile.ge = function(args) {
            return compare('>=', args);
        };
        compile.sort = function(args) {
            args.forEach(function(sortAttribute) {
                var firstChar = sortAttribute.charAt(0);
                var dir = 'ascending';
                if(firstChar == '-' || firstChar == '+'){
                    if(firstChar == '-'){
                        dir = 'descending';
                    }
                    sortAttribute = sortAttribute.substring(1);
                }
                // TODO check in database's indexedProperties
                // TODO number and other type wrappers?
                order.push(context(sortAttribute) + ' ' + dir);
            });
        };
        // FIXME!!!
        compile.fulltext = function(args) {
            var phrase = args[0];
            if (args.length > 1) {
                // property-faceted fulltext search
                var search = [];
                for (var i = 1; i < args.length; i++) {
                    search.push("cts:contains($d/property::" + obj.prefix + ":" + args[i] + ", cts:word-query('" + phrase + "'))");
                }
                return "(" + search.join(" or ") + ")";
            }
            // FIXME a more elegant way to search through a document and its properties?
            return "(cts:contains($d, cts:word-query('" + phrase + "')) or cts:contains(xdmp:document-properties(base-uri($d)), cts:word-query('" + phrase + "')))";
        };
        compile.fulltext = NYI;
        compile['in'] = NYI;
        compile.contains = function(args) {
            return compile('contains', args);
        };
        compile.count = function(args) {
            wrap.push('count');
        };
        compile.limit = function(args) {
            throw new URIError('"limit" cannot be chained in a query (yet)');
        };
        compile.sum = function(args) {
            // TODO args
            wrap.push('sum');
        };
        compile.mean = function(args) {
            // TODO args
            wrap.push('avg');
        };
        compile.min = function(args) {
            // TODO args
            wrap.push('min');
        };
        compile.max = function(args) {
            // TODO args
            wrap.push('max');
        };
        compile.first = NYI;
        compile.last = NYI;
        compile.one = function(args) {
            return fn('exactly-one', args);
        };
        // FIXME!!!
        compile.select = function(args) {
            throw new URIError("'select' NYI");

            var values = args.map(function(arg) {
                return "$d/property::obj:" + arg
            })
            if (values.length)
                flowr.r = "<select>{(" + values + ")}</select>";
        };
        // FIXME!!!
        compile.values = function(args) {
            throw new URIError("'values' NYI");

            var values = args.map(function(arg) {
                return "<v>{data($d/property::obj:" + arg +  ")}</v>";
            })
            if (values.length)
                flowr.r = "<values>{(" + values + ")}</values>";
        };
        compile.distinct = NYI;
        compile.recurse = NYI;
        compile.aggregate = NYI;

        /// non-standard xquery-specific extensions

        compile.not = function(args) {
            return fn("not", args);
        };
        compile.exists = function(args) {
            return fn("exists", args);
        };
        compile.concat = function(args) {
            return fn("concat", args);
        };
        compile.matches = function(args) {
            // TODO do a checkQuery and gaurd this term
            return fn("matches", args);
        };
        compile.trim = function(args) {
            return fn("normalize-space", args);
        };
        compile.begins = function(args) {
            return fn("starts-with", args);
        };
        compile.ends = function(args) {
            return fn("ends-with", args);
        };
        compile.lowercase = function(args) {
            return fn("lower-case", args);
        };
        compile.uppercase = function(args) {
            return fn("upper-case", args);
        };
        compile.round = function(args) {
            return fn("round", args);
        };
        compile.floor = function(args) {
            return fn("floor", args);
        };
        compile.ceiling = function(args) {
            return fn("ceiling", args);
        };
        compile.abs = function(args) {
            return fn("abs", args);
        };

        function context(prop, prefix) {
            prefix = prefix || 'obj';
            return '$d/' + prefix + ':' + prop;
        }
        function compare(op, args) {
            var name = context(args[0]);
            var val = args[1];
            if (val instanceof Date) val = val.toISOString();
            // FIXME check available indexes in database?
            if (typeof val == 'number') {
                name = 'xs:float(' + name + ')';
            }
            else {
                val = "'" + val + "'";
            }
            return name + ' ' + op + ' ' + val;
        }
        function fn(name, args) {
            return name + '(' + compile(args) + ')';
        }
        function NYI() {
            throw new Error('not yet implemented');
        }

        if (typeof query === 'string') query = parseQuery(query);
        // if query contains a limit pop from list and persist for suffix

        var MAX = 1000000; // FIXME get from settings? can eXist do Infinity?
        var lastArg = query.args.pop();
        var slice = [Infinity, 0];
        if (!lastArg || lastArg.name !== 'limit') {
            query.args.push(lastArg);
        }
        else {
            slice = lastArg.args;
        }

        var offset = Number(slice[1]) || 0;
        var limit = Number(slice[0]) || Infinity;
        var maxLimit = Number(slice[2]) || MAX;
        var order = [];
        var wrap = [];
        //var ret = '$d';
        var ret = '<sys:root><sys:uri>{ base-uri($d) }</sys:uri>{$d}</sys:root>';
        var where = compile[query.name](query.args);

        var xquery = 'declare namespace obj="' + objUri + '";\n';
        xquery += 'declare namespace sys="' + sysUri + '";\n';

        ///xquery += 'for $d in collection("' + _db.collection + '")/obj:object\n';
        ///if (where) xquery += 'where ' + where + '\n';
        var flowr = 'for $d in collection("/db/")/obj:object\n';
        flowr += 'where $d//sys:class = "' + _path + '"\n';
        if (where) flowr += 'and ' + where + '\n';

        if (order.length) flowr += 'order by ' + order.join(',\n    ') + '\n';
        flowr += 'return ' + ret + '\n';
        wrap.forEach(function(fn) {
            flowr = fn + '(\n' + flowr + '\n)';
        });

        xquery += flowr;

        //print('XQUERY:\n' + xquery);

        //var existNS = new Namespace('http://exist.sourceforge.net/NS/exist');
        var body = <exist:query xmlns:exist="http://exist.sourceforge.net/NS/exist">
            <exist:text>{xquery}</exist:text>
        </exist:query>;
        if (offset) body.@start = offset + 1;
        if (limit || maxLimit) body.@max = Math.min(limit, maxLimit);

        return when(_db.request('post', '', body), function(response) {

            if ([200, 202].indexOf(response.status) < 0) {
                throw new Error('db error: status ' + response.status);
            }
            // TODO stream properly

            return when(streamToString(response.body), function(body) {
                // FIXME regex to find obj element boundaries
                body = body.replace(/<\?.*?\?>\s*/, '');
                var docs = XML(body);

                // TODO should use docs.value.@type...why isn't it working?
                if (('' + docs.value).length) {
                    // FIXME
                    return Number(docs.value.toString());
                }

                var match = body.match(/^\s*<exist:result([^>]*)>/)
                if (!match) {
                    print('match:' + body)
                    print(xquery)
                    throw new Error('db error: match');
                }
                body = body.substring(match[0].length, body.lastIndexOf('</exist:result>'));
                match = match[1];

                var totalCount = Number(match.match(/ exist:hits="(\d+)"/)[1]);
                var length = Number(match.match(/ exist:count="(\d+)"/)[1]);
                var offset = Number(match.match(/ exist:start="(\d+)"/)[1]) - 1;
                if (totalCount && offset && totalCount !== offset) {
                    directives.range = [length, offset, totalCount]
                }

                directives.offset = offset; // HACK
                return LazyArray({
                    some: function(write){
                        for each (var doc in docs.children()) {
                            write(decodeObject(doc));
                        }
                    },
                    totalCount: totalCount,
                    length: length,
                    schema: function() { return _schema }
                });


            });

            // TODO stream
            /*var buffer = '';
            var head;
            var promise = response.body.forEach(function(chunk) {
                buffer += chunk;
                if (!head) {
                    head = buffer.match(/^\s*<exist:result ([^>])>/)
                    if (!head) return;
                    var attrs = head[1];
                    head = {};
                    head.
                }
            });*/
        });
    };

    store.setSchema = function(schema) {
        return _schema = schema;
    };

    store.getAllClasses = function() {
        var paths = [store.getPath()];
        var parent = _schema;
        while (true) {
            parent = parent["extends"];
            if (!parent) break;
            paths.unshift(parent.getPath());
        }
        return paths;
    };

    // TODO move these to config and rework them
    var objUri = 'http://persvr.org/xmldb/object';
    var objNS = new Namespace(objUri);
    var sysUri = 'http://persvr.org/xmldb/system';
    var sysNS = new Namespace(sysUri);

    return store;

    function decodeObject(xml) {
        //default xml namespace = objUri;
        try {
                xml = XML(xml);
        }
        catch (e) {
            print('bad parse: ' + xml);
            throw new Error('Bad Parse from XMLDB');
        }

        var uri = xml.sysNS::uri.toString();
        var model;
        var id;

        if (uri) {
            // query result
            var parts = uri.split('/');
            if (parts.length !== 4) throw new URIError('wtf? ' + uri);
            uri = decodeURIComponent(parts[2]);
            id = decodeURIComponent(parts[3]);
            model = getModelByPath(uri);
            xml = xml.objNS::object;
        }
        else {
             // TODO cache
            uri = store.getPath();
            model = getModelByPath(uri);
        }

        var object = {};
        for each (var child in xml.children()) {
            // skip all children not in obj namespace
            if (child.namespace() != objNS) continue;
            var result = decodeNode(child);
            object[result[0]] = result[1];
        }

        uri = '/' + uri + '/' + (id || object.id);

        Object.defineProperty(object, "getMetadata", {
            value: function() {
                return {
                    // FIXME not yet defined in exist
                    //"last-modified": lastModified,
                    // FIXME only add content-location if document uri is different model/id?
                    'content-location': uri,
                    schema: function() { return model; }
                    /*,
                    // TODO proper rev and ETag
                    getRevision: function() {
                        return properties.propNamespace::revision.toString()
                    }
                    etag: this.getRevision() + "//" + uri*/
                }
            },
            enumerable: false,
            writable: true
        });

        return Object.keys(object).length ? object : undefined;

        function decodeNode(node) {
            var key = node.name().localName;
            var type = node.@objNS::type.toString();
            // HACK remove once we clear out one more time
            if (!type) type = node.@type.toString();
            var value = node.toString();

            // first handle arrays
            if (key === 'Array') {
                key = node.@objNS::key.toString();

                // HACK remove once we clear out one more time
                if (!key) key = node.@key.toString();

                var array = [];
                for each (var child in node.children()) {
                    array.push(decodeNode(child)[1]);
                }
                return [key, array];
            }
            else if (!node.hasComplexContent()) {
                return [key, coerce(value, type)];
            }
            else if (type === 'xml') {
                var xml = node.children().toXMLString();
                return [ key, { $xml: handleInlineXml(xml) } ];
            }

            var object = {};
            var uri = node.objNS::Ref.@objNS::uri.toString();
            if (uri) {
                object.$ref = uri;
                node = node.objNS::Ref;
            }
            for each (var child in node.children()) {
                var result = decodeNode(child);
                object[ result[0] ] = result[1];
            }
            return [key, object];
        }

        function handleInlineXml(xml) {
            xml = xml.replace(/\s*<\?.*?\?>\s*/g, '');
            var match = xml.match(/^\s*<([^>]+)>/);
            // FIXME which error?
            if (!match) throw new TypeError('Malformed xml');

            xml = xml.substring(match[0].length);
            var parts = match[1].split(/ /g);
            var dataNs;
            for (var i = 1; i < parts.length; i++) {
                var part = parts[i];
                // HACK strip persvr xmldb namespaces
                var persvrNsRe = /^xmlns(:\w+)?="http:\/\/persvr.org\/xmldb\/(object|system)"$/;
                if (persvrNsRe.test(part)) {
                    part = '';
                    continue;
                }

                // HACK test for http://data.dscs.com/schema ns
                var dataNsRe = /^xmlns(:\w+)?="http:\/\/data.dscs.com\/schema\/"$/;
                if (dataNsRe.test(part)) dataNs = true;
            }
            // HACK add default http://data.dscs.com/schema ns if not present
            if (!dataNs) {
                parts.splice(1, 0, 'xmlns="http://data.dscs.com/schema"');
            }
            return '<' + parts.join(' ') + '>' + xml;
        }

    }

    function coerce(value, type) {
        if (type === 'boolean') {
            return value === 'true';
        }
        else if (type === 'number') {
            return Number(value);
        }
        else if (type === 'date') {
            return new Date(Date.parse(value));
        }
        else if (type === 'null') {
            return null;
        }
        else if (type === 'undefined') {
            return undefined;
        }
        else if (type === 'function') {
            var func;
            try {
                func = eval(value);
            }
            catch (e) {
                func = function() {
                    throw 'DeserializationError: ' + e;
                }
            }
            return func;
        }
        else if (type === 'NaN') {
            return NaN;
        }
        else if (type === 'Infinity') {
            return Infinity;
        }
        else if (type === '-Infinity') {
            return -Infinity;
        }
        else if (type === 'regex') {
            var parts = value.split('/');
            return RegExp(parts.slice(1,-1).join('/'), parts.slice(-1)[0]);
        }
        else if (type === 'object') {
            return {};
        }
        return value;
    }


    function encodeObject(object) {
        //default xml namespace = objUri;
        var doc = <obj:object xmlns:obj={objUri} xmlns:sys={sysUri}/>;
        Object.keys(object).forEach(function(key) {
            doc[encodeElementName(key)] = encodeNode(key, object[key]);
        });
        return doc;

        function encodeNode(name, value) {
            var type = typeof value;
            if (type === 'object' || type === 'function') {
                if (value === null) {
                    return build(name, 'null', value);
                }
                else if (value instanceof Date) {
                    return build(name, 'date', value.toISOString());
                }
                else if (value instanceof RegExp) {
                    return build(name, 'regex', value);
                }
                else if (type === 'function') {
                    return build(name, type, value);
                }

                else if (typeof value.forEach === 'function') {
                    var array = <obj:Array obj:key={name} xmlns:obj={objUri}/>;
                    value.forEach(function(item) {
                        array.appendChild(encodeNode(name, item));
                    });
                    return array;
                }
                else if (value.$xml) {
                    return build(name, 'xml', XML(value.$xml));
                }

                var node = <obj:Ref xmlns:obj={objUri}/>;
                var uri = value.$ref;
                for (var key in value) {
                    if (value.hasOwnProperty(key)) {
                        if (key === '$ref') {
                            node.@objNS::uri = uri || '';
                        }
                        else {
                            node.appendChild(encodeNode(key, value[key]));
                        }
                    }
                }
                return build(name, type, uri ? node : node.children());
            }
            else if (type === 'undefined') {
                return build(name, type, value);
            }
            return build(name, type, value)
        }

        function build(name, type, value) {
            // TODO escape $ and other special chars (and unescape on decode)
            return <obj:{name} obj:type={type} xmlns:obj={objUri}>{value}</obj:{name}>;
        }

        function encodeElementName(value) {
            // TODO
            return value;
        }
    }

};


function generateId() {
    return (''+Math.random()).substring(2);
};


/*

import module namespace v="http://exist-db.org/versioning";

v:doc(doc("/db/legal%2Fus%2Ffed%2Flegislative%2Fcrs%2Fbill-digest%2Frecord-history/112_HR_802~2011-02-28"), 1242)

*/