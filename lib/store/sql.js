/**
 * This is an SQL store that (partially) implements: 
 * http://www.w3.org/TR/WebSimpleDB/
 * and wraps an SQL database engine based on: 
 * based on http://www.w3.org/TR/webdatabase/
 */
var first = require("promised-io/lazy-array").first,
	AutoTransaction = require("../transaction").AutoTransaction,
	parseQuery = require("rql/parser").parseQuery,
	print = require("promised-io/process").print,
	defineProperty = require("commonjs-utils/es5-helper").defineProperty,
	when = require("promised-io/promise").when,
	defer = require("promised-io/promise").defer,
	sqlOperators = require("rql/parser").commonOperatorMap;

var valueToSql = exports.valueToSql = function(value){
    if(value instanceof Array){
        return "(" + value.map(function(element){
            return valueToSql(element);
        }).join(",") + ")";
    }
	return typeof(value) == "string" ? "'" + value.replace(/'/g,"''") + "'" : value + '';
}; 

var safeSqlName = exports.safeSqlName = function(name){
	if(name.match(/[^\w_]/)){
		throw new URIError("Illegal column name " + name);
	}
	return name;
};

try{
	// HACK way to detect rhino
	require('narwhal/narwhal/repl');
	var SQLDatabase = require("../../engines/rhino/lib/store-engine/sql").SQLDatabase;
}catch(e){
	// outside of nodules this may fail
	var SQLDatabase = require("../../engines/node/lib/store-engine/sql").SQLDatabase;
}


exports.SQLStore = function(config){
	var database = config.database || exports.defaultDatabase(config);
	var idColumn = config.idColumn = config.idColumn || "id";
	config.indexPrefix = config.indexPrefix || "idx_";
	var schema;
	var _path;
	
	function buildObject(object) {
		if (!object) return undefined;
		// remove NULLs
		for (var key in object) {
			if (object[key] === null) delete object[key];
		}
		// remove __rownum__ metadata for mssql queries
		delete object.__rownum__;
		object.getMetadata = function() {
			return {
				schema: function() {
					return store;
				}
			}
		};
		return object;
	}
	
	var store = { 
		selectColumns: ["*"],
		get: function(id){
			return when(store.executeSql("SELECT " + store.selectColumns.join(",") + " FROM " + config.table + " WHERE " + idColumn + "=?", [id]), function(result){
				return buildObject(first(result.rows));
			});
		},
		getId: function(object){
			return object[idColumn];
		},
		"delete": function(id){
			return store.executeSql("DELETE FROM " + config.table + " WHERE " + idColumn + "=?", [id]); // Promise
		},
		add: function(object, directives){
			var first = true;
			var valuesPlacement = "";
			var columnsString = "";
			var params = [];
			for(var i in object){
				if(object.hasOwnProperty(i) && typeof object[i] !== "function"){
					params.push(object[i]);
					valuesPlacement += first ? "?" : ",?";
					columnsString += (first ? "" : ",") + i;
					first = false;
				}
			}
			params.idColumn = config.idColumn;
			var results = store.executeSql("INSERT INTO " + config.table + " (" + columnsString + ") values (" + valuesPlacement + ")", params);
			return object.id;
			// FIXME this is broken for mssql dialect
			id = results.insertId;
			object[idColumn] = id;
			return id;
			
		},
		put: function(object, directives){
			var id = directives.id || object[config.idColumn];
			var overwrite = directives.overwrite;
			if(overwrite === undefined){
				overwrite = this.get(id);
			}

			if(!overwrite){
				store.add(object, directives);
			}
			var sql = "UPDATE " + config.table + " SET ";
			var first = true;
			var params = [];
			for(var i in object){
				if(object.hasOwnProperty(i) && typeof object[i] !== "function"){
					if(first){
						first = false;
					}
					else{
						sql += ",";
					}
					sql += i + "=?";
					params.push(object[i]);
				}
			}
			sql += " WHERE " + idColumn + "=?";
			params.push(object[idColumn]);

			return when(store.executeSql(sql, params), function(result){
				return id;
			});
		},
		query: function(query, options){
			options = options || {};
			query = parseQuery(query);
			var limit, count, offset, postHandler, results = true;
			var where = "";
			var select = this.selectColumns;
			var order = [];
			var params = (options.parameters = options.parameters || []);
			function convertRql(query){
				var conjunction = query.name;
				query.args.forEach(function(term, index){
					var column = term.args[0];
					switch(term.name){
						case "eq":
							if(term.args[1] instanceof Array){
								if(term.args[1].length == 0){
									// an empty IN clause is considered invalid SQL
									if(index > 0){
										where += " " + conjunction + " ";
									}
									where += "0=1";
								}
								else{
									safeSqlName(column);
									addClause(column + " IN " + valueToSql(term.args[1]));
								}
                                break;
							}
							// else fall through 
						case "ne": case "lt": case "le": case "gt": case "ge":
							safeSqlName(column);
							addClause(config.table + '.' + column + sqlOperators[term.name] + valueToSql(term.args[1]));
							break;
						case "sort":
							if(term.args.length === 0)
								throw new URIError("Must specify a sort criteria");
							term.args.forEach(function(sortAttribute){
								var firstChar = sortAttribute.charAt(0);
								var orderDir = "ASC";
								if(firstChar == "-" || firstChar == "+"){
									if(firstChar == "-"){
										orderDir = "DESC";
									}
									sortAttribute = sortAttribute.substring(1);
								}
								safeSqlName(sortAttribute);
								order.push(config.table + "." + sortAttribute + " " + orderDir);
							});
							break;
						case "and": case "or":
							where += "(";
							convertRql(term);
							where += ")";
							break;
						case "in":
							print("in() is deprecated");
							if(term.args[1].length == 0){
								// an empty IN clause is considered invalid SQL
								if(index > 0){
									where += " " + conjunction + " ";
								}
								where += "0=1";
							}
							else{
								safeSqlName(column);
								addClause(column + " IN " + valueToSql(term.args[1]));
							}
							break;
						case "select":
							term.args.forEach(safeSqlName);
							select = term.args.join(",");
							break;
						case "distinct":
							select = "DISTINCT " + select;
							break;
						case "count":
							count = true;
							results = false;
							postHandler = function(){
								return count;
							};
							break;
						case "one": case "first":
							limit = term.name == "one" ? 2 : 1;
							postHandler = function(){
								var firstRow;
								return when(results.rows.some(function(row){
									if(firstRow){
										throw new TypeError("More than one object found");
									}
									firstRow = row;
								}), function(){
									return buildObject(firstRow);
								});
							};
							break;
						case "limit":
							limit = term.args[0];
							offset = term.args[1];
							count = term.args[2] > limit; 
							break;
						case "mean":
							term.name = "avg"; 
						case "sum": case "max": case "min":
							select = term.name + "(" + safeSqlName(column) + ") as value";
							postHandler = function(){
								var firstRow;
								return when(results.rows.some(function(row){
									firstRow = row;
								}), function(){
									return firstRow.value;
								});
							};
							break;
						default:
							throw new URIError("Invalid query syntax, " + term.name+ " not implemented");
					}
					function addClause(sqlClause){
						if(where){
							where += " " + conjunction + " ";
						}
						where += sqlClause;
					}
				});
			}
			convertRql(query);
			var structure = {
				select: select,
				where: where,
				from: config.table,
				order: order,
				config: config
			};
			if(count){
				count = when(store.executeSql(store.generateSqlCount(structure)), function(results){
					return first(results.rows).count;
				});
			}
			if(results){
				results = store.executeSql(limit ? store.generateSqlWithLimit(structure, limit, offset || 0) :
					store.generateSql(structure));
			}
			if(postHandler){
				return postHandler();
			}
			return when(results, function(results){
				results = results.rows;
				if(count){
					results.totalCount = count;
					results.length = Math.min(limit, count);
				}
				if(config.type === "mssql") {
					var totalCount = results.totalCount;
					results = results.map(function(item) {
						return buildObject(item);
					});
					results.totalCount = totalCount;
				}
				return results;
			});
		},
		generateSql: function(structure){
			return "SELECT " + structure.select + " FROM " + structure.from +
				(structure.where && (" WHERE " + structure.where)) + (structure.order.length ? (" ORDER BY " + structure.order.join(", ")): "");
		},	
		generateSqlCount: function(structure){
			return "SELECT COUNT(*) as count FROM " + structure.from +
				(structure.where && (" WHERE " + structure.where));
		},	
		generateSqlWithLimit: function(structure, limit, offset){
			return store.generateSql(structure) + " LIMIT " + limit + " OFFSET " + offset;
		},	
		executeSql: function(sql, parameters){
			var deferred = defer();
			var result, error;
			database.executeSql(sql, parameters, function(value){
				deferred.resolve(result = value);
			}, function(e){
				deferred.reject(error = e);
			});
			// return synchronously if the data is already available.
			if(result){
				return result;
			}
			if(error){
				throw error;
			}
			return deferred.promise;
		},
		getSchema: function(){
			return {properties:{}};
		},
		setSchema: function(modelSchema) {
			return schema = modelSchema;
		},
		setPath: function(path) {
			_path = path;
		},
		getPath: function() {
			return _path;
		},
		setIndex: function(column) {
			var sql = "CREATE INDEX " + config.indexPrefix + column + " ON " + config.table + " (" + column + ")";
			print(sql);
			//print( first(this.executeSql(sql).rows) );
			
		},
        transaction: function(){
            return database.transaction();
        }
	};
	var dialect = exports.dialects[config.type];
	for(var i in dialect){
		store[i] = dialect[i]
	}
	for(var i in config){
		store[i] = config[i];
	}
	
	return AutoTransaction(store, database);
}

try{
	var DATABASE = require("commonjs-utils/settings").database;
}catch(e){
	print("No settings file defined for a database " + e);
}

var defaultDatabase;
exports.defaultDatabase = function(parameters){
	parameters = parameters || {};
	for(var i in DATABASE){
		if(!(i in parameters)){
			parameters[i] = DATABASE[i];
		}
	}
	
	if(defaultDatabase){
		return defaultDatabase;
	}
	defaultDatabase = SQLDatabase(parameters);
	require("../transaction").registerDatabase(defaultDatabase);
	return defaultDatabase;
};
exports.openDatabase = function(name){
	throw new Error("not implemented yet"); 	
};
exports.dialects = {
	mysql:{
		getSchema: function(){
			this.startTransaction();
			var results = this.executeSql("DESCRIBE " + config.table, {});
			this.commitTransaction();
			var schema = {properties:{}};
			results.some(function(column){
				schema.properties[column.Field] = {
					"default": column.Default,
					type: [column.Type.match(/(char)|(text)/) ? "string" :
						column.Type.match(/tinyint/) ? "boolean" :
						column.Type.match(/(int)|(number)/) ? "number" :
						"any", "null"]
				};
				if(column.Key == "PRI"){
					schema.links = [{
						rel: "full",
						hrefProperty: column.Field
					}];
				}
			});
			return schema;
		}
	},
	mssql:{
		generateSqlWithLimit: function(structure, limit, offset){
            sql = "SELECT " + structure.select;
            sql += " FROM (SELECT ROW_NUMBER() OVER (ORDER BY ";
            if (structure.order.length) {
                sql += structure.order.join(", ");
            }
            else {
                sql += structure.from + "." + structure.config.idColumn;
            }
            sql += ") AS __rownum__, " + structure.select;
            sql += " FROM " + structure.from;
            sql += structure.where && " WHERE " + structure.where;
            sql += ") AS " + structure.from;
            if (offset)
                sql += " WHERE __rownum__ > " + offset;
            if (limit)
                sql += (offset && " AND" || " WHERE") + " __rownum__ <= " + (limit + offset);
            return sql;
		}
	}
}
