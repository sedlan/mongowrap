/**
 * Module dependencies
 */

var uuid = require('node-uuid');
var mongo = require('mongodb');
var MongoClient = mongo.MongoClient;

exports = module.exports = MongoWrap;

function MongoWrap(params) {
	if (!params) {
		throw new Error('Missing params argument');
	}
	if (!params.unauthenticatedUser) {
		throw new Error('Missing params.unauthenticatedUser argument');
	}
	if (!params.unauthenticatedUser.username) {
		throw new Error('Missing params.unauthenticatedUser.username argument');
	}
	if (!params.unauthenticatedUser.password) {
		throw new Error('Missing params.unauthenticatedUser.password argument');
	}
	// Initialize database
	console.log('Initializing MongoDB');
	this.host = params.host || 'localhost';
	this.port = params.port || mongo.Connection.DEFAULT_PORT;
	this.dbName = params.dbName || 'test';
	username = params.username || null;
	password = params.password || null;

	var connectionString = 'mongodb://';
	if (username && password) {
		connectionString += username + ':' + password + '@';
	}
	connectionString += this.host + ':' + this.port + '/' + this.dbName;
	this.connectionString = connectionString;
	this.db = null;
	this.anyUser = params.unauthenticatedUser;
}

MongoWrap.prototype.connect = function(callback) {
	if (!callback) {
		throw new Error('Missing callback argument');
	}
	var self = this;
	console.log('Connecting to MongoDB');

	MongoClient.connect(self.connectionString, function(err, db) {
	    if (err) {
	    	throw err;
	    }
    	self.db = db;

    	// create a db user to allow unauthenticated calls to the API
    	var admin = db.admin();
    	admin.listDatabases(function(err, results) {
    		if (!err && results && results.databases) {
    			var dbs = results.databases;
    			var found = false;
    			for (var i = 0, l = dbs.length; i < l; i++) {
    				var one = dbs[i];
    				if (one && one.name) {
    					if (one.name === self.dbName) {
    						found = true;
    						break;
    					}
    				}
    			}
    			if (!found || (found && one.empty)) {
    				var user = {
    					_id: uuid(),
    					username: self.anyUser.username,
    					password: self.anyUser.password
    				}
    				db.collection('users').insert(user, {safe:true}, function(err, objects) {
    					callback();
    				});
    			} else {
    				callback();
    			}
    		}
    	});
	});
};

MongoWrap.prototype.count = function(/* name[, query][, options], callback */) {
	var args = Array.prototype.slice.call(arguments, 0);
	if (args.length < 2) {
		throw new Error('Name and callback arguments are required');
	}
	// last argument is callback
	var callback = args.pop();
	// first argument (other than a callback) is collection name
	var name = args.shift();
	var query = args.length ? args.shift() || {} : {};
	var options = args.length ? args.shift() || {} : {};

	var collection = this.db.collection(name);
	if (query && query.id) {
		query._id = query.id;
		delete query.id;
	}
	collection.count(query, options, callback);
};

MongoWrap.prototype.find = function(/* name, query[, options], callback */) {
	var args = Array.prototype.slice.call(arguments, 0);
	if (args.length < 3) {
		throw new Error('Name, query and callback arguments are required');
	}
	// last argument is callback
	var callback = args.pop();
	// first argument (other than a callback) is collection name
	var name = args.shift();
	// query is second
	var query = args.shift() || {};
	var options = args.length ? args.shift() || {} : {};

	var collection = this.db.collection(name);
	if (query && query.id) {
		query._id = query.id;
		delete query.id;
	}
	collection.find(query, options).toArray(function(err, docs) {
		if (!err && docs) {
			for (var i = 0, l = docs.length; i < l; i++) {
				if (docs[i]._id) {
					docs[i].id = docs[i]._id;
					delete docs[i]._id;
				}
			}
		}
		callback(err, docs);
	});
};

MongoWrap.prototype.get = function(/* name, id, callback */) {
	var args = Array.prototype.slice.call(arguments, 0);
	if (args.length < 3) {
		throw new Error('Name, id and callback arguments are required');
	}
	var callback = args.pop();
	var name = args.shift();
	var id = args.shift();

	var collection = this.db.collection(name);
	collection.findOne({'_id': id}, function(err, item) {
		if (item && item._id) {
			item.id = item._id;
			delete item._id;
		}
		callback(err, item);
	});
};

MongoWrap.prototype.put = function(/* name, id, data, callback */) {
	var args = Array.prototype.slice.call(arguments, 0);
	if (args.length < 4) {
		throw new Error('Name, id, data and callback arguments are required');
	}
	var callback = args.pop();
	var name = args.shift();
	var id = args.shift();
	var data = args.shift();

	if (data.id) {
		delete data.id;
	}
	data._id = id;

	var collection = this.db.collection(name);
	collection.update({'_id': id}, data, {safe:true}, function(err) {
		callback(err);
	});
};

MongoWrap.prototype.post = function(/* name, data, callback */) {
	var args = Array.prototype.slice.call(arguments, 0);
	if (args.length < 3) {
		throw new Error('Name, data and callback arguments are required');
	}
	var callback = args.pop();
	var name = args.shift();
	var data = args.shift();

	if (data instanceof Array) {
		for (var i = 0, l = data.length; i < l; i++) {
			if (data[i].id) {
				var id = data[i].id;
				delete data[i].id;
			} else {
				id = uuid();
			}
			data[i]._id = id;
		}
	} else {
		if (data.id) {
			id = data.id;
			delete data.id;
		} else {
			id = uuid();
		}
		data._id = id;
	}

	var collection = this.db.collection(name);
	collection.insert(data, {safe:true}, function(err, objects) {
		if (objects instanceof Array) {
			for (var i = 0, l = objects.length; i < l; i++) {
				if (objects[i] && objects[i]._id) {
					objects[i].id = objects[i]._id;
					delete objects[i]._id;
				}
			}
		} else {
			if (objects && objects._id) {
				objects.id = objects._id;
				delete objects._id;
			}
		}
		callback(err, objects);
	});
};

MongoWrap.prototype.del = function(/* name, id, callback */) {
	var args = Array.prototype.slice.call(arguments, 0);
	if (args.length < 3) {
		throw new Error('Name, id and callback arguments are required');
	}
	var callback = args.pop();
	var name = args.shift();
	var id = args.shift();

	var collection = this.db.collection(name);
	collection.remove({_id: id}, function(err) {
		callback(err);
	});
};
