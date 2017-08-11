var redis = require('./redis'),
		moment = require('moment');
		momentTZ = require('moment-timezone');
		async = require('async');

var noop = function(){};

exports.version = '2.0.0';

exports.redis = redis;
exports.Score = Score;


/**
 * Initialize a 'Score'
 **/
function Score() {
	this.client = redis.createClient();
}

/**
 * Add a score to the scoreboard, appends if key exists
 * @param {String} key
 * @param {Number} score
 * @param {String} value
 **/
Score.prototype.index = function(key, score, value, callback) {

	let db = this.client;
	// tzone = timezone || "Africa/Lagos";
	// timeKey = exports.genKey(key.toLowerCase(), momentTZ.tz(new Date(), tzone)),
	key = key.toLowerCase();
	
	async.parallel([
		// overall insert
		function(done){
			db.exists(key, function(error, exist) {
				if(!exist) {
					console.log('Onetime leaderboard: inserting new HiScore: ' + score + ' for user ' + value );					
					db.zadd(key, score, value, done);	
				} else {
					db.zscore(key, value, function(err, hiScore) {
						console.log('Onetime leaderboard current HiScore: ' + hiScore + ', new score: ' + score + ' for user ' + value );				
						if(score > hiScore) {
							//console.log('Alltime leaderboard: replacing highscore for user ' + value );
							db.zrem(key, value, callback || noop);
							db.zadd(key, score, value, done);
						}
					});
				}
			});
		}
	], callback || noop);

	return this;
};

/**
 * Remove a score from the scoreboard
 * @param {String} key
 * @param {String} value
 **/
Score.prototype.remove = function(key, value, callback) {
	let db = this.client;
	
	db.zrem(key, value, callback || noop);
};

/**
 * Get scoreboard order by leader first
 * @param {String | Object} key
 * @param {Number} start
 * @param {Number} max
 **/
Score.prototype.leader = function(conditions, callback) {
	this.query = new Query(conditions, this).type('leader');
	return this.query.find(callback);	
};

/**
 * Get scoreboard order by leader first
 * @param {String | Object} key
 * @param {Number} start
 * @param {Number} max
 **/
Score.prototype.rank = function(conditions, callback) {
	this.query = new Query(conditions, this).type('rank');
	return this.query.find(callback);	
};

/**
 * Get scoreboard order by leader first
 * @param {String | Object} key
 * @param {Number} start
 * @param {Number} max
 **/
Score.prototype.count = function(conditions, callback) {
	this.query = new Query(conditions, this).type('count');
	return this.query.find(callback);	
};

/**
 * Get scoreboard order by leader first
 * @param {String | Object} key
 * @param {Number} start
 * @param {Number} max
 **/
Score.prototype.around = function(conditions, callback) {
	this.query = new Query(conditions, this).type('around');
	return this.query.find(callback);	
};


/**
 * Initialize a Query
 **/
function Query(conditions, score) {
	this.conditions = conditions;
	this.keys = conditions.keys;
	this.start = 0;
	this.end = -1;
	this.player = conditions.player;
	this.score = score;
}

Query.prototype.type = function(type) {
	this.type = type;
	return this;
};

Query.prototype.find = function(callback) {
	this.callback = callback;
	return this;
};

Query.prototype.in = function(keys) {
	this.keys = keys;
	return this;
};

Query.prototype.limit = function(limit) {
	this.limit = limit;
	return this;
};

Query.prototype.skip = function(skip) {
	this.skip = skip;
	return this;	
}

Query.prototype.player = function(player) {
	this.player = player;
	return this;	
}

Query.prototype.run = function(callback) {
	var type = this.type;
	this[type](callback);
};

Query.prototype.leader = function(callback) {
	var keys = [];

	if(this.conditions.date &&
		this.conditions.date.$start && 
		this.conditions.date.$end) {

		console.log('start date: ' + this.conditions.date.$start + ', end date: ' + this.conditions.date.$end);
		
		keys = exports.getRangeKeys(this.keys, this.conditions.date.$start, this.conditions.date.$end);
		console.log('keys: ' + keys);
	} else if (this.keys.length == 1) {
		keys = this.keys;
	} else {
		keys = this.keys;
	}

	if(keys.length == 1) {
		console.log('calling rev range');
		this.revrange(keys[0], this.skip, this.limit, callback);
	} else {
		console.log('calling multirev range');
		this.multirevrange(keys, this.skip, this.limit, callback);
	}
};

Query.prototype.revrange = function(key, start, end, callback) {
	var db = this.score.client;

	var s = (start > 0) ? start : 0;
	var e = (end > 0) ? ((parseInt(start) + parseInt(end)) - 1) : -1;

	db.zrevrange(key, s, e, 'withscores', function(err, res) {
		console.log('score reply: ' + res);
		callback(err, res);
	});
};

Query.prototype.multirevrange = function(keys, start, end, callback) {
	var db = this.score.client,
		keys = keys,
		multikey = exports.genUniqueKey(keys);

	var s = (start > 0) ? start : 0;
	var e = (end > 0) ? ((parseInt(start) + parseInt(end)) - 1) : -1;

	var multi = db.multi();
	multi.zunionstore([multikey, keys.length].concat(keys).concat(['aggregate', 'max']));
	multi.zrevrange(multikey, s, e, 'withscores'); 
	multi.exec(function(err, replies) {
		console.log('replies: ' + replies);
		callback(err, replies[1]);
		db.del(multikey);
	});
};

Query.prototype.rank = function(callback) {
	var keys = [];

	if(this.conditions.date &&
		this.conditions.date.$start && 
		this.conditions.date.$end) {

		//console.log('start date: ' + this.conditions.date.$start + ', end date: ' + this.conditions.date.$end);
		
		keys = exports.getRangeKeys(this.keys, this.conditions.date.$start, this.conditions.date.$end);
		//console.log('keys: ' + keys);
	} else if (this.keys.length == 1) {
		keys = this.keys;
	} else {
		keys = this.keys;
	}

	//console.log("Player is: " + this.player);

	if(keys.length == 1) {
		//console.log('calling rev range');
		this.revrank(keys[0], this.player, callback);
	} else {
		//console.log('calling multirev range');
		this.multirevrank(keys, this.player, callback);
	}
};

Query.prototype.revrank = function(key, player, callback) {
	var db = this.score.client;

	//Add one because the results are 0 based
	db.zrevrank(key, player, function(err, res) {
		//console.log('rank: ' + res);
		callback(err, res + 1);
	});
};

Query.prototype.multirevrank = function(keys, player, callback) {
	var db = this.score.client,
		keys = keys,
		multikey = exports.genUniqueKey(keys);

	var multi = db.multi();
	multi.zunionstore([multikey, keys.length].concat(keys).concat(['aggregate', 'max']));
	multi.zrevrank(multikey, player); 
	multi.exec(function(err, replies) {
		//Add one because the results are 0 based
		callback(err, replies[1] + 1);
		//console.log('multi rank: ' + (replies[1] + 1));
		db.del(multikey);
	});
};

Query.prototype.count = function(callback) {
	var keys = [];

	if(this.conditions.date &&
		this.conditions.date.$start && 
		this.conditions.date.$end) {

		//console.log('start date: ' + this.conditions.date.$start + ', end date: ' + this.conditions.date.$end);
		
		keys = exports.getRangeKeys(this.keys, this.conditions.date.$start, this.conditions.date.$end);
		//console.log('keys: ' + keys);
	} else if (this.keys.length == 1) {
		keys = this.keys;
	} else {
		keys = this.keys;
	}

	if(keys.length == 1) {
		//console.log('calling rev range');
		this.zcount(keys[0], callback);
	} else {
		//console.log('calling multirev range');
		this.zmulticount(keys, callback);
	}
};

Query.prototype.zcount = function(key, callback) {
	var db = this.score.client;

	//Add one because the results are 0 based
	db.zcount(key, -Infinity, +Infinity, function(err, res) {
		//console.log('count: ' + res);
		callback(err, res);
	});
};

Query.prototype.zmulticount = function(keys, callback) {
	var db = this.score.client,
		keys = keys,
		multikey = exports.genUniqueKey(keys);

	var multi = db.multi();
	multi.zunionstore([multikey, keys.length].concat(keys).concat(['aggregate', 'max']));
	multi.zcount(multikey, -Infinity, +Infinity); 
	multi.exec(function(err, replies) {
		//Add one because the results are 0 based
		callback(err, replies[1]);
		//console.log('multi count: ' + replies);
		db.del(multikey);
	});
};