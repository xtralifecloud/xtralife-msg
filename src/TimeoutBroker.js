/*
 * decaffeinate suggestions:
 * DS002: Fix invalid constructor
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const Promise = require('bluebird');

const Broker = require('./NormalBroker.js');

// TODO OSS : redis is actually promisified in xtralife... we could use promises instead of ugly defer

class TimeoutBroker extends Broker {
	constructor(prefix, redis, pubsub, timeoutHandler, checkInterval, ackTimeout, key){
		super(prefix, redis, pubsub, key);
		this.timeoutHandler = timeoutHandler;
		if (checkInterval == null) { checkInterval = 15000; }
		this.checkInterval = checkInterval;
		if (ackTimeout == null) { ackTimeout = 15000; }
		this.ackTimeout = ackTimeout;
		if (key == null) { key = "Broker:channel-" + prefix; }
		this.prefix = prefix;
		this.key = key;
		this.redis = redis;
		this.pubsub = pubsub;

		this.timeoutsKey = `broker:${this.prefix}:timeouts`;
		this.lastTimeoutKey = `broker:${this.prefix}:lasttimeout`;

		if (this.timeoutHandler != null) {
			this._interval = setInterval(() => {
				return this._checkAckTimeouts(err=> {
					if (err != null) { return logger.error(err, {message: "Broker error while checking Timeouts", stack: err.stack}); }
			});
			}
			, this.checkInterval);
		}

		this.stats.timedout = 0;
	}

	stop(){
		super.stop();
		if (this._interval) { return clearInterval(this._interval); }
	}

	_resetStats() {
		super._resetStats();
		return this.stats.timedout = 0;
	}

	_addTimeout(user){
		const timeout = new Date().getTime() + this.ackTimeout;

		const def = Promise.defer();
		this.redis.zadd(this.timeoutsKey, timeout, user, function(err, data){
			if (err != null) { return def.reject(err); } else { return def.resolve(data); }
		});
		return def.promise;
	}

	_clearTimeout(user){

		const def = Promise.defer();
		this.redis.zrem(this.timeoutsKey, user, function(err, data){
			if (err != null) { return def.reject(err); } else { return def.resolve(data); }
		});
		return def.promise;
	}

	_checkAckTimeouts(cb){
		const expiry = Math.round(this.checkInterval/1000);
		return this.redis.set(this._timeoutLock(), Date.now(), "NX", "EX", expiry, (err, result)=> {
			if (err != null) { return cb(err); }
			if (result === 'OK') {
				// we've grabbed the lock
				return this.redis.get(this.lastTimeoutKey, (err, lasttimeout)=> {
					if (err != null) { return cb(err); }
					if (lasttimeout == null) { lasttimeout = "-inf"; }

					const now = Date.now();
					if ((lasttimeout !== "-inf") && (now < lasttimeout)) { return cb(null, null); }
					return this.redis.zrangebyscore(this.timeoutsKey, lasttimeout, "("+now, (err, users)=> {
						if (err != null) { return cb(err); }

						this.redis.set(this.lastTimeoutKey, now, err=> {
							if (err != null) { return cb(err); }
						});
						if (users.length === 0) {
							return cb(null, []);
						} else {
							this.stats.timedout+=users.length;
							for (let user of Array.from(users)) { this._timeout(user, this.timeoutHandler); } // should be done in the tx instead
							return cb(err, users);
						}
					});
				});
			}
		});
	}

	_timeout(user, timeoutHandler){

		const _prefix = this.prefix;
		const _getMessages = ()=> {
			const def = Promise.defer();

			this.redis.lrange(this._messageQueue(user), 0, -1, function(err, data){
				if (err != null) { return def.reject(err); } else { return def.resolve(data); }
			});
			return def.promise;
		};

		const _pushMessages = messages=> {
			if (messages.length === 0) { return Promise.resolve(); }
			const def = Promise.defer();
			const args = [this._timedoutQueue(user)];
			for (let message of Array.from(messages)) { args.push(message); }
			args.push(function(err, data){
				if (err != null) { return def.reject(err); } else { return def.resolve(data); }
			});

			this.redis.rpush.apply(this.redis, args);

			return def.promise;
		};

		const _removeMessages = count=> {
			const def = Promise.defer();
			this.redis.ltrim(this._messageQueue(user), 0, -(count+1), function(err, data){
				if (err != null) { return def.reject(err); } else { return def.resolve(data); }
			});
			return def.promise;
		};


		return _getMessages()
		.then(messages=> {
			return _pushMessages(messages)
			.then(() => _removeMessages(messages.length))
			.then(() => {
				return Array.from(messages).map((message) => timeoutHandler(_prefix, user, JSON.parse(message)));
			});
	}).catch(err => logger.error(err, {stack: err.stack}));
	}

	// get the message queue length for an array of users, returns a promise for array of {user, count}
	timedoutStats(users){
		const multi = this.redis.multi(); // pipeline all requests
		const def = Promise.defer();

		for (let each of Array.from(users)) { multi.llen(this._timedoutQueue(each)); }
		multi.exec(function(err, data){
			if (err != null) { return def.reject(err); } else { return def.resolve(data); }
		});

		return def.promise;
	}

	_countTimedoutMessages(user, redis){
		if (redis == null) { ({
            redis
        } = this); }
		const def = Promise.defer();
		redis.llen(this._timedoutQueue(user), function(err, data){
			if (err != null) { return def.reject(err); } else { return def.resolve(data); }
		});

		return def.promise;
	}

	_countPendingMessages(user, redis){
		if (redis == null) { ({
            redis
        } = this); }
		return Promise.all([
			this._countTimedoutMessages(user, redis),
			super._countPendingMessages(user, redis)
		])
		.spread((timedout, pending) => timedout+pending);
	}

	_peekMessage(user){

		const _lrange = ()=> {
			const def = Promise.defer();
			this.redis.lrange(this._timedoutQueue(user), 0, 0, function(err, data){
				if (err != null) { return def.reject(err); } else { return def.resolve(data); }
			});
			return def.promise;
		};

		return _lrange()
		.then(res=> {
			if (res.length === 0) { return TimeoutBroker.prototype.__proto__._peekMessage.call(this, user); } else { return res; }
		});
	}

	_popMessage(user){
		const _lpop = ()=> {
			const def = Promise.defer();
			this.redis.lpop(this._timedoutQueue(user), function(err, data){
				if (err != null) { return def.reject(err); } else { return def.resolve(data); }
			});
			return def.promise;
		};

		return _lpop().then(res=> {
			return res || TimeoutBroker.prototype.__proto__._popMessage.call(this, user);
		});
	}

	// When messages have timedout, they're put in this queue to avoid multiple notifications to occur
	_timedoutQueue(user){
		// TODO OSS : allow configuration of hardcoded "broker"
		return `broker:${this.prefix}:user.timedout:${user}`;
	}


	_timeoutLock(){
		// TODO OSS : allow configuration of hardcoded "broker"
		return `broker:${this.prefix}:timeoutlock`;
	}


	send(user, message){
		return this._addTimeout(user)
		.then(() => TimeoutBroker.prototype.__proto__.send.call(this, user, message));
	}

	ack(user, id){
		return this._clearTimeout(user)
		.then(() => TimeoutBroker.prototype.__proto__.ack.call(this, user, id));
	}
}


module.exports = TimeoutBroker;