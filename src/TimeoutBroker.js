/*
 * decaffeinate suggestions:
 * DS001: Remove Babel/TypeScript constructor workaround
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const Promise = require('bluebird');

const Broker = require('./NormalBroker.js');

class TimeoutBroker extends Broker {
	constructor(prefix, redis, pubsub, timeoutHandler, checkInterval, ackTimeout, key){
		super(prefix, redis, pubsub, key);
		this.timeoutHandler = timeoutHandler;
		if (!checkInterval) { checkInterval = 15000; }
		this.checkInterval = checkInterval;
		if (!ackTimeout) { ackTimeout = 15000; }
		this.ackTimeout = ackTimeout;
		if (!key) { key = "Broker:channel-" + prefix; }
		this.prefix = prefix;
		this.key = key;
		this.redis = redis;
		this.pubsub = pubsub;

		this.timeoutsKey = `broker:${this.prefix}:timeouts`;
		this.lastTimeoutKey = `broker:${this.prefix}:lasttimeout`;

		if (this.timeoutHandler != null) {
			this._interval = setInterval(() => {
				return this._checkAckTimeouts(err=> {
					// @ts-ignore
					if (err != null) { return logger.error(err, {message: "Broker error while checking Timeouts", stack: err.stack}); }
			});
			}
			, this.checkInterval);
		}

		this.stats.timedout = 0;
	}

	async stop(){
		await super.stop();
		if (this._interval) { return clearInterval(this._interval); }
	}

	_resetStats() {
		super._resetStats();
		return this.stats.timedout = 0;
	}

	async _addTimeout(user){
		const timeout = new Date().getTime() + this.ackTimeout;
		return await this.redis.zadd(this.timeoutsKey, timeout, user.toString());
	}


	async _clearTimeout(user){
		return await this.redis.zrem(this.timeoutsKey, user.toString());
	}

	async _checkAckTimeouts(cb){
		const expiry = Math.round(this.checkInterval/1000);

		const result = await this.redis.set(this._timeoutLock(), Date.now(), "NX", "EX", expiry).catch(err => cb(err))
		if (result === 'OK') {
			// we've grabbed the lock
			let lastTimeout = await this.redis.get(this.lastTimeoutKey).catch(err => cb(err));
				if (lastTimeout == null) { lastTimeout = "-inf"; }

				const now = Date.now();
				if ((lastTimeout !== "-inf") && (now < lastTimeout)) { return cb(null, null); }
				const users = await this.redis.zrangebyscore(this.timeoutsKey, lastTimeout, "(" + now ).catch(err => cb(err))
			  await this.redis.set(this.lastTimeoutKey, now).catch(err => cb(err))

				if (users.length === 0) {
					return cb(null, []);
				} else {

					this.stats.timedout+=users.length;
					for (let user of Array.from(users)) { this._timeout(user, this.timeoutHandler); } // should be done in the tx instead
					return cb(null, users);
				}
		}
	}

	_timeout(user, timeoutHandler){
		const _prefix = this.prefix;
		const _getMessages = async () => {
			return await this.redis.lrange(this._messageQueue(user), 0, -1);
		};

		const _pushMessages = async messages => {
			if (messages.length === 0) { return Promise.resolve(); }
			const promises = [];
			for (let message of Array.from(messages)) { promises.push(this.redis.rpush(this._timedoutQueue(user), message)) }
			return await Promise.all(promises)
		};

		const _removeMessages = async count => {
			return await this.redis.ltrim(this._timedoutQueue(user), 0, -(count+1)).catch(err => console.log(err));
		};


		return _getMessages()
		.then(messages=> {
			return _pushMessages(messages)
			.then( () => {
				return _removeMessages(messages.length)
			})
			.then(() => {
				return Array.from(messages).map((message) => {
					timeoutHandler(_prefix, user, JSON.parse(message))
				});
			});
	// @ts-ignore
	}).catch(err => logger.error(err, {stack: err.stack}));
	}

	async timedoutStats(users){
		const multi = this.redis.multi(); // pipeline all requests
		for (let each of Array.from(users)) { multi.llen(this._timedoutQueue(each)); }

		return await multi.exec();
	}
	async _countTimedoutMessages(user, redis){
		if (redis == null) { ({
            redis
        } = this); }

		return await redis.llen(this._timedoutQueue(user));
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

		const _lrange = async () => {
			return await this.redis.lrange(this._timedoutQueue(user), 0, 0);
		};

		return _lrange()
		.then(async res=> {
			if (res.length === 0) { return await super._peekMessage(user); } else { return res; }
		});
	}

	_popMessage(user){
		const _lpop = async ()=> {
			return await this.redis.lpop(this._timedoutQueue(user));
		};

		return _lpop().then(async res=> {
			// @ts-ignore
			return res || await super._popMessage(user);
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
		// @ts-ignore
		.then(async () => {
			await super.send(user, message)
		});
	}

	ack(user, id){
		return this._clearTimeout(user)
		.then(async () => {
			await super.ack(user, id)
		});
	}
}


module.exports = TimeoutBroker;