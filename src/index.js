/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const Promise = require('bluebird');

const LocalBroker = require('./LocalBroker.js');
const Broker = require('./NormalBroker.js');
const TimeoutBroker = require('./TimeoutBroker.js');

class MultiplexedBroker {
	constructor(redis, pubsub, timeoutHandler, checkInterval, ackTimeout) {
		this.redis = redis;
		this.pubsub = pubsub;
		this.timeoutHandler = timeoutHandler;
		if (!checkInterval) { checkInterval = 15000; }
		this.checkInterval = checkInterval;
		if (!ackTimeout) { ackTimeout = 50000; }
		this.ackTimeout = ackTimeout;
		this._brokers = {};
		this.ready = Promise.resolve();
	}

	start(prefix) {
		return this._getBroker(prefix);
	}

	stop() {
		// @ts-ignore
		return Object.values(this._brokers).map(each => each.stop())
	}

	send(prefix, user, message) {
		return (this._getBroker(prefix)).send(user, message);
	}

	sendVolatile(prefix, user, message) {
		return (this._getBroker(prefix))._publishMessage(user, message);
	}

	ack(prefix, user, id) {
		return (this._getBroker(prefix)).ack(user, id);
	}

	receive(prefix, user) {
		return (this._getBroker(prefix)).receive(user);
	}

	cancelReceive(prefix, user, id) {
		return (this._getBroker(prefix)).cancelReceive(user, id);
	}

	queueStats(prefix, users) {
		return (this._getBroker(prefix)).queueStats(users);
	}

	_getBroker(prefix) {
		return this._brokers[prefix] || (this._brokers[prefix] = new TimeoutBroker(prefix, this.redis, this.pubsub, this.timeoutHandler, this.checkInterval, this.ackTimeout));
	}
}


module.exports = { TimeoutBroker, Broker, LocalBroker, MultiplexedBroker };
