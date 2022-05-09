/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const Promise = require('bluebird');

const { v4: uuidv4 } = require('uuid');

class LocalBroker {
	constructor(){
		this._dispatch = {};

		this.stats= {ccu:0, localDispatch: 0, maxccuGauge:0, ccuGauge: 0}; // subclasses will add other statistics to this object
	}

	// local receive
	// id can be set by our sub class
	// we add a promise to _dispatch, so we can later resolve it, or reject it, or timeout
	// for each user, we will store an array of promises, each with its own id (used to cancel)
	receive(user, id = null){
		const deferred = this._defer();
		// @ts-ignore
		deferred.promise.id = (id == null) ? this._getPromiseId() : id;

		this._addToDispatch(user, deferred);

		return deferred.promise;
	}

	cancelReceive(user, id){
		return this._localCancelReceive(user, id);
	}

	_resetStats() {
		this.stats.ccu = 0;
		return this.stats.localDispatch = 0;
	}

	_getPromiseId(){
		return uuidv4();
	}

	_localCancelReceive(user, id){
		if (this._hasDispatch(user)) {
			let deferred;
			for (let each of Array.from(this._getDispatch(user))) { if (each.promise.id === id) { deferred = each; } }

			this._dispatch[user] = this._getDispatch(user).filter(each => each.promise.id !== id);

			if (!deferred.promise.isFulfilled()) { return deferred.reject(new Error("cancelled")); }
		}
	}

	_dispatchMessage(user, message){
		if (this._hasDispatch(user)) {
			const allPromises = this._getDispatch(user); // get promises
			this._removeFromDispatch(user); // all promises will be resolved, so remove them now (this allows the receive to receive() again)
			this.stats.localDispatch+= allPromises.length;
			return (Array.from(allPromises).map((deferred) => deferred.resolve(message)));
		}
	}

	_getDispatch(user){
		return this._dispatch[user];
	}

	_hasDispatch(user){
		return (this._dispatch[user] != null);
	}

	_addToDispatch(user, deferred){
		if (!this._hasDispatch(user)) {
			this._dispatch[user] = [deferred];
			this.stats.ccu++;
			this.stats.ccuGauge++;
			return this.stats.maxccuGauge = Math.max(this.stats.ccuGauge, this.stats.maxccuGauge);
		} else {
			return this._getDispatch(user).push(deferred);
		}
	}

	_removeFromDispatch(user){
		if (this._dispatch[user] != null) {
			delete this._dispatch[user];
			this.stats.ccu--;
			return this.stats.ccuGauge--;
		}
	}

	_defer() {
		let resolve, reject;
		const promise = new Promise(function() {
			resolve = arguments[0];
			reject = arguments[1];
		});
		return {
			resolve: resolve,
			reject: reject,
			promise: promise
		};
	}
}

module.exports = LocalBroker;