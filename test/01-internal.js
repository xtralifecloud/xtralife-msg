/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const should = require('should');
const Redis = require('redis');

const Broker = require('../src/index.js');

let broker = null;

global.logger = require('winston');

describe("local Broker", function() {
	this.timeout(200);

	before('it should create a broker', () => broker = new Broker.LocalBroker);

	it('should use receive then send', function(done){

		const receive = broker.receive("user");

		receive.then(function(message){
			message.msg.should.eql("ok");
			broker._removeFromDispatch("user");
			return done();
		});

		return broker._dispatchMessage("user", {msg: "ok"});
});

	it('should timeout when asked to', function(done){
		const receive = broker.receive("user").timeout(20); // timeout after 20ms without a message

		receive.then(message => true.should.eql(false)).catch(err => done());
		return null;
	});

	it('should dispatch to the same user more than once', function(done){
		let count = 0;

		const receive1 = broker.receive("user");
		const receive2 = broker.receive("user");

		for (let receive of [receive1, receive2]) {
			((receive => receive.then(function(message){
                message.msg.should.eql("ok");
                count++;
                if (count === 2) {
                    broker._removeFromDispatch("user");
                    return done();
                }
            })))(receive);
		}

		return broker._dispatchMessage("user", {msg: "ok"});
});

	it('should dispatch only to correct user', function(done){
		const neverreceive = broker.receive("otheruser");
		const receive = broker.receive("user"); // timeout after 20ms without a message

		receive.then(function(message){
			broker._removeFromDispatch("user");

			return done();
		});

		neverreceive.then(message => should.fail());

		return broker._dispatchMessage("user", {"for":"your eyes only"});
});

	it('should allow cancelling receive', function(done){
		const receive = broker.receive("user");

		receive.then(message => should.fail()).catch(function(err){
			err.message.should.eql("cancelled");
			return broker._removeFromDispatch("user");
		});

		const receive2 = broker.receive("user");

		receive2.then(function(message){
			message.msg.should.eql("ok");
			return done();
		});

		broker.cancelReceive("user", receive.id);
		return broker._dispatchMessage("user", {msg: "ok"});
});

	return it('should not cancel fullfilled promises', function(done){
		const receive = broker.receive("user");

		receive.then(function(message){
			message.msg.should.eql("ok");
			return done();
		});

		broker._dispatchMessage("user", {msg: "ok"});
		return broker.cancelReceive("user", receive.id);
	});
}); // will not cause cancellation, because user was already removed from dispatch
