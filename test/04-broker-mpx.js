/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
require('mocha');
const should = require('should');
const Redis = require('redis');

const BrokerMpx = require('../src/index.js').MultiplexedBroker;
const Q = require('bluebird');

let timeoutTriggered = false;

const toHandler = function(prefix, user, message){
	console.log(`timeout ${user}`);
	console.log(message);
	return timeoutTriggered = true;
};

let broker = null;
// @ts-ignore
global.logger = require('winston');

describe("Broker Multiplexed", function() {
	this.timeout(2000);

	before('needs a broker instance', function(){
		broker = new BrokerMpx(Redis.createClient(), Redis.createClient(), toHandler, 50, 100); // check every 5ms, timeout after 10ms !
		return broker.ready;
	});

	it('should receive then send', function(done){
		broker.receive("testMpx", "user").then(function(message){
			message.msg.should.eql("ok1");
			return broker.ack("testMpx", "user", message.id).then(() => done());}).catch(err => done(err));

		return setTimeout(() => // there's a rare race condition here...
        broker.send("testMpx", "user", {msg: "ok1"})
		, 10);
	});

	return after('stop all at the end', function(done){
		broker.stop(); // TimeoutBroker must be stopped (to stop checking for timeouts)
		return done();
	});
});
