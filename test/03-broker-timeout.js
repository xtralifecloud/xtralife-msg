/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
require('mocha');

const should = require('should');
const Redis = require('redis');

const Broker = require('../src/index.js').TimeoutBroker;
const Q = require('bluebird');

let timeoutTriggered = false;

const toHandler = function(prefix, user, message){
	prefix.should.eql('testTimeout');
	user.should.eql('timeoutuser');
	message.msg.should.eql('oktimeout');
	return timeoutTriggered = true;
};

let broker = null;
// @ts-ignore
global.logger = require('winston');

describe("Broker Timeout", function() {
	this.timeout(200);

	before('needs a broker instance', function(done){
		broker = new Broker("testTimeout",  Redis.createClient(), Redis.createClient(), toHandler, 1000, 1000); // check every 5ms, timeout after 10ms !
		broker.ready.then(() => done());

		return null;
	});


	beforeEach('cleanup', function(done){
		timeoutTriggered.should.eql(false);

		// no cleanup needed, except when developing and when tests are not clean
		//broker._removeFromDispatch "timeoutuser"
		//broker.redis.del "broker:testTimeout:user:timeoutuser", done
		return done();
	});

	it('should receive then send', function(done){
		broker.receive("timeoutuser").then(function(message){
			message.msg.should.eql("ok1");
			return broker.ack("timeoutuser", message.id);}).then(function() {
			timeoutTriggered = false;
			return done();}).done();

		broker.send("timeoutuser", {msg: "ok1"})
		.done();
		return null;
	});

	let _message = null;

	it('should send then receive', function(done){
		broker.send("timeoutuser", {msg: "ok1"});

		return broker.receive("timeoutuser").then(function(message){
			_message = message;
			message.msg.should.eql("ok1");
			return broker.ack("timeoutuser", message.id);}).then(() => done())
		.done();
	});

	it('should allow ACKing an old message', done => broker.ack("timeoutuser", _message.id)
    .then(() => done())
    .catch(err => done(err))
    .done());

	it('should redeliver message if no ACK', function(done){
		this.timeout(2000);
		broker.receive("timeoutuser").then(function(message){
			message.msg.should.eql("ok2");
			return broker.receive("timeoutuser").then(function(message){ // no ACK => redeliver
				message.msg.should.eql("ok2");
				return broker.ack("timeoutuser", message.id).then(() => broker.receive("timeoutuser").timeout(1000).catch(() => done()));
			});}).done();

		return broker.send("timeoutuser", {msg: "ok2"})
		.done();
	});

	it('should timeout very fast', function(done){
		this.timeout(5000);
		broker.send("timeoutuser", {msg: "oktimeout"});

		setTimeout(function(){
			timeoutTriggered.should.eql(true);
			timeoutTriggered = false;

			return broker.receive("timeoutuser").then(function(message){
				message.msg.should.eql("oktimeout");
				return broker._countPendingMessages("timeoutuser").then(function(count){
					count.should.eql(1);
					return broker.ack("timeoutuser", message.id).then(() => broker._countPendingMessages("timeoutuser").then(function(count){
                        count.should.eql(0);
                        return done();
                    }));
				});}).done();
		}
		, 3000);
		return null;
	});

	it('should timeout without ACK, even after receive', function(done){
		this.timeout(4000);
		broker.send("timeoutuser", {msg: "oktimeout"}).done();

		return broker.receive("timeoutuser")
		.then(function(message){
			message.msg.should.eql("oktimeout");
			return setTimeout(function(){
				timeoutTriggered.should.eql(true);
				timeoutTriggered = false;
				return broker.ack("timeoutuser", message.id).then(() => done()).done();
			}
			, 3000);}).done();
	});

	return after('check we left no message in the queue', done => broker.redis.llen("broker:testTimeout:user:timeoutuser", function(err, count){
        count.should.eql(0);
        broker.stop(); // TimeoutBroker must be stopped (to stop checking for timeouts)
        return broker.redis.del("broker:testTimeout:lasttimeout", err => // clean up
        //console.log broker.stats
        done());
    }));
});