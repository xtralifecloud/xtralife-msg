/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
require('mocha');

const should = require('should');
const Redis = require('redis');

const {
    Broker
} = require('../src/index.js');
const {
    BrokerStatsPublisher
} = require('../src/index.js');

const Q = require('bluebird');

let broker = null;
let broker2 = null;
// @ts-ignore
global.logger = require('winston');

describe("Broker", function() {
	this.timeout(1000);

	before('needs a broker instance or two', function(done){
		let ready = 0;
		const check = function() {
			ready++;
			if (ready === 2) { return done(); } // check our 2 brokers are ready
		};


		broker = new Broker("test",  Redis.createClient(), Redis.createClient());
		broker.ready.then(check);

		broker2 = new Broker("test",  Redis.createClient(), Redis.createClient());
		broker2.ready.then(check);
		return null;
	});

	beforeEach('cleanup', done => // no cleanup needed, except when developing and when tests are not clean
    //broker._removeFromDispatch "userNormal"
    //broker2._removeFromDispatch "userNormal"
    //broker.redis.del "broker:test:userNormal:userNormal", done
    done());

	it('should receive then send', function(done){
		broker.receive("rcvsnd").then(function(message){
			message.msg.should.eql("ok1");
			return broker.ack("rcvsnd", message.id).then(() => done());
		});

		return setTimeout(() => broker.send("rcvsnd", {msg: "ok1"})
		, 20);
	});

	it('should send then receive', function(done){
		broker.send("sndrcv", {msg: "ok1"});

		broker.receive("sndrcv").then(function(message){
			message.msg.should.eql("ok1");
			return broker.ack("sndrcv", message.id).then(() => done());
		});
		return null;
	});

	it('should send accross brokers', function(done){
		broker2.receive("accross").then(function(message){
			message.msg.should.eql("ok2");
			return broker.ack("accross", message.id).then(() => done());
		});

		setTimeout(() => broker.send("accross", {msg: "ok2"})
		, 20);
		return null;
	});

	it('should use prefix as a scope to publish', function(done){
		const broker3 = new Broker("othertest",  Redis.createClient(), Redis.createClient());
		broker3.ready.then(function() {
			broker3.receive("prefix").then(message => // will never receive
            should.fail(true, false));

			broker.receive("prefix").then(function(message){
				message.msg.should.eql("ok3");
				return broker.ack("prefix", message.id).then(() => done());
			});

			return setTimeout(() => broker.send("prefix", {msg: "ok3"})
			, 20);
		});
		return null;
	});

	it.skip('should cancel accross brokers', function(done){
		const promise = broker.receive("cancel");
		should(promise.id).be.not.be.eql(null);

		promise.catch(function(err){
			err.message.should.eql("cancelled");
			return done();
		});

		return process.nextTick(() => // not a real race condition, as a nextTick is enough to lift it
        broker2.cancelReceive("cancel", promise.id));
	});

	it('should allow ACKing an old message', () => broker.ack("acking", 0));

	it('should redeliver message if no ACK', function(done){
		broker.receive("redeliver").then(function(message){
			message.msg.should.eql("ok2");
			return broker.receive("redeliver").then(message => // no ACK => redeliver
            broker2.ack("redeliver", message.id).then(() => broker.receive("redeliver").timeout(25).catch(() => done())));
		}); // once ACKed => wait for timeout and done

		return setTimeout(() => broker.send("redeliver", {msg: "ok2"})
		, 20);
	});

	it.skip('should be fast even when mixing two brokers', function(){
		let each;
		this.timeout(3000); // up to 2500msg/s sent/received/acked (1500 only with Q)
		const getRandomBroker = function() {
			if (Math.random() > 0.5) { return broker2; } else { return broker; }
		};

		const users = (__range__(0, 2499, true).map((index) => "user"+index));
		const receiveAll = Q.all(((() => {
			const result = [];
			for (each of Array.from(users)) { 				result.push(getRandomBroker().receive(each).then( message => getRandomBroker().ack(message.user, message.id)));
			}
			return result;
		})())
		);

		const sendAll = Q.all(((() => {
			const result1 = [];
			for (each of Array.from(users)) { 				result1.push(getRandomBroker().send(each, {user: each}));
			}
			return result1;
		})()));

		return receiveAll.then(() => sendAll);
	});


	it.skip('should let me check the message queues length', function(){
		broker.send("user", {msg:1});
		return broker.pendingStats(["user", "user9999999", "user"]).then(function(res){
			res.should.eql([1,0,1]);

			return broker.receive("user").then(message => // no ACK => redeliver
            broker2.ack("user", message.id).then(function() {}));
		});
	});

	it.skip('should be fast to check multiple queue lengths', function(){
		const users = (__range__(0, 499, true).map((index) => "user"+index)); // about 100ms for 1000 queries... not bad
		return broker.pendingStats(users).then(res => res.length.should.eql(500));
	});


	return after('check we left no message in the queue', done => broker.redis.llen("broker:test:userNormal:userNormal", function(err, count){
        count.should.eql(0);
        //console.log broker.stats
        //console.log broker2.stats
        return done();
    }));
});
function __range__(left, right, inclusive) {
  let range = [];
  let ascending = left < right;
  let end = !inclusive ? right : ascending ? right + 1 : right - 1;
  for (let i = left; ascending ? i < end : i > end; ascending ? i++ : i--) {
    range.push(i);
  }
  return range;
}