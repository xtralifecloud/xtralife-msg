/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
require('mocha');

const should = require('should');
const Redis = require('ioredis');

const Broker = require('../src/index.js').TimeoutBroker;
const Q = require('bluebird');

let timeoutTriggered = false;

const toHandler = function (prefix, user, message) {
  prefix.should.eql('testTimeout');
  user.should.eql('timeoutuser');
  message.msg.should.eql('oktimeout');
  return timeoutTriggered = true;
};

let broker = null;
let redis = null;
let redisPubSub = null;
// @ts-ignore
const winston = require('winston')
global.logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.Console(),
  ]
});

describe("Broker Timeout", function () {
  this.timeout(200);

  before('needs a broker instance',  function(done) {
		redis = new Redis();
		redisPubSub = new Redis();
    broker = new Broker("testTimeout", redis, redisPubSub, toHandler, 1000, 1000);
    broker.ready.then(() => done()).catch(err => done(err));
  });

  beforeEach('cleanup', function (done) {
    timeoutTriggered.should.eql(false);

    // no cleanup needed, except when developing and when tests are not clean
    broker._removeFromDispatch("timeoutuser")
    broker.redis.del("broker:testTimeout:user:timeoutuser")
    return done();
  });

  it('should receive then send', function (done) {
    broker.receive("timeoutuser").then(function (message) {
      message.msg.should.eql("ok1");
      return broker.ack("timeoutuser", message.id);
    }).then(function () {
      timeoutTriggered = false;
      return done();
    })

    broker.send("timeoutuser", { msg: "ok1" })
    return null;
  });

  let _message = null;

  it('should send then receive', function (done) {
    broker.send("timeoutuser", { msg: "ok1" });

    broker.receive("timeoutuser").then(function (message) {
      _message = message;
      message.msg.should.eql("ok1");
      broker.ack("timeoutuser", message.id);
    }).then(() => done())
  });

  it('should allow ACKing an old message', done => {
    broker.ack("timeoutuser", _message.id)
      .then(() => done())
      .catch(err => done(err))
  });

  const timeoutPromise = (prom, time) =>
    Promise.race([prom, new Promise((_r, rej) => setTimeout(rej, time))]);

  it('should redeliver message if no ACK', function (done) {
    this.timeout(2000);
    broker.receive("timeoutuser").then(function (message) {
      message.msg.should.eql("ok2");
      return broker.receive("timeoutuser").then(function (message) { // no ACK => redeliver
        message.msg.should.eql("ok2");
        return broker.ack("timeoutuser", message.id).then(() => {
          timeoutPromise((() => broker.receive("timeoutuser"))(), 1000).catch(() => done());
        });
      });
    })

    broker.send("timeoutuser", { msg: "ok2" })

  });

  it('should timeout very fast', function (done) {
    this.timeout(5000);
    broker.send("timeoutuser", { msg: "oktimeout" });

    setTimeout(function () {
        timeoutTriggered.should.eql(true);
        timeoutTriggered = false;

        broker.receive("timeoutuser").then(function (message) {
          message.msg.should.eql("oktimeout");
          broker._countPendingMessages("timeoutuser").then(function (count) {
            count.should.eql(1);
            broker.ack("timeoutuser", message.id).then(() => broker._countPendingMessages("timeoutuser").then(function (count) {
              count.should.eql(0);
              return done();
            }));
          });
        })
      }
      , 3000);
    return null;
  });

  it('should timeout without ACK, even after receive', function (done) {
    this.timeout(4000);
    broker.send("timeoutuser", { msg: "oktimeout" })

    broker.receive("timeoutuser")
      .then(function (message) {
        message.msg.should.eql("oktimeout");
        setTimeout(function () {
            timeoutTriggered.should.eql(true);
            timeoutTriggered = false;
            broker.ack("timeoutuser", message.id).then(() => done())
          }
          , 3000);
      })
  });

  return after('check we left no message in the queue', () => broker.redis.llen("broker:testTimeout:user:timeoutuser").then(count => {
    count.should.eql(0);
    broker.redis.del("broker:testTimeout:lasttimeout");
		broker.stop(); // TimeoutBroker must be stopped (to stop checking for timeouts)
		// Redis connections must be stopped in order to prevent from the test to keep stuck
		redis.disconnect();
		redisPubSub.disconnect();
  }));
});
