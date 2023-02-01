/*
 * decaffeinate suggestions:
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS205: Consider reworking code to avoid use of IIFEs
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
require('mocha');

const should = require('should');
const Redis = require('ioredis');

const {
  Broker
} = require('../src/index.js');
const {
  BrokerStatsPublisher
} = require('../src/index.js');

const Q = require('bluebird');

let broker = null;
let broker2 = null;
let redis = null;
let redisPubSub = null;
let redis2 = null;
let redisPubSub2 = null;
let redis3 = null;
let redisPubSub3 = null;
// @ts-ignore
const winston = require('winston')
global.logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.Console(),
  ]
});

describe("Broker", function () {
  this.timeout(1000);

  before('needs a broker instance or two', done => {
    let ready = 0;
    const check = function() {
      ready++;
      if (ready === 2) { return done(); } // check our 2 brokers are ready
    };

    redis = new Redis();
    redisPubSub = new Redis();
    broker = new Broker("test", redis, redisPubSub);
    broker.ready.then(check);

    redis2 = new Redis();
    redisPubSub2 = new Redis();
    broker2 = new Broker("test", redis2, redisPubSub2);
    broker2.ready.then(check);
    return null;
  });

  beforeEach('cleanup', done => // no cleanup needed, except when developing and when tests are not clean
    //broker._removeFromDispatch "userNormal"
    //broker2._removeFromDispatch "userNormal"
    //broker.redis.del "broker:test:userNormal:userNormal", done
    done());

  it('should receive then send', done => {
    broker.receive("rcvsnd").then(message => {
      message.msg.should.eql("ok1");
      return broker.ack("rcvsnd", message.id).then(() => done());
    });

    return setTimeout(() => broker.send("rcvsnd", { msg: "ok1" })
      , 20);
  });

  it('should send then receive', function (done) {
    broker.send("sndrcv", { msg: "ok1" });

    broker.receive("sndrcv").then(function (message) {
      message.msg.should.eql("ok1");
      return broker.ack("sndrcv", message.id).then(() => done());
    });
    return null;
  });

  it('should send accross brokers', function (done) {
    broker2.receive("accross").then(function (message) {
      message.msg.should.eql("ok2");
      return broker.ack("accross", message.id).then(() => done());
    });

    setTimeout(() => broker.send("accross", { msg: "ok2" })
      , 20);
    return null;
  });

  it('should use prefix as a scope to publish', function (done) {
    redis3 = new Redis();
    redisPubSub3 = new Redis();
    const broker3 = new Broker("othertest", redis3, redisPubSub3);
    broker3.ready.then(() => {
      broker3.receive("prefix").then(message => // will never receive
        should.fail(true, false));

      broker.receive("prefix").then(function (message) {
        message.msg.should.eql("ok3");
        return broker.ack("prefix", message.id).then(() => done());
      });

      return setTimeout(() => broker.send("prefix", { msg: "ok3" })
        , 20);
    });
  });

  it('should cancel accross brokers', function (done) {
    const promise = broker.receive("cancel");
    should(promise.id).be.not.be.eql(null);

    promise.catch(err => {
      err.message.should.eql("cancelled");
      return done();
    });

    return process.nextTick(() => // not a real race condition, as a nextTick is enough to lift it
      broker2.cancelReceive("cancel", promise.id));
  });

  it('should allow ACKing an old message', () => broker.ack("acking", 0));

  const timeoutPromise = (prom, time) =>
    Promise.race([prom, new Promise((_r, rej) => setTimeout(rej, time))]);

  it('should redeliver message if no ACK', function (done) {
    broker.receive("redeliver").then(function (message) {
      message.msg.should.eql("ok2");
      return broker.receive("redeliver").then(message => // no ACK => redeliver
        broker2.ack("redeliver", message.id).then(() => {
          timeoutPromise((() => broker.receive("redeliver"))(), 25).catch(() => done());
        }));
    }); // once ACKed => wait for timeout and done

    return setTimeout(() => broker.send("redeliver", { msg: "ok2" })
      , 20);
  });

  it('should be fast even when mixing two brokers', function () {
    let each;
    this.timeout(3000); // up to 2500msg/s sent/received/acked (1500 only with Q)
    const getRandomBroker = function () {
      if (Math.random() > 0.5) {
        return broker2;
      } else {
        return broker;
      }
    };

    const users = (__range__(0, 2499, true).map((index) => "user" + index));
    const receiveAll = Q.all(((() => {
        const result = [];
        for (each of Array.from(users)) {
          result.push(getRandomBroker().receive(each).then(message => getRandomBroker().ack(message.user, message.id)));
        }
        return result;
      })())
    );

    const sendAll = Q.all(((() => {
      const result1 = [];
      for (each of Array.from(users)) {
        result1.push(getRandomBroker().send(each, { user: each }));
      }
      return result1;
    })()));

    return receiveAll.then(() => sendAll);
  });


  it('should let me check the message queues length', function () {
    broker.send("user", { msg: 1 });
    return broker.pendingStats(["user", "user9999999", "user"]).then(function (res) {
      res.should.eql([[null, 1], [null, 0], [null, 1]]);

      return broker.receive("user").then(message => // no ACK => redeliver
        broker2.ack("user", message.id).then(function () {
        }));
    });
  });

  it('should be fast to check multiple queue lengths', function () {
    const users = (__range__(0, 499, true).map((index) => "user" + index)); // about 100ms for 1000 queries... not bad
    return broker.pendingStats(users).then(res => res.length.should.eql(500));
  });

  return after('check we left no message in the queue', async () => broker.redis.llen("broker:test:userNormal:userNormal").then(count => {
    count.should.eql(0);
		// Redis connections must be stopped in order to prevent from the test to keep stuck
		redis.disconnect();
		redisPubSub.disconnect();
		redis2.disconnect();
		redisPubSub2.disconnect();
		redis3.disconnect();
		redisPubSub3.disconnect();
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
