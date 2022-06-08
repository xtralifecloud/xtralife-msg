/*
 * decaffeinate suggestions:
 * DS001: Remove Babel/TypeScript constructor workaround
 * DS101: Remove unnecessary use of Array.from
 * DS102: Remove unnecessary code created because of implicit returns
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const Promise = require('bluebird');

const {v4: uuidv4} = require('uuid');

const LocalBroker = require('./LocalBroker.js');

class Broker extends LocalBroker {
    constructor(prefix, redis, pubsub, key = null) {
        super();
        this.prefix = prefix;
        this.redis = redis;
        this.pubsub = pubsub;
        this.key = key;
        if (this.key == null) {
            this.key = "Broker:channel-" + this.prefix;
        }
        this.ready = this._receiveFromPubSub(); // @ready is a promise which will be resolved when the redis subscribe is OK

        this.stats.sent = 0;
        this.stats.sentGauge = 0;
        this.stats.acked = 0;
        this.stats.ackedGauge = 0;
    }

    // send a message to a user
    // user : the id of the user (string or int)
    // message : the message (JS object)
    // returns : a promise
    // note : a .id field is added to the message with the message id
    send(user, message) {
        message.id = uuidv4();

        this.stats.sent++;
        this.stats.sentGauge++;
        return Promise.all([
            this._saveMessage(user, message), // save the message
            this._publishMessage(user, message) // publish it
        ]);
    }

    // receive a single message for a user
    // user : the id of the user (string or int)
    // returns : a promise for the message (JS Object)
    // note : the promise contains a .id field which must be used cancelReceive
    receive(user) {
        const id = this._getPromiseId();
        const result = this._countPendingMessages(user).then(count => {
            if (count > 0) { // if we have pending messages
                return this._peekMessage(user).then(JSON.parse); // then resolve to that pending message
            } else {
                // @ts-ignore
                return super.receive(user, id);
            }
        }); // else use local receive to wait until a message arrives
        // @ts-ignore
        result.id = id;
        return result;
    }

    // ACK a message
    // user : the id of the user
    // id : the message.id field
    // returns : a promise which will resolve when the ACK is OK
    ack(user, id) {
        return this._peekMessage(user)
            .then(function (array) {
                if (array.length === 0) {
                    return "null";
                } else {
                    return array[0];
                }
            })
            .then(JSON.parse)
            .then(async message => { // peek message
                if ((message != null) && (id != null) && (message.id !== id)) {
                    return message; // if we try to ack the wrong message, should be Q.reject() instead ?
                } else {
                    if (message != null) {
                        this.stats.acked++;
                        this.stats.ackedGauge++;
                        return await this._popMessage(user).then(JSON.parse);
                    } else {
                        return null;
                    }
                }
            });
    }

    // cancel a receive call
    // user : the user id
    // id : the promise.id from the promise returned by receive
    async cancelReceive(user, id) {
        // distribute the cancel among all brokers
        return await this.redis.publish(this.key, JSON.stringify({type: "cancel", user, id}));
    }

    // get the message queue length for an array of users, returns a promise for array of integers, in the same order
    async pendingStats(users) {
        const multi = this.redis.multi(); // pipeline all requests

        for (let user of Array.from(users)) {
            multi.LLEN(this._messageQueue(user));
        }
        return await multi.exec();
    }

    _resetStats() {
        super._resetStats();
        this.stats.sent = 0;
        return this.stats.acked = 0;
    }

    _messageQueue(user) {
        return `broker:${this.prefix}:user:${user}`; // TODO OSS: allow ("broker") configuration instead of hardcoding
    }

    async _saveMessage(user, message) {
        return await this.redis.RPUSH(this._messageQueue(user), JSON.stringify(message)).catch(err => console.log(err));
    }

    async _publishMessage(user, message) {
       return await this.redis.publish(this.key, JSON.stringify({
            type: 'dispatch',
            user,
            body: message
        }))
    }

    async _countPendingMessages(user, redis) {
        if (redis == null) {
            ({
                redis
            } = this);
        }

        return await redis.LLEN(this._messageQueue(user));
    }

    async _peekMessage(user) {
        return await this.redis.LRANGE(this._messageQueue(user), 0, 0)
    }

    async _popMessage(user) {
        return await this.redis.LPOP(this._messageQueue(user))
    }

    async _receiveFromPubSub() {
        this.pubsub.setMaxListeners(1000);

        await this.pubsub.on('error', err => {
            // @ts-ignore
            logger.info('Error with Redis, broker attempting reconnect in 1s');
            // @ts-ignore
            logger.error(err);
            return setTimeout(() => {
                  return this.pubsub.subscribe(this.key, err => {
                      // @ts-ignore
                      if (err != null) {
                          return logger.error('Broker could not resubscribe to Redis');
                          // @ts-ignore
                      } else {
                          return logger.info("Broker resubscribed");
                      }
                  });
              }
              , 1000);
        })

        return await this.pubsub.subscribe(this.key, (messageString, channelName) => {
            // we're subscribed on only one topic, but we're all on the same redis cx
            // so we must return if the topic isn't ours
            if (channelName !== this.key) {
                return;
            }
            const message = JSON.parse(messageString);
            switch (message.type) {
                case "dispatch":
                    return this._dispatchMessage(message.user, message.body); // dispatch message locally
                case "cancel":
                    return this._localCancelReceive(message.user, message.id); // attempt local cancel receive
                default:
                    // @ts-ignore
                    return logger.error("invalid message received from pubsub");
            }
        })
    }

    async stop() {
        return await this.pubsub.unsubscribe(this.key).catch(err => logger.error(err.message, {stack: err.stack}))
    }
}


module.exports = Broker;