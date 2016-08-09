Promise = require 'bluebird'

LocalBroker = require './LocalBroker.coffee'
Broker = require './NormalBroker.coffee'
TimeoutBroker = require './TimeoutBroker.coffee'

class MultiplexedBroker
	constructor: (@redis, @pubsub, @timeoutHandler, @checkInterval=15000, @ackTimeout= 50000)->
		@_brokers={}
		@ready = Promise.resolve()

	start: (prefix)->
		@_getBroker prefix

	stop: ()->
		(each.stop() for _,each of @_brokers)

	send: (prefix, user, message)->
		(@_getBroker prefix).send user, message

	sendVolatile: (prefix, user, message)->
		(@_getBroker prefix)._publishMessage user, message

	ack: (prefix, user, id)->
		(@_getBroker prefix).ack user, id

	receive: (prefix, user)->
		(@_getBroker prefix).receive user

	cancelReceive: (prefix, user, id)->
		(@_getBroker prefix).cancelReceive user, id

	queueStats: (prefix, users)->
		(@_getBroker prefix).queueStats users

	_getBroker: (prefix)->
		@_brokers[prefix] or @_brokers[prefix] = new TimeoutBroker(prefix, @redis, @pubsub, @timeoutHandler, @checkInterval, @ackTimeout)


module.exports = {TimeoutBroker, Broker, LocalBroker, MultiplexedBroker}
