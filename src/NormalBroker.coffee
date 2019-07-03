Promise = require 'bluebird'

uuidv4 = require 'uuid/v4'

LocalBroker = require './LocalBroker.coffee'

# TODO OSS : redis is actually promisified in xtralife... we could use promises instead of ugly defer

class Broker extends LocalBroker
	constructor: (@prefix, @redis, @pubsub, @key = null)->
		super()
		unless @key? then @key = "Broker:channel-" + @prefix
		@ready = @_receiveFromPubSub() # @ready is a promise which will be resolved when the redis subscribe is OK

		@stats.sent = 0
		@stats.sentGauge = 0
		@stats.acked = 0
		@stats.ackedGauge = 0

	# send a message to a user
	# user : the id of the user (string or int)
	# message : the message (JS object)
	# returns : a promise
	# note : a .id field is added to the message with the message id
	send: (user, message)->
		message.id = uuidv4()

		@stats.sent++
		@stats.sentGauge++
		return Promise.all [
			@_saveMessage(user, message), # save the message
			@_publishMessage(user, message) # publish it
		]

	# receive a single message for a user
	# user : the id of the user (string or int)
	# returns : a promise for the message (JS Object)
	# note : the promise contains a .id field which must be used cancelReceive
	receive: (user)->
		id = @_getPromiseId()
		result = @_countPendingMessages(user).then (count)=>
			if count > 0 # if we have pending messages
				@_peekMessage(user).then(JSON.parse) # then resolve to that pending message
			else
				super(user, id) # else use local receive to wait until a message arrives
		result.id = id
		result

	# ACK a message
	# user : the id of the user
	# id : the message.id field
	# returns : a promise which will resolve when the ACK is OK
	ack: (user, id)->
		@_peekMessage(user)
		.then (array)->
			if array.length is 0 then "null" else array[0]
		.then(JSON.parse)
		.then (message)=> # peek message
			if message? and id? and message.id != id
				message # if we try to ack the wrong message, should be Q.reject() instead ?
			else
				if message?
					@stats.acked++
					@stats.ackedGauge++
					@_popMessage(user).then(JSON.parse)
				else null

	# cancel a receive call
	# user : the user id
	# id : the promise.id from the promise returned by receive
	cancelReceive: (user, id)->
		# distribute the cancel among all brokers
		@redis.publish @key, JSON.stringify({type: "cancel", user: user, id: id})

	# get the message queue length for an array of users, returns a promise for array of integers, in the same order
	pendingStats: (users)->
		multi = @redis.multi() # pipeline all requests

		def = Promise.defer()
		(multi.llen @_messageQueue(user)) for user in users
		multi.exec (err, data)->
			if err? then def.reject err else def.resolve data

		def.promise

	_llen: (red, key)->
		def = Promise.defer()
		red.llen key, (err, data)->
			if err? then def.reject err else def.resolve data

		def.promise

	_resetStats: ->
		super()
		@stats.sent = 0
		@stats.acked = 0

	_messageQueue: (user)->
		"broker:#{@prefix}:user:#{user}" # TODO OSS: allow ("broker") configuration instead of hardcoding

	_saveMessage: (user, message)->
		def = Promise.defer()
		# TODO OSS : expire queues
		# messageQueues should really expire, after a few months, for easier maintenance of gone players
		@redis.rpush @_messageQueue(user), JSON.stringify(message), (err, data)->
			if err? then def.reject err else def.resolve data
		def.promise

	_publishMessage: (user, message)->
		def = Promise.defer()

		@redis.publish @key, JSON.stringify({
			type: 'dispatch'
			user: user
			body: message}), (err, data)->
			if err? then def.reject err else def.resolve data
		def.promise

	_countPendingMessages: (user, redis=@redis)->
		def = Promise.defer()
		redis.llen @_messageQueue(user), (err, data)->
			if err? then def.reject err else def.resolve data
		def.promise

	_peekMessage: (user)->
		def = Promise.defer()
		@redis.lrange @_messageQueue(user), 0, 0, (err, data)->
			if err? then def.reject err else def.resolve data
		def.promise


	_popMessage: (user)->
		def = Promise.defer()
		@redis.lpop @_messageQueue(user), (err, data)->
			if err? then def.reject err else def.resolve data
		def.promise

	_receiveFromPubSub: ()->
		def = Promise.defer()
		@pubsub.setMaxListeners 1000
		@pubsub.on 'message', (topic, json)=>
			# we're subscribed on only one topic, but we're all on the same redis cx
			# so we must return if the topic isn't ours
			return if topic isnt @key
			message = JSON.parse json
			switch message.type
				when "dispatch"
					@_dispatchMessage message.user, message.body # dispatch message locally
				when "cancel"
					@_localCancelReceive message.user, message.id # attempt local cancel receive
				else
					logger.error "invalid message received from pubsub"

		@pubsub.on 'subscribe', (topic, count)=>
			def.resolve(count) # we're ready now, so let's resolve

		# start the subscription
		@pubsub.subscribe @key, (err)=>
			if err? then def.reject(err) # an error occurred, @ready will be rejected

		# in case of error, we assume redis went down and subscription must be restarted
		@pubsub.on 'error', (err)=>
			logger.info 'Error with Redis, broker attempting reconnect in 1s'
			logger.error err
			setTimeout =>
				@pubsub.subscribe @key, (err)=>
					if err? then logger.error 'Broker could not resubscribe to Redis'
					else logger.info "Broker resubscribed"
			, 1000

		def.promise

	stop: ()->
		@pubsub.unsubscribe @key, (err)->
			if err? then logger.error err.message, {stack: err.stack}


module.exports = Broker