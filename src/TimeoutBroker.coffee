Promise = require 'bluebird'

Broker = require './NormalBroker.coffee'

# TODO OSS : redis is actually promisified in xtralife... we could use promises instead of ugly defer

class TimeoutBroker extends Broker
	constructor: (prefix, redis, pubsub, @timeoutHandler, @checkInterval=15000, @ackTimeout=15000, key)->
		unless key? then key = "Broker:channel-" + prefix
		super(prefix, redis, pubsub, key)
		@prefix = prefix
		@key = key
		@redis = redis
		@pubsub = pubsub

		@timeoutsKey = "broker:#{@prefix}:timeouts"
		@lastTimeoutKey = "broker:#{@prefix}:lasttimeout"

		if @timeoutHandler?
			@_interval = setInterval =>
				@_checkAckTimeouts (err)=>
					if err? then logger.error err, {message: "Broker error while checking Timeouts", stack: err.stack}
			, @checkInterval

		@stats.timedout = 0

	stop: ()->
		super()
		if @_interval then clearInterval @_interval

	_resetStats: ->
		super()
		@stats.timedout = 0

	_addTimeout: (user)->
		timeout = new Date().getTime() + @ackTimeout

		def = Promise.defer()
		@redis.zadd @timeoutsKey, timeout, user, (err, data)->
			if err? then def.reject err else def.resolve data
		def.promise

	_clearTimeout: (user)->

		def = Promise.defer()
		@redis.zrem @timeoutsKey, user, (err, data)->
			if err? then def.reject err else def.resolve data
		def.promise

	_checkAckTimeouts: (cb)->

		@redis.setnx @_timeoutLock(), Date.now(), (err, result)=>
			if err? then return cb(err)
			if result is 1
				# we've grabbed the lock
				expiry = Math.round(@checkInterval/1000)
				@redis.expire @_timeoutLock(), expiry, (err)=>
					if err? then return cb(err)

					@redis.get @lastTimeoutKey, (err, lasttimeout)=>
						if err? then return cb(err)
						unless lasttimeout? then lasttimeout = "-inf"

						now = Date.now()
						return cb(null, null) if lasttimeout != "-inf" and now < lasttimeout
						@redis.zrangebyscore @timeoutsKey, lasttimeout, "("+now, (err, users)=>
							return cb(err) if err?

							@redis.set @lastTimeoutKey, now, (err)=>
								if err? then return cb(err)
							if users.length == 0
								cb(null, [])
							else
								@stats.timedout+=users.length
								@_timeout(user, @timeoutHandler) for user in users # should be done in the tx instead
								cb(err, users)

	_timeout: (user, timeoutHandler)->

		_prefix = @prefix
		_getMessages = ()=>
			def = Promise.defer()

			@redis.lrange @_messageQueue(user), 0, -1, (err, data)->
				if err? then def.reject err else def.resolve data
			def.promise

		_pushMessages = (messages)=>
			if messages.length is 0 then return Promise.resolve()
			def = Promise.defer()
			args = [@_timedoutQueue(user)]
			args.push message for message in messages
			args.push (err, data)->
				if err? then def.reject err else def.resolve data

			@redis.rpush.apply @redis, args

			def.promise

		_removeMessages = (count)=>
			def = Promise.defer()
			@redis.ltrim @_messageQueue(user), 0, -(count+1), (err, data)->
				if err? then def.reject err else def.resolve data
			def.promise


		_getMessages()
		.then (messages)=>
			_pushMessages(messages)
			.then =>_removeMessages(messages.length)
			.then =>
				timeoutHandler _prefix, user, JSON.parse(message) for message in messages
		.catch (err)->
			logger.error err, {stack: err.stack}

	# get the message queue length for an array of users, returns a promise for array of {user, count}
	timedoutStats: (users)->
		multi = @redis.multi() # pipeline all requests
		def = Promise.defer()

		(multi.llen @_timedoutQueue(each)) for each in users
		multi.exec (err, data)->
			if err? then def.reject err else def.resolve data

		def.promise

	_countTimedoutMessages: (user, redis=@redis)->
		def = Promise.defer()
		redis.llen @_timedoutQueue(user), (err, data)->
			if err? then def.reject err else def.resolve data

		def.promise

	_countPendingMessages: (user, redis=@redis)->
		Promise.all [
			@_countTimedoutMessages user, redis
			super(user, redis)
		]
		.spread (timedout, pending)->
			return timedout+pending

	_peekMessage: (user)->

		_lrange = ()=>
			def = Promise.defer()
			@redis.lrange @_timedoutQueue(user), 0, 0, (err, data)->
				if err? then def.reject err else def.resolve data
			def.promise

		_lrange()
		.then (res)=>
			if res.length is 0 then super(user) else res

	_popMessage: (user)->
		_lpop = ()=>
			def = Promise.defer()
			@redis.lpop @_timedoutQueue(user), (err, data)->
				if err? then def.reject err else def.resolve data
			def.promise

		_lpop().then (res)=>
			res or super(user)

	# When messages have timedout, they're put in this queue to avoid multiple notifications to occur
	_timedoutQueue: (user)->
		# TODO OSS : allow configuration of hardcoded "broker"
		"broker:#{@prefix}:user.timedout:#{user}"


	_timeoutLock: ()->
		# TODO OSS : allow configuration of hardcoded "broker"
		"broker:#{@prefix}:timeoutlock"


	send: (user, message)->
		@_addTimeout(user)
		.then => super(user, message)

	ack: (user, id)->
		@_clearTimeout(user)
		.then => super(user, id)


module.exports = TimeoutBroker