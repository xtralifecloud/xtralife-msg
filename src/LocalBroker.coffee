Promise = require 'bluebird'
uuid = require 'node-uuid'

class LocalBroker
	constructor: ()->
		@_dispatch = {}

		@stats= ccu:0, localDispatch: 0, maxccuGauge:0, ccuGauge: 0 # subclasses will add other statistics to this object

	# local receive
	# id can be set by our sub class
	# we add a promise to _dispatch, so we can later resolve it, or reject it, or timeout
	# for each user, we will store an array of promises, each with its own id (used to cancel)
	receive: (user, id = null)->
		deferred = Promise.defer()
		deferred.promise.id = if !id? then @_getPromiseId() else id

		@_addToDispatch user, deferred

		return deferred.promise

	cancelReceive: (user, id)->
		@_localCancelReceive user, id

	_resetStats: ->
		@stats.ccu = 0
		@stats.localDispatch = 0

	_getPromiseId: ()->
		uuid.v4({rng:uuid.nodeRNG})

	_localCancelReceive: (user, id)->
		if @_hasDispatch(user)
			deferred = each for each in @_getDispatch(user) when each.promise.id == id

			@_dispatch[user] = @_getDispatch(user).filter (each) ->
				each.promise.id != id

			if !deferred.promise.isFulfilled() then deferred.reject(new Error("cancelled"))

	_dispatchMessage: (user, message)->
		if @_hasDispatch(user)
			allPromises = @_getDispatch(user) # get promises
			@_removeFromDispatch(user) # all promises will be resolved, so remove them now (this allows the receive to receive() again)
			@stats.localDispatch+= allPromises.length
			(deferred.resolve message for deferred in allPromises)

	_getDispatch: (user)->
		@_dispatch[user]

	_hasDispatch: (user)->
		@_dispatch[user]?

	_addToDispatch: (user, deferred)->
		if !@_hasDispatch(user)
			@_dispatch[user] = [deferred]
			@stats.ccu++
			@stats.ccuGauge++
			@stats.maxccuGauge = Math.max @stats.ccuGauge, @stats.maxccuGauge
		else
			@_getDispatch(user).push(deferred)

	_removeFromDispatch: (user)->
		if @_dispatch[user]?
			delete @_dispatch[user]
			@stats.ccu--
			@stats.ccuGauge--

module.exports = LocalBroker