should = require 'should'
Redis = require 'redis'

Broker = require '../src/index.coffee'

broker = null

global.logger = require 'winston'

describe "local Broker", ->
	this.timeout(200)

	before 'it should create a broker', ()->

		broker = new Broker.LocalBroker

	it 'should use receive then send', (done)->

		receive = broker.receive("user")

		receive.then (message)->
			message.msg.should.eql "ok"
			broker._removeFromDispatch("user")
			done()

		broker._dispatchMessage "user", {msg: "ok"}

	it 'should timeout when asked to', (done)->
		receive = broker.receive("user").timeout(20) # timeout after 20ms without a message

		receive.then (message)->
			true.should.eql(false)
		.catch (err)->
			done()
		return null

	it 'should dispatch to the same user more than once', (done)->
		count = 0

		receive1 = broker.receive("user")
		receive2 = broker.receive("user")

		for receive in [receive1, receive2]
			do (receive)->
				receive.then (message)->
					message.msg.should.eql("ok")
					count++
					if count == 2
						broker._removeFromDispatch("user")
						done()

		broker._dispatchMessage "user", {msg: "ok"}

	it 'should dispatch only to correct user', (done)->
		neverreceive = broker.receive("otheruser")
		receive = broker.receive("user") # timeout after 20ms without a message

		receive.then (message)->
			broker._removeFromDispatch("user")

			done()

		neverreceive.then (message)->
			should.fail()

		broker._dispatchMessage "user", {"for":"your eyes only"}

	it 'should allow cancelling receive', (done)->
		receive = broker.receive("user")

		receive.then (message)->
			should.fail()
		.catch (err)->
			err.message.should.eql("cancelled")
			broker._removeFromDispatch("user")

		receive2 = broker.receive("user")

		receive2.then (message)->
			message.msg.should.eql("ok")
			done()

		broker.cancelReceive "user", receive.id
		broker._dispatchMessage "user", {msg: "ok"}

	it 'should not cancel fullfilled promises', (done)->
		receive = broker.receive("user")

		receive.then (message)->
			message.msg.should.eql("ok")
			done()

		broker._dispatchMessage "user", {msg: "ok"}
		broker.cancelReceive "user", receive.id # will not cause cancellation, because user was already removed from dispatch
