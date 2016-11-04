should = require 'should'
Redis = require 'redis'

Broker = require('../src/index.coffee').TimeoutBroker
Q = require 'bluebird'

timeoutTriggered = false

toHandler = (prefix, user, message)->
	prefix.should.eql('testTimeout')
	user.should.eql('timeoutuser')
	message.msg.should.eql('oktimeout')
	timeoutTriggered = true

broker = null
global.logger = require 'winston'

describe "Broker Timeout", ->
	this.timeout(200)

	before 'needs a broker instance', (done)->
		broker = new Broker "testTimeout",  Redis.createClient(), Redis.createClient(), toHandler, 1000, 1000 # check every 5ms, timeout after 10ms !
		broker.ready.then ->
			done()

		return null


	beforeEach 'cleanup', (done)->
		timeoutTriggered.should.eql(false)

		# no cleanup needed, except when developing and when tests are not clean
		#broker._removeFromDispatch "timeoutuser"
		#broker.redis.del "broker:testTimeout:user:timeoutuser", done
		done()

	it 'should receive then send', (done)->
		broker.receive("timeoutuser").then (message)->
			message.msg.should.eql "ok1"
			broker.ack("timeoutuser", message.id)
		.then ->
			timeoutTriggered = false
			done()
		.done()

		broker.send("timeoutuser", {msg: "ok1"})
		.done()
		return null

	_message = null

	it 'should send then receive', (done)->
		broker.send("timeoutuser", {msg: "ok1"})

		broker.receive("timeoutuser").then (message)->
			_message = message
			message.msg.should.eql "ok1"
			broker.ack("timeoutuser", message.id)
		.then ->done()
		.done()

	it 'should allow ACKing an old message', (done)->
		broker.ack("timeoutuser", _message.id)
		.then ->done()
		.catch (err)-> done(err)
		.done()

	it 'should redeliver message if no ACK', (done)->
		this.timeout 2000
		broker.receive("timeoutuser").then (message)->
			message.msg.should.eql "ok2"
			broker.receive("timeoutuser").then (message)-> # no ACK => redeliver
				message.msg.should.eql "ok2"
				broker.ack("timeoutuser", message.id).then ->
					broker.receive("timeoutuser").timeout(1000).catch -> done() # once ACKed => wait for timeout and done
		.done()

		broker.send("timeoutuser", {msg: "ok2"})
		.done()

	it 'should timeout very fast', (done)->
		this.timeout 5000
		broker.send("timeoutuser", {msg: "oktimeout"})

		setTimeout ()->
			timeoutTriggered.should.eql(true)
			timeoutTriggered = false

			broker.receive("timeoutuser").then (message)->
				message.msg.should.eql "oktimeout"
				broker._countPendingMessages("timeoutuser").then (count)->
					count.should.eql(1)
					broker.ack("timeoutuser", message.id).then ->
						broker._countPendingMessages("timeoutuser").then (count)->
							count.should.eql(0)
							done()
			.done()
		, 3000
		return null

	it 'should timeout without ACK, even after receive', (done)->
		this.timeout 4000
		broker.send("timeoutuser", {msg: "oktimeout"}).done()

		broker.receive("timeoutuser")
		.then (message)->
			message.msg.should.eql "oktimeout"
			setTimeout ()->
				timeoutTriggered.should.eql(true)
				timeoutTriggered = false
				broker.ack("timeoutuser", message.id).then ->
					done()
				.done()
			, 3000
		.done()

	after 'check we left no message in the queue', (done)->
		broker.redis.llen "broker:testTimeout:user:timeoutuser", (err, count)->
			count.should.eql(0)
			broker.stop() # TimeoutBroker must be stopped (to stop checking for timeouts)
			broker.redis.del "broker:testTimeout:lasttimeout", (err)-> # clean up
				#console.log broker.stats
				done()