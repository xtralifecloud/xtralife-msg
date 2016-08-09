should = require 'should'
Redis = require 'redis'

Broker = require('../src/index.coffee').Broker
BrokerStatsPublisher  = require('../src/index.coffee').BrokerStatsPublisher

Q = require 'bluebird'

broker = null
broker2 = null
global.logger = require 'winston'

describe "Broker", ->
	this.timeout(1000)

	before 'needs a broker instance or two', (done)->
		ready = 0
		check = ->
			ready++
			if (ready == 2) then done() # check our 2 brokers are ready


		broker = new Broker "test",  Redis.createClient(), Redis.createClient()
		broker.ready.then check

		broker2 = new Broker "test",  Redis.createClient(), Redis.createClient()
		broker2.ready.then check
		return null

	beforeEach 'cleanup', (done)->
		# no cleanup needed, except when developing and when tests are not clean
		#broker._removeFromDispatch "userNormal"
		#broker2._removeFromDispatch "userNormal"
		#broker.redis.del "broker:test:userNormal:userNormal", done
		done()

	it 'should receive then send', (done)->
		broker.receive("rcvsnd").then (message)->
			message.msg.should.eql "ok1"
			broker.ack("rcvsnd", message.id).then ->
				done()

		setTimeout ->
			broker.send("rcvsnd", {msg: "ok1"})
		, 20

	it 'should send then receive', (done)->
		broker.send("sndrcv", {msg: "ok1"})

		broker.receive("sndrcv").then (message)->
			message.msg.should.eql "ok1"
			broker.ack("sndrcv", message.id).then ->done()
		return null

	it 'should send accross brokers', (done)->
		broker2.receive("accross").then (message)->
			message.msg.should.eql "ok2"
			broker.ack("accross", message.id).then ->done()

		setTimeout ->
			broker.send("accross", {msg: "ok2"})
		, 20
		return null

	it 'should use prefix as a scope to publish', (done)->
		broker3 = new Broker "othertest",  Redis.createClient(), Redis.createClient()
		broker3.ready.then ->
			broker3.receive("prefix").then (message)-> # will never receive
				should.fail()

			broker.receive("prefix").then (message)->
				message.msg.should.eql "ok3"
				broker.ack("prefix", message.id).then ->done()

			setTimeout ->
				broker.send("prefix", {msg: "ok3"})
			, 20
		return null

	it 'should cancel accross brokers', (done)->
		promise = broker.receive("cancel")
		should(promise.id).be.not.be.eql(null)

		promise.catch (err)->
			err.message.should.eql("cancelled")
			done()

		process.nextTick -> # not a real race condition, as a nextTick is enough to lift it
			broker2.cancelReceive "cancel", promise.id

	it 'should allow ACKing an old message', ()->
		broker.ack("acking", 0)

	it 'should redeliver message if no ACK', (done)->
		broker.receive("redeliver").then (message)->
			message.msg.should.eql "ok2"
			broker.receive("redeliver").then (message)-> # no ACK => redeliver
				broker2.ack("redeliver", message.id).then ->
					broker.receive("redeliver").timeout(25).catch ->done() # once ACKed => wait for timeout and done

		setTimeout ->
			broker.send("redeliver", {msg: "ok2"})
		, 20

	it.skip 'should be fast even when mixing two brokers', ()->
		this.timeout(3000) # up to 2500msg/s sent/received/acked (1500 only with Q)
		getRandomBroker = ->
			if Math.random() > 0.5 then broker2 else broker

		users = ("user"+index for index in [0..2499])
		receiveAll = Q.all (getRandomBroker().receive(each).then( (message)->
			getRandomBroker().ack(message.user, message.id)
		) for each in users)

		sendAll = Q.all (getRandomBroker().send each, {user: each} for each in users)

		receiveAll.then ->
			sendAll


	it.skip 'should let me check the message queues length', ()->
		broker.send("user", {msg:1})
		broker.pendingStats(["user", "user9999999", "user"]).then (res)->
			res.should.eql([1,0,1])

			broker.receive("user").then (message)-> # no ACK => redeliver
				broker2.ack("user", message.id).then ->

	it.skip 'should be fast to check multiple queue lengths', ()->
		users = ("user"+index for index in [0..499]) # about 100ms for 1000 queries... not bad
		broker.pendingStats(users).then (res)->
			res.length.should.eql(500)


	after 'check we left no message in the queue', (done)->
		broker.redis.llen "broker:test:userNormal:userNormal", (err, count)->
			count.should.eql(0)
			#console.log broker.stats
			#console.log broker2.stats
			done()