should = require 'should'
Redis = require 'redis'

BrokerMpx = require('../src/index.coffee').MultiplexedBroker
Q = require 'bluebird'

timeoutTriggered = false

toHandler = (prefix, user, message)->
	console.log "timeout #{user}"
	console.log message
	timeoutTriggered = true

broker = null
global.logger = require 'winston'

describe "Broker Multiplexed", ->
	this.timeout(2000)

	before 'needs a broker instance', ()->
		broker = new BrokerMpx Redis.createClient(), Redis.createClient(), toHandler, 50, 100 # check every 5ms, timeout after 10ms !
		broker.ready

	it 'should receive then send', (done)->
		broker.receive("testMpx", "user").then (message)->
			message.msg.should.eql "ok1"
			broker.ack("testMpx", "user", message.id).then -> done()
		.catch (err)->
			done(err)

		setTimeout -> # there's a rare race condition here...
			broker.send("testMpx", "user", {msg: "ok1"})
		, 10

	after 'stop all at the end', (done)->
		broker.stop() # TimeoutBroker must be stopped (to stop checking for timeouts)
		done()
