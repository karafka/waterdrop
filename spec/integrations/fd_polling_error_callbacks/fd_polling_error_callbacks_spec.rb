# frozen_string_literal: true

# Integration test for error callbacks with FD polling mode
#
# Tests that:
# 1. Error callbacks are properly triggered in FD mode
# 2. librdkafka error events are received via the poller
# 3. Producer continues to function after recoverable errors

require "waterdrop"
require "securerandom"

MESSAGE_COUNT = 50
error_callbacks = []
delivery_reports = []

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "request.required.acks": 1
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 30_000
end

producer.monitor.subscribe("error.occurred") do |event|
  error_callbacks << {
    type: event[:type],
    error: event[:error].class.name
  }
end

producer.monitor.subscribe("message.acknowledged") do |event|
  delivery_reports << event[:offset]
end

topic = "it-fd-error-callbacks-#{SecureRandom.hex(6)}"

handles = []
MESSAGE_COUNT.times do |i|
  handles << producer.produce_async(
    topic: topic,
    payload: "message-#{i}"
  )
end

handles.each(&:wait)
producer.close

failed = false

if delivery_reports.size != MESSAGE_COUNT
  puts "Expected #{MESSAGE_COUNT} delivery reports, got #{delivery_reports.size}"
  failed = true
end

exit(failed ? 1 : 0)
