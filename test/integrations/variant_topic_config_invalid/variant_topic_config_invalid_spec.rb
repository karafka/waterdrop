# frozen_string_literal: true

# Integration test for WaterDrop variant topic config errors.
#
# A variant with an invalid `topic_config` value fails at its first dispatch with
# `WaterDrop::Errors::ProduceError` (sync and async) or `WaterDrop::Errors::ProduceManyError`
# (many), caused by `Rdkafka::Config::ConfigError`. Each failure emits one `error.occurred`
# event, and the producer keeps delivering to that topic. `close` with such a message buffered
# closes the producer. An invalid producer `kafka` config raises `Rdkafka::Config::ConfigError`
# itself, with no `error.occurred` event.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Returns the error raised by the block or nil when nothing was raised
def raised_error
  yield
  nil
rescue => e
  e
end

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

events = []
producer.monitor.subscribe("error.occurred") { |event| events << event }

topic = generate_topic("variant-invalid")
message = { topic: topic, payload: "variant" }
variant = producer.with(topic_config: { acks: "bogus" })

{
  produce_sync: [WaterDrop::Errors::ProduceError, ->(v) { v.produce_sync(message) }],
  produce_async: [WaterDrop::Errors::ProduceError, ->(v) { v.produce_async(message) }],
  produce_many_sync: [WaterDrop::Errors::ProduceManyError, ->(v) { v.produce_many_sync([message]) }],
  produce_many_async: [WaterDrop::Errors::ProduceManyError, ->(v) { v.produce_many_async([message]) }]
}.each do |name, (error_class, call)|
  events.clear
  error = raised_error { call.call(variant) }

  unless error.instance_of?(error_class)
    warn "FAIL: #{name}: expected #{error_class}, got #{error.inspect}"
    exit(1)
  end

  unless error.cause.instance_of?(Rdkafka::Config::ConfigError)
    warn "FAIL: #{name}: expected cause Rdkafka::Config::ConfigError, got #{error.cause.inspect}"
    exit(1)
  end

  unless events.size == 1 && events.first[:error].equal?(error)
    warn "FAIL: #{name}: expected one error.occurred with the raised error, got #{events.size} events"
    exit(1)
  end
end

offset = producer.produce_sync(topic: topic, payload: "producer").offset

unless offset.zero?
  warn "FAIL: producer delivery: expected offset 0, got #{offset}"
  exit(1)
end

events.clear
variant.buffer(message)

close_error = raised_error { producer.close }

unless close_error.nil?
  warn "FAIL: close with the message buffered raised #{close_error.inspect}"
  exit(1)
end

unless producer.status.closed?
  warn "FAIL: close did not close the producer (status #{producer.status})"
  exit(1)
end

unless events.size == 1
  warn "FAIL: close: expected 1 error.occurred, got #{events.size}"
  exit(1)
end

invalid = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS, acks: "bogus" }
end

invalid_events = []
invalid.monitor.subscribe("error.occurred") { |event| invalid_events << event }

invalid_error = raised_error { invalid.produce_sync(message) }

unless invalid_error.instance_of?(Rdkafka::Config::ConfigError)
  warn "FAIL: invalid producer kafka config: expected Rdkafka::Config::ConfigError, got #{invalid_error.inspect}"
  exit(1)
end

unless invalid_events.empty?
  warn "FAIL: invalid producer kafka config emitted #{invalid_events.size} error.occurred"
  exit(1)
end

invalid.close
