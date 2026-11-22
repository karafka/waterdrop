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
    warn "FAIL: #{name} raises #{error_class}"
    exit(1)
  end

  unless error&.cause.instance_of?(Rdkafka::Config::ConfigError)
    warn "FAIL: #{name} error is caused by Rdkafka::Config::ConfigError"
    exit(1)
  end

  unless events.size == 1 && events.first[:error].equal?(error)
    warn "FAIL: #{name} emits one error.occurred event with the raised error"
    exit(1)
  end
end

unless producer.produce_sync(topic: topic, payload: "producer").offset.zero?
  warn "FAIL: producer delivers to the topic"
  exit(1)
end

events.clear
variant.buffer(message)

unless raised_error { producer.close }.nil?
  warn "FAIL: close with the message buffered does not raise"
  exit(1)
end

unless producer.status.closed?
  warn "FAIL: close closes the producer"
  exit(1)
end

unless events.size == 1
  warn "FAIL: close emits one error.occurred event"
  exit(1)
end

invalid = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS, acks: "bogus" }
end

invalid_events = []
invalid.monitor.subscribe("error.occurred") { |event| invalid_events << event }

unless raised_error { invalid.produce_sync(message) }.instance_of?(Rdkafka::Config::ConfigError)
  warn "FAIL: invalid producer kafka config raises Rdkafka::Config::ConfigError"
  exit(1)
end

unless invalid_events.empty?
  warn "FAIL: invalid producer kafka config emits no error.occurred"
  exit(1)
end

invalid.close
