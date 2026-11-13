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

failed = false

# Records and prints a single assertion result instead of raising, so every check runs.
def check(failed, condition, message)
  if condition
    puts "  ok: #{message}"
    failed
  else
    puts "  FAIL: #{message}"
    true
  end
end

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

  failed = check(failed, error.instance_of?(error_class), "#{name} raises #{error_class}")
  failed = check(
    failed,
    error&.cause.instance_of?(Rdkafka::Config::ConfigError),
    "#{name} error is caused by Rdkafka::Config::ConfigError"
  )
  failed = check(
    failed,
    events.size == 1 && events.first[:error].equal?(error),
    "#{name} emits one error.occurred event with the raised error"
  )
end

failed = check(
  failed,
  producer.produce_sync(topic: topic, payload: "producer").offset.zero?,
  "producer delivers to the topic"
)

events.clear
variant.buffer(message)

failed = check(failed, raised_error { producer.close }.nil?, "close with the message buffered does not raise")
failed = check(failed, producer.status.closed?, "close closes the producer")
failed = check(failed, events.size == 1, "close emits one error.occurred event")

invalid = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS, acks: "bogus" }
end

invalid_events = []
invalid.monitor.subscribe("error.occurred") { |event| invalid_events << event }

failed = check(
  failed,
  raised_error { invalid.produce_sync(message) }.instance_of?(Rdkafka::Config::ConfigError),
  "invalid producer kafka config raises Rdkafka::Config::ConfigError"
)
failed = check(failed, invalid_events.empty?, "invalid producer kafka config emits no error.occurred")

invalid.close

exit(failed ? 1 : 0)
