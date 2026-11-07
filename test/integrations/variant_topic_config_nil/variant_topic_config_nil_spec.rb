# frozen_string_literal: true

# Integration test for WaterDrop variant input validation.
#
# A variant built with `topic_config: nil` or `max_wait_timeout: nil` raises
# `WaterDrop::Errors::VariantInvalidError`. A variant that omits both keys uses the producer
# defaults, and the producer keeps producing.

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

# Returns the class of the error raised by the block or nil when nothing was raised
def raised_class
  yield
  nil
rescue => e
  e.class
end

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

topic = generate_topic("variant-nil")

failed = check(
  failed,
  raised_class { producer.with(topic_config: nil) } == WaterDrop::Errors::VariantInvalidError,
  "topic_config: nil raises VariantInvalidError"
)

failed = check(
  failed,
  raised_class { producer.with(max_wait_timeout: nil) } == WaterDrop::Errors::VariantInvalidError,
  "max_wait_timeout: nil raises VariantInvalidError"
)

variant = producer.with(topic_config: { acks: 1 })

failed = check(
  failed,
  variant.max_wait_timeout == producer.config.max_wait_timeout,
  "omitted max_wait_timeout uses the producer default"
)

failed = check(
  failed,
  variant.produce_sync(topic: topic, payload: "variant").offset.zero?,
  "variant produces"
)

failed = check(
  failed,
  producer.produce_sync(topic: topic, payload: "producer").offset == 1,
  "producer produces"
)

producer.close

exit(failed ? 1 : 0)
