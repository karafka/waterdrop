# frozen_string_literal: true

# Integration test for WaterDrop variant input validation.
#
# A variant built with `topic_config: nil` or `max_wait_timeout: nil` raises
# `WaterDrop::Errors::VariantInvalidError`. A variant that omits both keys uses the producer
# defaults, and the producer keeps producing.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

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

unless raised_class { producer.with(topic_config: nil) } == WaterDrop::Errors::VariantInvalidError
  warn "FAIL: topic_config: nil raises VariantInvalidError"
  exit(1)
end

unless raised_class { producer.with(max_wait_timeout: nil) } == WaterDrop::Errors::VariantInvalidError
  warn "FAIL: max_wait_timeout: nil raises VariantInvalidError"
  exit(1)
end

variant = producer.with(topic_config: { acks: 1 })

unless variant.max_wait_timeout == producer.config.max_wait_timeout
  warn "FAIL: omitted max_wait_timeout uses the producer default"
  exit(1)
end

unless variant.produce_sync(topic: topic, payload: "variant").offset.zero?
  warn "FAIL: variant produces"
  exit(1)
end

unless producer.produce_sync(topic: topic, payload: "producer").offset == 1
  warn "FAIL: producer produces"
  exit(1)
end

producer.close
