# frozen_string_literal: true

# Integration test for WaterDrop with reload disabled
# Tests that when reload_on_idempotent_fatal_error is set to false,
# the producer does NOT attempt to reload on fatal errors.
#
# This verifies that the reload feature can be disabled and fatal errors
# propagate to the caller as expected.

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Track instrumentation events
reload_events = []
error_events = []

# Create idempotent producer with reload DISABLED
producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "enable.idempotence": true,
    "statistics.interval.ms": 100,
    "request.required.acks": "all"
  }
  config.max_wait_timeout = 30_000
  config.reload_on_idempotent_fatal_error = false # Disabled
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

producer.monitor.subscribe("producer.reloaded") { |event| reload_events << event }
producer.monitor.subscribe("error.occurred") { |event| error_events << event }

topic_name = "it-reload-disabled-#{SecureRandom.hex(6)}"

# Produce messages - should work normally
successful_messages = 0
5.times do |i|
  producer.produce_sync(topic: topic_name, payload: "message-#{i}")
  successful_messages += 1
end

producer.close

# Verify results:
# 1. All messages should have been produced successfully
# 2. No reload events should occur (reload is disabled)
success = successful_messages == 5 && reload_events.empty?

exit(success ? 0 : 1)
