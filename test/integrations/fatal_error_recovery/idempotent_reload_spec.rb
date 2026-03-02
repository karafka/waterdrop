# frozen_string_literal: true

# Integration test for WaterDrop idempotent producer fatal error recovery
# Tests that when an idempotent producer encounters a fatal error, it successfully
# reloads the client and continues producing messages.
#
# This simulates a scenario where the idempotent producer hits a fatal error
# and verifies the producer can automatically recover and continue producing.

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Track instrumentation events
reload_events = []
error_events = []

# Create idempotent producer with reload enabled
producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "enable.idempotence": true,
    "statistics.interval.ms": 100,
    "request.required.acks": "all"
  }
  config.max_wait_timeout = 30_000
  config.reload_on_idempotent_fatal_error = true
  config.wait_backoff_on_idempotent_fatal_error = 100 # Short for testing
  config.max_attempts_on_idempotent_fatal_error = 3
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

producer.monitor.subscribe("producer.reloaded") { |event| reload_events << event }
producer.monitor.subscribe("error.occurred") { |event| error_events << event }

# Monkey patch to inject a fatal error on first produce call
call_count = 0
original_produce = producer.client.method(:produce)

producer.client.define_singleton_method(:produce) do |**kwargs|
  call_count += 1
  if call_count == 1
    # Simulate a fatal idempotent error on first call
    error = Rdkafka::RdkafkaError.new(-138, "Simulated fatal error")
    # Mark it as fatal
    error.define_singleton_method(:fatal?) { true }
    raise error
  else
    # Call original method for subsequent calls
    original_produce.call(**kwargs)
  end
end

topic_name = "it-idem-reload-#{SecureRandom.hex(6)}"

# Produce messages - first one will trigger fatal error and reload
successful_messages = 0
5.times do |i|
  producer.produce_sync(topic: topic_name, payload: "message-#{i}")
  successful_messages += 1
end

producer.close

# Verify results:
# 1. All messages should have been produced successfully
# 2. Producer should have reloaded once (reload_events.size == 1)
# 3. An error event should have been recorded for the fatal error
success = successful_messages == 5 && reload_events.size == 1

exit(success ? 0 : 1)
