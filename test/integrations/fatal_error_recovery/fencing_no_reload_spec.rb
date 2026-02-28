# frozen_string_literal: true

# Integration test for WaterDrop fatal error handling with producer fencing
# Tests that when a transactional producer encounters a fencing error (ERR__FENCED),
# it does NOT attempt to reload the producer, as fencing is not recoverable.
#
# Fencing occurs when another producer with the same transactional.id takes over,
# which is an unrecoverable state. The producer should fail immediately rather than
# attempting to reload.

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Track instrumentation events
reload_events = []
error_events = []

# Create first transactional producer
transactional_id = "fence-test-#{SecureRandom.uuid}"
producer1 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "transactional.id": transactional_id,
    "transaction.timeout.ms": 30_000,
    "message.timeout.ms": 30_000
  }
  config.max_wait_timeout = 5_000
  config.reload_on_transaction_fatal_error = true
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

producer1.monitor.subscribe("producer.reloaded") { |event| reload_events << event }
producer1.monitor.subscribe("error.occurred") { |event| error_events << event }

# Start a transaction with first producer
topic_name = "it-fence-#{SecureRandom.hex(6)}"
producer1.transaction do
  producer1.produce_sync(topic: topic_name, payload: "message1")
end

# Create second producer with SAME transactional.id to fence the first one
producer2 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "transactional.id": transactional_id,
    "transaction.timeout.ms": 30_000,
    "message.timeout.ms": 30_000
  }
  config.max_wait_timeout = 5_000
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

# Start transaction with second producer - this will fence the first one
producer2.transaction do
  producer2.produce_sync(topic: topic_name, payload: "message2")
end

# Now try to use the fenced producer1 - it should fail without reloading
fenced_error_raised = false
begin
  producer1.transaction do
    producer1.produce_sync(topic: topic_name, payload: "message3")
  end
rescue Rdkafka::RdkafkaError => e
  # Should get a fencing error
  fenced_error_raised = e.message.include?("fenced") || e.code == :fenced
end

producer1.close
producer2.close

# Verify results:
# 1. A fencing-related error should have been raised
# 2. The producer should NOT have attempted to reload (reload_events should be empty)
success = fenced_error_raised && reload_events.empty?

exit(success ? 0 : 1)
