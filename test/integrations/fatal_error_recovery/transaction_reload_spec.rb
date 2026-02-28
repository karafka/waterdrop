# frozen_string_literal: true

# Integration test for WaterDrop transactional producer with reload configuration
# Tests that a transactional producer with reload_on_transaction_fatal_error enabled
# can successfully execute transactions.
#
# This verifies the configuration works correctly and the producer functions normally
# with transactional fatal error recovery enabled. The fencing test covers the actual
# fatal error scenario where reload should NOT happen (fencing is unrecoverable).

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Track instrumentation events
reload_events = []
error_events = []

# Create transactional producer with reload enabled
transactional_id = "tx-reload-test-#{SecureRandom.uuid}"
producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "transactional.id": transactional_id,
    "transaction.timeout.ms": 30_000,
    "message.timeout.ms": 30_000,
    "enable.idempotence": true
  }
  config.max_wait_timeout = 30_000
  config.reload_on_transaction_fatal_error = true
  config.wait_backoff_on_transaction_fatal_error = 100
  config.max_attempts_on_transaction_fatal_error = 3
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

producer.monitor.subscribe("producer.reloaded") { |event| reload_events << event }
producer.monitor.subscribe("error.occurred") { |event| error_events << event }

topic_name = "it-tx-reload-#{SecureRandom.hex(6)}"

# Verify configuration is set correctly
config_valid = producer.config.reload_on_transaction_fatal_error == true &&
  producer.config.wait_backoff_on_transaction_fatal_error == 100 &&
  producer.config.max_attempts_on_transaction_fatal_error == 3

# Produce messages in transactions
successful_transactions = 0
3.times do |i|
  producer.transaction do
    producer.produce_sync(topic: topic_name, payload: "transaction-#{i}-msg-1")
    producer.produce_sync(topic: topic_name, payload: "transaction-#{i}-msg-2")
  end
  successful_transactions += 1
end

producer.close

# Verify results:
# 1. Configuration should be set correctly
# 2. All transactions should complete successfully
# 3. No reload events should occur (no errors in normal operation)
success = config_valid && successful_transactions == 3 && reload_events.empty?

exit(success ? 0 : 1)
