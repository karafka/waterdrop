# frozen_string_literal: true

# Integration test for WaterDrop Producer Testing API
# Tests the fatal error injection and query capabilities using the Testing module
# to validate real librdkafka fatal error behavior without requiring broker-side issues.
#
# This test verifies:
# 1. Fatal error injection using trigger_test_fatal_error
# 2. Fatal error state query using fatal_error
# 3. Producer with reload enabled can recover and produce after fatal error
# 4. Producer without reload cannot produce after fatal error

require 'waterdrop'
require 'waterdrop/producer/testing'
require 'logger'
require 'securerandom'

BOOTSTRAP_SERVERS = ENV.fetch('BOOTSTRAP_SERVERS', '127.0.0.1:9092')

topic_name = "it-testing-api-#{SecureRandom.hex(6)}"

# Test 1: Producer with reload disabled - should NOT recover
producer_no_reload = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'enable.idempotence': true
  }
  config.max_wait_timeout = 30_000
  config.reload_on_idempotent_fatal_error = false
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

producer_no_reload.singleton_class.include(WaterDrop::Producer::Testing)

# Verify producer works initially
report = producer_no_reload.produce_sync(topic: topic_name, payload: 'before-fatal')
exit(1) unless report.is_a?(Rdkafka::Producer::DeliveryReport) && report.error.nil?

# Verify no fatal error initially
exit(1) unless producer_no_reload.fatal_error.nil?

# Trigger fatal error
result = producer_no_reload.trigger_test_fatal_error(47, 'Test no-reload behavior')
exit(1) unless result.zero?

# Verify fatal error is present
fatal_error = producer_no_reload.fatal_error
exit(1) if fatal_error.nil?
exit(1) unless fatal_error[:error_code] == 47
exit(1) unless fatal_error[:error_string].is_a?(String) && !fatal_error[:error_string].empty?

# Try to produce after fatal error - should fail
produce_failed = false
begin
  producer_no_reload.produce_sync(topic: topic_name, payload: 'after-fatal')
rescue WaterDrop::Errors::ProduceError => e
  produce_failed = true
  # Verify the error contains fatal librdkafka error
  exit(1) unless e.cause.is_a?(Rdkafka::RdkafkaError)
  exit(1) unless e.cause.fatal?
end

exit(1) unless produce_failed

# Fatal error should still be present
exit(1) unless producer_no_reload.fatal_error[:error_code] == 47

producer_no_reload.close

# Test 2: Producer with reload enabled - should recover
reload_events = []
reloaded_events = []

producer_with_reload = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'enable.idempotence': true
  }
  config.max_wait_timeout = 30_000
  config.reload_on_idempotent_fatal_error = true
  config.wait_backoff_on_idempotent_fatal_error = 100
  config.max_attempts_on_idempotent_fatal_error = 3
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

producer_with_reload.singleton_class.include(WaterDrop::Producer::Testing)

producer_with_reload.monitor.subscribe('producer.reload') { |event| reload_events << event }
producer_with_reload.monitor.subscribe('producer.reloaded') { |event| reloaded_events << event }

# Verify producer works initially
report = producer_with_reload.produce_sync(topic: topic_name, payload: 'before-fatal-2')
exit(1) unless report.is_a?(Rdkafka::Producer::DeliveryReport) && report.error.nil?

# Trigger fatal error
result = producer_with_reload.trigger_test_fatal_error(47, 'Test reload behavior')
exit(1) unless result.zero?

# Verify fatal error is present
fatal_error = producer_with_reload.fatal_error
exit(1) if fatal_error.nil?
exit(1) unless fatal_error[:error_code] == 47

# Try to produce after fatal error - should succeed after reload
begin
  report = producer_with_reload.produce_sync(topic: topic_name, payload: 'after-fatal-reload')
  exit(1) unless report.is_a?(Rdkafka::Producer::DeliveryReport)
rescue WaterDrop::Errors::ProduceError
  # Should not fail - reload should allow recovery
  exit(1)
end

# Verify reload events were emitted
exit(1) unless reload_events.size >= 1
exit(1) unless reloaded_events.size >= 1

# Verify reload event contains proper error information
reload_event = reload_events.first
exit(1) unless reload_event[:error].is_a?(Rdkafka::RdkafkaError)
exit(1) unless reload_event[:error].fatal?

producer_with_reload.close

exit(0)
