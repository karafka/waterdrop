# frozen_string_literal: true

# Integration test for WaterDrop Producer Testing API
# Tests the fatal error injection and query capabilities using the Testing module
# to validate real librdkafka fatal error behavior without requiring broker-side issues.
#
# This test verifies:
# 1. Fatal error injection using trigger_test_fatal_error
# 2. Fatal error state query using fatal_error
# 3. Producer reload behavior after fatal error injection
# 4. Event instrumentation during fatal error reload

require 'waterdrop'
require 'waterdrop/producer/testing'
require 'logger'
require 'securerandom'

BOOTSTRAP_SERVERS = ENV.fetch('BOOTSTRAP_SERVERS', '127.0.0.1:9092')

# Track instrumentation events
reload_events = []
reloaded_events = []
error_events = []

# Create idempotent producer with reload enabled
producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'enable.idempotence': true,
    'statistics.interval.ms': 100,
    'request.required.acks': 'all'
  }
  config.max_wait_timeout = 30_000
  config.reload_on_idempotent_fatal_error = true
  config.wait_backoff_on_idempotent_fatal_error = 100 # Short for testing
  config.max_attempts_on_idempotent_fatal_error = 3
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

# Include the Testing module
producer.singleton_class.include(WaterDrop::Producer::Testing)

# Subscribe to events
producer.monitor.subscribe('producer.reload') { |event| reload_events << event }
producer.monitor.subscribe('producer.reloaded') { |event| reloaded_events << event }
producer.monitor.subscribe('error.occurred') { |event| error_events << event }

topic_name = "it-testing-api-#{SecureRandom.hex(6)}"

puts 'Step 1: Verify producer works normally'
report = producer.produce_sync(topic: topic_name, payload: 'test-message-1')
unless report.is_a?(Rdkafka::Producer::DeliveryReport) && report.error.nil?
  puts 'FAILED: Initial produce failed'
  exit(1)
end
puts '✓ Producer works normally'

puts "\nStep 2: Verify no fatal error initially"
initial_fatal_error = producer.fatal_error
unless initial_fatal_error.nil?
  puts 'FAILED: Fatal error present before injection'
  exit(1)
end
puts '✓ No fatal error initially'

puts "\nStep 3: Trigger fatal error using Testing API"
result = producer.trigger_test_fatal_error(47, 'Integration test fatal error')
unless result == 0
  puts "FAILED: trigger_test_fatal_error returned #{result}, expected 0"
  exit(1)
end
puts '✓ Fatal error triggered successfully'

puts "\nStep 4: Query fatal error state"
fatal_error = producer.fatal_error
unless fatal_error
  puts 'FAILED: Fatal error not present after triggering'
  exit(1)
end

unless fatal_error[:error_code] == 47
  puts "FAILED: Expected error code 47, got #{fatal_error[:error_code]}"
  exit(1)
end

unless fatal_error[:error_string].is_a?(String) && !fatal_error[:error_string].empty?
  puts 'FAILED: Error string is missing or invalid'
  exit(1)
end
puts "✓ Fatal error state correct: code=#{fatal_error[:error_code]}, " \
     "string='#{fatal_error[:error_string]}'"

puts "\nStep 5: Verify fatal error persistence"
second_query = producer.fatal_error
unless second_query == fatal_error
  puts 'FAILED: Fatal error state changed between queries'
  exit(1)
end
puts '✓ Fatal error state persists'

puts "\nStep 6: Attempt produce after fatal error (will trigger reload)"
# Mock the client's produce to fail once with fatal error, then succeed
original_client = producer.client
call_count = 0

original_client.define_singleton_method(:produce) do |**_kwargs|
  call_count += 1
  if call_count == 1
    # Raise fatal error to trigger reload path
    error = Rdkafka::RdkafkaError.new(-150, 'Fatal error in produce')
    error.define_singleton_method(:fatal?) { true }
    raise error
  else
    # After reload, simulate successful produce
    handle = instance_double(
      Rdkafka::Producer::DeliveryHandle,
      wait: Rdkafka::Producer::DeliveryReport.new(0, 0, topic_name, nil, nil)
    )
    handle
  end
end

begin
  producer.produce_sync(topic: topic_name, payload: 'test-after-fatal')
  # Producer should have reloaded and succeeded
rescue StandardError => e
  puts "FAILED: Produce after fatal error failed: #{e.message}"
  exit(1)
end

puts '✓ Producer handled fatal error (reload attempted)'

puts "\nStep 7: Verify reload events were emitted"
unless reload_events.size >= 1
  puts "FAILED: Expected at least 1 producer.reload event, got #{reload_events.size}"
  exit(1)
end
puts "✓ producer.reload events emitted: #{reload_events.size}"

unless reloaded_events.size >= 1
  puts "FAILED: Expected at least 1 producer.reloaded event, got #{reloaded_events.size}"
  exit(1)
end
puts "✓ producer.reloaded events emitted: #{reloaded_events.size}"

puts "\nStep 8: Verify reload event payload"
reload_event = reload_events.first
unless reload_event[:producer_id] == producer.id
  puts 'FAILED: Reload event producer_id mismatch'
  exit(1)
end

unless reload_event[:error].is_a?(Rdkafka::RdkafkaError)
  puts 'FAILED: Reload event error is not an RdkafkaError'
  exit(1)
end

unless reload_event[:attempt] == 1
  puts "FAILED: Expected attempt=1, got #{reload_event[:attempt]}"
  exit(1)
end
puts '✓ Reload event payload valid'

puts "\nStep 9: Verify error.occurred events"
fatal_error_events = error_events.select { |e| e[:type] == 'librdkafka.idempotent_fatal_error' }
unless fatal_error_events.size >= 1
  puts 'FAILED: Expected error.occurred event for fatal error'
  exit(1)
end
puts "✓ error.occurred events recorded: #{fatal_error_events.size}"

puts "\nStep 10: Test with different error code"
# Create a fresh producer for this test
producer2 = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'enable.idempotence': true
  }
  config.max_wait_timeout = 30_000
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

producer2.singleton_class.include(WaterDrop::Producer::Testing)

# Test with error code 64
result2 = producer2.trigger_test_fatal_error(64, 'Testing error code 64')
unless result2 == 0
  puts "FAILED: trigger_test_fatal_error with code 64 returned #{result2}"
  producer2.close
  exit(1)
end

error64 = producer2.fatal_error
unless error64 && error64[:error_code] == 64
  puts 'FAILED: Error code 64 not set correctly'
  producer2.close
  exit(1)
end
puts '✓ Different error codes work correctly'

producer2.close

# Cleanup
producer.close

puts "\n#{'=' * 60}"
puts 'ALL TESTS PASSED ✓'
puts '=' * 60
puts "\nSummary:"
puts '- Fatal error injection: ✓'
puts '- Fatal error querying: ✓'
puts '- Fatal error persistence: ✓'
puts '- Reload triggering: ✓'
puts '- Event instrumentation: ✓'
puts '- Multiple error codes: ✓'

exit(0)
