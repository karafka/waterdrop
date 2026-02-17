# frozen_string_literal: true

# Integration test for FD polling thread priority configuration
#
# Tests that:
# 1. Thread priority can be configured via WaterDrop::Polling.setup
# 2. The poller thread actually runs with the configured priority
# 3. Callbacks execute on the poller thread with correct priority

require "waterdrop"
require "securerandom"

EXPECTED_PRIORITY = -2
topic = "it-fd-priority-#{SecureRandom.hex(6)}"
failed = false
callback_thread_priority = nil
callback_thread_name = nil

# Configure thread priority before creating any producers
WaterDrop::Polling.setup do |config|
  config.thread_priority = EXPECTED_PRIORITY
end

# Verify configuration was applied
if WaterDrop::Polling::Config.config.thread_priority != EXPECTED_PRIORITY
  puts "Config not applied: expected #{EXPECTED_PRIORITY}, got #{WaterDrop::Polling::Config.config.thread_priority}"
  exit 1
end

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
  }
  config.polling.mode = :fd
end

# Subscribe to message acknowledged callback - this runs on the poller thread
producer.monitor.subscribe("message.acknowledged") do |_event|
  callback_thread_priority = Thread.current.priority
  callback_thread_name = Thread.current.name
end

# Produce a message to trigger the callback
begin
  producer.produce_sync(topic: topic, payload: "priority test")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka error: #{e.message}"
  producer.close
  exit 1
end

producer.close

# Verify callback was executed
if callback_thread_priority.nil?
  puts "Callback was not executed"
  failed = true
end

# Verify thread name follows pattern waterdrop.poller#N
unless callback_thread_name&.match?(/^waterdrop\.poller#\d+$/)
  puts "Unexpected thread name: #{callback_thread_name}"
  failed = true
end

# Verify thread priority matches configuration
if callback_thread_priority != EXPECTED_PRIORITY
  puts "Thread priority mismatch: expected #{EXPECTED_PRIORITY}, got #{callback_thread_priority}"
  failed = true
end

# Reset config to defaults for other tests
WaterDrop::Polling.setup do |config|
  config.thread_priority = 0
end

exit(failed ? 1 : 0)
