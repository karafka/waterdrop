# frozen_string_literal: true

# Integration test for dedicated poller instances
#
# Tests that:
# 1. Producers can use the default global singleton poller
# 2. Producers can use a dedicated custom poller for isolation
# 3. Mixing default and dedicated pollers works correctly
# 4. Dedicated pollers can be shared between multiple producers
# 5. Each poller runs its own thread

require "waterdrop"
require "securerandom"

topic = "it-fd-dedicated-poller-#{SecureRandom.hex(6)}"
failed = false

# Track callbacks per producer
callbacks = {
  default1: [],
  default2: [],
  dedicated1: [],
  dedicated2: [],
  shared1: [],
  shared2: []
}
callbacks_mutex = Mutex.new

# Create a dedicated poller for isolation
dedicated_poller = WaterDrop::Polling::Poller.new

# Create another dedicated poller to be shared between producers
shared_poller = WaterDrop::Polling::Poller.new

# Producer 1: Uses default global singleton poller
producer_default1 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 10_000
end

# Producer 2: Also uses default global singleton poller
producer_default2 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 10_000
end

# Producer 3: Uses dedicated poller (isolated)
producer_dedicated1 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
  }
  config.polling.mode = :fd
  config.polling.poller = dedicated_poller
  config.max_wait_timeout = 10_000
end

# Producer 4: Uses another dedicated poller (for callback isolation test)
producer_dedicated2 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
  }
  config.polling.mode = :fd
  config.polling.poller = WaterDrop::Polling::Poller.new
  config.max_wait_timeout = 10_000
end

# Producer 5 and 6: Share the same dedicated poller
producer_shared1 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
  }
  config.polling.mode = :fd
  config.polling.poller = shared_poller
  config.max_wait_timeout = 10_000
end

producer_shared2 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
  }
  config.polling.mode = :fd
  config.polling.poller = shared_poller
  config.max_wait_timeout = 10_000
end

# Subscribe to callbacks to track which producer received which callback
{
  default1: producer_default1,
  default2: producer_default2,
  dedicated1: producer_dedicated1,
  dedicated2: producer_dedicated2,
  shared1: producer_shared1,
  shared2: producer_shared2
}.each do |name, producer|
  producer.monitor.subscribe("message.acknowledged") do |_event|
    callbacks_mutex.synchronize do
      callbacks[name] << {
        time: Process.clock_gettime(Process::CLOCK_MONOTONIC),
        thread: Thread.current.name
      }
    end
  end
end

# Test 1: Verify poller assignments
unless producer_default1.poller == WaterDrop::Polling::Poller.instance
  puts "producer_default1 should use singleton poller"
  failed = true
end

unless producer_default2.poller == WaterDrop::Polling::Poller.instance
  puts "producer_default2 should use singleton poller"
  failed = true
end

unless producer_dedicated1.poller == dedicated_poller
  puts "producer_dedicated1 should use dedicated_poller"
  failed = true
end

unless producer_shared1.poller == shared_poller
  puts "producer_shared1 should use shared_poller"
  failed = true
end

unless producer_shared2.poller == shared_poller
  puts "producer_shared2 should use shared_poller"
  failed = true
end

# Test 2: Produce messages from all producers
begin
  producer_default1.produce_sync(topic: topic, payload: "default1")
  producer_default2.produce_sync(topic: topic, payload: "default2")
  producer_dedicated1.produce_sync(topic: topic, payload: "dedicated1")
  producer_dedicated2.produce_sync(topic: topic, payload: "dedicated2")
  producer_shared1.produce_sync(topic: topic, payload: "shared1")
  producer_shared2.produce_sync(topic: topic, payload: "shared2")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka error: #{e.message}"
  failed = true
end

# Wait for all callbacks
sleep(0.5)

# Test 3: Verify all producers received callbacks
callbacks.each do |name, cb_list|
  if cb_list.empty?
    puts "No callbacks received for #{name}"
    failed = true
  end
end

# Test 4: Verify thread isolation - producers using same poller share a thread
default_threads = callbacks_mutex.synchronize do
  (callbacks[:default1] + callbacks[:default2]).map { |c| c[:thread] }.uniq
end

shared_threads = callbacks_mutex.synchronize do
  (callbacks[:shared1] + callbacks[:shared2]).map { |c| c[:thread] }.uniq
end

# Default producers should share the same poller thread
if default_threads.size > 1
  puts "Default producers should share the same poller thread, got: #{default_threads}"
  failed = true
end

# Shared producers should share their poller thread
if shared_threads.size > 1
  puts "Shared producers should share the same poller thread, got: #{shared_threads}"
  failed = true
end

# Test 4b: Verify each poller has a unique thread name with incremental ID
dedicated1_thread_names = callbacks_mutex.synchronize do
  callbacks[:dedicated1].map { |c| c[:thread] }.uniq
end

# Each thread name should follow the pattern "waterdrop.poller#N"
all_thread_names = (default_threads + dedicated1_thread_names + shared_threads).uniq

all_thread_names.each do |name|
  unless name.match?(/^waterdrop\.poller#\d+$/)
    puts "Thread name '#{name}' doesn't match expected pattern 'waterdrop.poller#N'"
    failed = true
  end
end

# Different pollers should have different thread names
if all_thread_names.size < 3
  puts "Expected at least 3 different thread names (default, dedicated, shared), got: #{all_thread_names}"
  failed = true
end

# Clean up
producer_default1.close
producer_default2.close
producer_dedicated1.close
producer_dedicated2.close
producer_shared1.close
producer_shared2.close

# Test 5: Verify dedicated pollers stop when their last producer closes
sleep(0.5)

if dedicated_poller.alive?
  puts "dedicated_poller should have stopped after its producer closed"
  failed = true
end

if shared_poller.alive?
  puts "shared_poller should have stopped after both its producers closed"
  failed = true
end

# The singleton poller may or may not be alive depending on other tests
# So we don't check it here

exit(failed ? 1 : 0)
