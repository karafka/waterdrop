# frozen_string_literal: true

# Integration test for FD polling thread lifecycle management
#
# Tests that:
# 1. Polling thread starts when first producer registers
# 2. Polling thread stops when last producer closes (no resource leakage)
# 3. A new producer can start successfully after all producers have been closed
#
# This ensures proper resource management and prevents thread leakage.

require "waterdrop"
require "securerandom"

def poller_thread_alive?
  poller = WaterDrop::Polling::Poller.instance
  thread = poller.instance_variable_get(:@thread)
  thread&.alive?
end

def poller_producer_count
  poller = WaterDrop::Polling::Poller.instance
  poller.instance_variable_get(:@producers).size
end

def create_fd_producer
  WaterDrop::Producer.new do |config|
    config.deliver = true
    config.kafka = {
      "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    }
    config.polling.mode = :fd
    config.max_wait_timeout = 5_000
  end
end

topic = "it-fd-lifecycle-#{SecureRandom.hex(6)}"

# === Test 1: Thread starts with first producer ===
puts "Test 1: Thread starts with first producer..."

unless poller_thread_alive? == false || poller_thread_alive?.nil?
  puts "FAIL: Poller thread should not be running initially"
  exit 1
end

producer1 = create_fd_producer

# Produce to ensure client is connected
begin
  producer1.produce_sync(topic: topic, payload: "message 1")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka not available: #{e.message}"
  exit 1
end

unless poller_thread_alive?
  puts "FAIL: Poller thread should be running after producer registers"
  exit 1
end

puts "  PASS: Thread started with first producer"

# === Test 2: Thread continues with multiple producers ===
puts "Test 2: Thread continues with multiple producers..."

producer2 = create_fd_producer
producer2.produce_sync(topic: topic, payload: "message 2")

if poller_producer_count != 2
  puts "FAIL: Expected 2 producers registered, got #{poller_producer_count}"
  exit 1
end

unless poller_thread_alive?
  puts "FAIL: Poller thread should still be running with 2 producers"
  exit 1
end

puts "  PASS: Thread running with multiple producers"

# === Test 3: Thread continues when one producer closes ===
puts "Test 3: Thread continues when one producer closes..."

producer1.close
sleep(0.1) # Give time for close to process

if poller_producer_count != 1
  puts "FAIL: Expected 1 producer registered after close, got #{poller_producer_count}"
  exit 1
end

unless poller_thread_alive?
  puts "FAIL: Poller thread should still be running with 1 producer remaining"
  exit 1
end

puts "  PASS: Thread continues with remaining producer"

# === Test 4: Thread stops when last producer closes ===
puts "Test 4: Thread stops when last producer closes..."

producer2.close
sleep(0.2) # Give time for thread to stop

if poller_producer_count != 0
  puts "FAIL: Expected 0 producers registered, got #{poller_producer_count}"
  exit 1
end

if poller_thread_alive?
  puts "FAIL: Poller thread should have stopped after last producer closed"
  exit 1
end

puts "  PASS: Thread stopped after last producer closed"

# === Test 5: New producer works after all closed (restart scenario) ===
puts "Test 5: New producer works after all producers were closed..."

producer3 = create_fd_producer

unless poller_thread_alive?
  puts "FAIL: Poller thread should restart when new producer registers"
  exit 1
end

# Verify the new producer actually works
begin
  producer3.produce_sync(topic: topic, payload: "message after restart")
rescue => e
  puts "FAIL: New producer failed to produce: #{e.message}"
  producer3.close
  exit 1
end

if poller_producer_count != 1
  puts "FAIL: Expected 1 producer registered, got #{poller_producer_count}"
  exit 1
end

puts "  PASS: New producer works after restart"

# Cleanup
producer3.close
sleep(0.2)

if poller_thread_alive?
  puts "FAIL: Thread should stop after final cleanup"
  exit 1
end

puts ""
puts "SUCCESS: All FD polling thread lifecycle tests passed"
exit 0
