# frozen_string_literal: true

# Integration test for FD polling thread lifecycle management
#
# Tests that:
# 1. Polling thread starts when first producer registers
# 2. Polling thread stops when last producer closes (no resource leakage)
# 3. A new producer can start successfully after all producers have been closed

require "waterdrop"
require "securerandom"

def poller_thread_alive?
  WaterDrop::Polling::Poller.instance.alive?
end

def poller_producer_count
  WaterDrop::Polling::Poller.instance.count
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
failed = false

# Test 1: Thread starts with first producer
if poller_thread_alive?
  puts "Thread should not be running initially"
  failed = true
end

producer1 = create_fd_producer

begin
  producer1.produce_sync(topic: topic, payload: "message 1")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka not available: #{e.message}"
  exit 1
end

unless poller_thread_alive?
  puts "Thread should be running after producer registers"
  failed = true
end

# Test 2: Thread continues with multiple producers
producer2 = create_fd_producer
producer2.produce_sync(topic: topic, payload: "message 2")

if poller_producer_count != 2
  puts "Expected 2 producers registered, got #{poller_producer_count}"
  failed = true
end

# Test 3: Thread continues when one producer closes
producer1.close
sleep(0.1)

if poller_producer_count != 1
  puts "Expected 1 producer registered after close, got #{poller_producer_count}"
  failed = true
end

unless poller_thread_alive?
  puts "Thread should still be running with 1 producer remaining"
  failed = true
end

# Test 4: Thread stops when last producer closes
producer2.close
sleep(0.2)

if poller_producer_count != 0
  puts "Expected 0 producers registered, got #{poller_producer_count}"
  failed = true
end

if poller_thread_alive?
  puts "Thread should have stopped after last producer closed"
  failed = true
end

# Test 5: New producer works after all closed
producer3 = create_fd_producer

begin
  producer3.produce_sync(topic: topic, payload: "message after restart")
rescue => e
  puts "New producer failed to produce: #{e.message}"
  producer3.close
  exit 1
end

unless poller_thread_alive?
  puts "Thread should restart when new producer registers"
  failed = true
end

producer3.close
sleep(0.2)

if poller_thread_alive?
  puts "Thread should stop after final cleanup"
  failed = true
end

exit(failed ? 1 : 0)
