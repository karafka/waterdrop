# frozen_string_literal: true

# Integration test to verify that FD polling doesn't block produce calls
# This is a critical safety test - if poll_drain holds the mutex too long,
# produce calls would be delayed, causing latency spikes.
#
# These tests ensure that even with max_poll_time set to 100ms, individual
# produce calls complete quickly (< 20ms) because the mutex in with_inner
# is only held briefly for counter operations, not during the actual poll.

require "waterdrop"
require "securerandom"

# Maximum acceptable latency for a single produce call (in seconds)
MAX_PRODUCE_LATENCY = 0.020

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092") }
  config.polling.mode = :fd
  config.polling.fd.max_time = 100
  config.max_wait_timeout = 5_000
end

topic = "it-fd-non-blocking-#{SecureRandom.hex(6)}"

begin
  producer.produce_sync(topic: topic, payload: "warmup")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka not available: #{e.message}"
  exit 1
end

failed = false

# Test 1: Verify produce calls are not blocked while polling is active
latencies = []
errors = []
stop_flag = false

producer_thread = Thread.new do
  100.times do |i|
    break if stop_flag

    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    begin
      producer.produce_async(topic: topic, payload: "message-#{i}")
    rescue => e
      errors << e
    end
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    latencies << elapsed
  end
end

load_thread = Thread.new do
  handles = []
  200.times do |i|
    break if stop_flag

    begin
      handles << producer.produce_async(topic: topic, payload: "load-#{i}")
    rescue
      # Ignore errors in load generation
    end
    sleep(0.001)
  end

  handles.each do |h|
    h.wait
  rescue
    nil
  end
end

producer_thread.join(10)
stop_flag = true
load_thread.join(5)

unless errors.empty?
  puts "Produce errors: #{errors.map(&:message).join(", ")}"
  producer.close
  exit 1
end

if latencies.empty?
  puts "No latency data collected"
  producer.close
  exit 1
end

max_latency = latencies.max

if max_latency >= MAX_PRODUCE_LATENCY
  puts "Max produce latency #{(max_latency * 1000).round(2)}ms exceeded threshold"
  failed = true
end

# Test 2: Maintain low latency even with high event volume
burst_handles = 500.times.map do |i|
  producer.produce_async(topic: topic, payload: "burst-#{i}")
end

latencies_high_volume = []
50.times do |i|
  start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  producer.produce_async(topic: topic, payload: "measure-#{i}")
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
  latencies_high_volume << elapsed
end

burst_handles.each do |h|
  h.wait
rescue
  nil
end

max_latency_high_volume = latencies_high_volume.max

if max_latency_high_volume >= MAX_PRODUCE_LATENCY
  puts "Max produce latency #{(max_latency_high_volume * 1000).round(2)}ms exceeded threshold during high volume"
  failed = true
end

producer.close

exit(failed ? 1 : 0)
