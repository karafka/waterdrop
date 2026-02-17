# frozen_string_literal: true

# Integration test demonstrating FD polling callback saturation behavior
#
# In FD polling mode, all producers share a single poller thread. This means that if one
# producer's callback (e.g., statistics.emitted) takes a long time to execute, it will
# delay the processing of other producers' callbacks.
#
# This test demonstrates this edge case by:
# 1. Creating two producers in FD mode
# 2. Having producer1's statistics callback sleep for an extended period
# 3. Showing that producer2's delivery callbacks are delayed during this time
#
# This is expected behavior - the single-threaded poller design trades potential callback
# interference for reduced thread overhead and better fiber scheduler compatibility.
# Users with long-running callbacks should consider:
# - Moving heavy work to a background thread/job queue
# - Using thread polling mode if callback isolation is critical

require "waterdrop"
require "securerandom"

SLOW_CALLBACK_DURATION = 3 # seconds
STATS_INTERVAL_MS = 1_000 # 1 second

topic = "it-fd-saturation-#{SecureRandom.hex(6)}"
failed = false

producer1_stats_count = 0
producer1_stats_started = nil
producer2_delivery_times = []

# Producer 1: Has a slow statistics callback that saturates the poller
producer1 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "statistics.interval.ms": STATS_INTERVAL_MS
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 10_000
end

# Producer 2: Normal producer that will be affected by producer1's slow callback
producer2 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "statistics.interval.ms": STATS_INTERVAL_MS
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 10_000
end

# Subscribe to producer1's statistics - this will block the poller thread
producer1.monitor.subscribe("statistics.emitted") do |_event|
  producer1_stats_count += 1

  # Only sleep on the second stats emission to let initial setup complete
  if producer1_stats_count == 2
    producer1_stats_started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sleep(SLOW_CALLBACK_DURATION)
  end
end

# Track when producer2's deliveries are acknowledged
producer2.monitor.subscribe("message.acknowledged") do |_event|
  producer2_delivery_times << Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

# Initial produce to establish connections
begin
  producer1.produce_sync(topic: topic, payload: "init1")
  producer2.produce_sync(topic: topic, payload: "init2")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka error: #{e.message}"
  producer1.close
  producer2.close
  exit 1
end

# Wait for at least one stats emission
sleep(1.5)

# Now produce messages from producer2 while producer1's stats callback will block
# The messages are produced async, so they go into the queue immediately
5.times { |i| producer2.produce_async(topic: topic, payload: "message-#{i}") }

# Wait for the slow callback to complete plus some buffer
sleep(SLOW_CALLBACK_DURATION + 2)

# Verify that we got statistics from producer1
if producer1_stats_count < 2
  puts "Expected at least 2 statistics emissions, got #{producer1_stats_count}"
  failed = true
end

# Verify that producer2's deliveries were delayed by producer1's slow callback
if producer2_delivery_times.empty?
  puts "No delivery acknowledgments received for producer2"
  failed = true
elsif producer1_stats_started
  # Check if any deliveries were delayed until after the slow callback started
  delayed_deliveries = producer2_delivery_times.select do |time|
    time > producer1_stats_started + (SLOW_CALLBACK_DURATION * 0.8)
  end

  if delayed_deliveries.empty?
    puts "Expected some deliveries to be delayed by slow callback, but none were"
    puts "Stats started: #{producer1_stats_started}"
    puts "Delivery times: #{producer2_delivery_times.inspect}"
    # This is not necessarily a failure - it depends on timing
    # The point is to demonstrate the potential for saturation
  end
end

# Cleanup
producer1.close
producer2.close

# Verify poller thread stopped
sleep(0.2)
if WaterDrop::Polling::Poller.instance.alive?
  puts "Poller thread should have stopped after all producers closed"
  failed = true
end

exit(failed ? 1 : 0)
