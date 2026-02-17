# frozen_string_literal: true

# Integration test to verify that statistics events write to the queue FD
# and wake up the poller, rather than relying on the IO.select timeout.
#
# The poller has a 1 second POLL_TIMEOUT, but if statistics write to the FD,
# we should see callbacks much more frequently when statistics.interval.ms is 100ms.
#
# This test:
# 1. Sets statistics.interval.ms to 100ms (10x faster than POLL_TIMEOUT)
# 2. Produces one message to establish connection
# 3. Waits 3 seconds and counts statistics callbacks
# 4. Verifies we got significantly more callbacks than 3 (which is what we'd get
#    if only the 1s timeout was waking us up)

require "waterdrop"
require "securerandom"

# Statistics interval - much faster than the 1s POLL_TIMEOUT
STATISTICS_INTERVAL_MS = 100

# How long to wait
WAIT_TIME_SECONDS = 3

# If only timeout wakes us up (1s), we'd see ~3 callbacks in 3 seconds
# With 100ms statistics writing to FD, we should see ~30 callbacks
# Use a threshold that clearly shows FD wakeup is working
MIN_EXPECTED_CALLBACKS = 15

# Track statistics callback invocations
statistics_callbacks = []
statistics_mutex = Mutex.new

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    # Statistics every 100ms - much faster than 1s POLL_TIMEOUT
    "statistics.interval.ms": STATISTICS_INTERVAL_MS
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 5_000
end

# Subscribe to statistics events
producer.monitor.subscribe("statistics.emitted") do |_event|
  statistics_mutex.synchronize do
    statistics_callbacks << Time.now
  end
end

topic = "it-fd-stats-wakeup-#{SecureRandom.hex(6)}"

# Produce one message to establish connection
begin
  producer.produce_sync(topic: topic, payload: "initial message")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka not available: #{e.message}"
  exit 1
end

# Clear any callbacks from connection setup
sleep(0.2)
statistics_mutex.synchronize { statistics_callbacks.clear }

# Start timing
start_time = Time.now

# Wait without producing - statistics should wake up the FD
sleep(WAIT_TIME_SECONDS)

# Get final statistics
final_callbacks = statistics_mutex.synchronize { statistics_callbacks.dup }
elapsed = Time.now - start_time

producer.close

callback_count = final_callbacks.size

# Calculate actual average interval
if final_callbacks.size >= 2
  intervals = []
  final_callbacks.each_cons(2) do |a, b|
    intervals << (b - a)
  end
  avg_interval_ms = (intervals.sum / intervals.size * 1000).round(1)
else
  avg_interval_ms = "N/A"
end

# Verify we got enough callbacks (proving FD wakeup, not just timeout)
if callback_count < MIN_EXPECTED_CALLBACKS
  puts "FAIL: Expected at least #{MIN_EXPECTED_CALLBACKS} statistics callbacks in #{elapsed.round(1)}s"
  puts "  Got only #{callback_count} callbacks"
  puts "  If we only got ~3 callbacks, the FD is NOT being signaled by statistics"
  puts "  (would be relying on 1s POLL_TIMEOUT instead of 100ms statistics interval)"
  puts "  Average interval: #{avg_interval_ms}ms (expected ~100ms)"
  exit 1
end

puts "SUCCESS: Statistics events correctly wake up FD poller"
puts "  Callbacks in #{elapsed.round(1)}s: #{callback_count}"
puts "  Average interval: #{avg_interval_ms}ms (expected ~100ms)"
puts "  This proves librdkafka writes to queue FD on statistics events"
exit 0
