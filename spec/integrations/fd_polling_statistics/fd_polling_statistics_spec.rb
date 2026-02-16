# frozen_string_literal: true

# Integration test to verify that statistics callbacks fire in FD polling mode
# even when there is no produce activity.
#
# This tests that the IO.select timeout in the poller correctly triggers
# periodic polling which fires statistics callbacks (via librdkafka's
# statistics.interval.ms setting).
#
# The test:
# 1. Creates a producer with FD polling, high socket timeout (60s), but 1s statistics interval
# 2. Produces one message to establish connection
# 3. Waits without producing and verifies statistics callbacks fire at ~1s intervals

require "waterdrop"
require "securerandom"

# Statistics interval in milliseconds
STATISTICS_INTERVAL_MS = 1000

# How long to wait for statistics callbacks (should see multiple ticks)
WAIT_TIME_SECONDS = 5

# Minimum number of statistics callbacks expected
# With 1s interval and 5s wait, we should see at least 3-4 callbacks
MIN_EXPECTED_CALLBACKS = 3

# Track statistics callback invocations
statistics_callbacks = []
statistics_mutex = Mutex.new

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    # High socket timeout - we're testing that statistics fire without network activity
    "socket.timeout.ms": 60_000,
    # Statistics every second
    "statistics.interval.ms": STATISTICS_INTERVAL_MS
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 5_000
end

# Subscribe to statistics events
producer.monitor.subscribe("statistics.emitted") do |event|
  statistics_mutex.synchronize do
    statistics_callbacks << {
      time: Time.now,
      broker_count: event[:statistics]["brokers"]&.size || 0
    }
  end
end

topic = "it-fd-stats-#{SecureRandom.hex(6)}"

# Produce one message to establish connection and trigger initial callbacks
begin
  producer.produce_sync(topic: topic, payload: "initial message")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka not available: #{e.message}"
  exit 1
end

# Record callback count after initial produce
initial_count = statistics_mutex.synchronize { statistics_callbacks.size }

# Wait without producing - statistics should still tick
sleep(WAIT_TIME_SECONDS)

# Get final statistics
final_callbacks = statistics_mutex.synchronize { statistics_callbacks.dup }
callbacks_during_idle = final_callbacks.size - initial_count

producer.close

# Verify we got statistics callbacks during idle period
if callbacks_during_idle < MIN_EXPECTED_CALLBACKS
  puts "FAIL: Expected at least #{MIN_EXPECTED_CALLBACKS} statistics callbacks during " \
       "#{WAIT_TIME_SECONDS}s idle period, got #{callbacks_during_idle}"
  puts "Total callbacks: #{final_callbacks.size} (#{initial_count} before idle)"

  # Show timing of callbacks
  final_callbacks.each_with_index do |cb, i|
    puts "  Callback #{i + 1}: #{cb[:time]} (brokers: #{cb[:broker_count]})"
  end

  exit 1
end

# Verify callbacks are roughly 1 second apart (allow some tolerance)
if final_callbacks.size >= 2
  intervals = []
  final_callbacks.each_cons(2) do |a, b|
    intervals << (b[:time] - a[:time])
  end

  avg_interval = intervals.sum / intervals.size
  # Average should be close to 1 second (allow 0.5s tolerance for timing variance)
  if avg_interval < 0.5 || avg_interval > 1.5
    puts "WARN: Average statistics interval #{avg_interval.round(2)}s is outside expected range (0.5-1.5s)"
  end
end

puts "SUCCESS: FD polling statistics callbacks work correctly"
puts "  Total callbacks: #{final_callbacks.size}"
puts "  Callbacks during #{WAIT_TIME_SECONDS}s idle: #{callbacks_during_idle}"
exit 0
