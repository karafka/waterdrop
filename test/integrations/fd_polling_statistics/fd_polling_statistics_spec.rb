# frozen_string_literal: true

# Integration test to verify that statistics callbacks fire in FD polling mode
# even when there is no produce activity.
#
# This tests that the IO.select timeout in the poller correctly triggers
# periodic polling which fires statistics callbacks.

require "waterdrop"
require "securerandom"

STATISTICS_INTERVAL_MS = 1000
WAIT_TIME_SECONDS = 5
MIN_EXPECTED_CALLBACKS = 3

statistics_callbacks = []
statistics_mutex = Mutex.new

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "socket.timeout.ms": 60_000,
    "statistics.interval.ms": STATISTICS_INTERVAL_MS
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 5_000
end

producer.monitor.subscribe("statistics.emitted") do |event|
  statistics_mutex.synchronize do
    statistics_callbacks << {
      time: Time.now,
      broker_count: event[:statistics]["brokers"]&.size || 0
    }
  end
end

topic = "it-fd-stats-#{SecureRandom.hex(6)}"

begin
  producer.produce_sync(topic: topic, payload: "initial message")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka not available: #{e.message}"
  exit 1
end

initial_count = statistics_mutex.synchronize { statistics_callbacks.size }
sleep(WAIT_TIME_SECONDS)
final_callbacks = statistics_mutex.synchronize { statistics_callbacks.dup }
callbacks_during_idle = final_callbacks.size - initial_count

producer.close

if callbacks_during_idle < MIN_EXPECTED_CALLBACKS
  puts "Expected at least #{MIN_EXPECTED_CALLBACKS} statistics callbacks, got #{callbacks_during_idle}"
  exit 1
end

exit 0
