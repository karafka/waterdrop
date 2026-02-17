# frozen_string_literal: true

# Integration test to verify that statistics events write to the queue FD
# and wake up the poller, rather than relying on the IO.select timeout.
#
# The poller has a 1 second POLL_TIMEOUT, but if statistics write to the FD,
# we should see callbacks much more frequently when statistics.interval.ms is 100ms.

require "waterdrop"
require "securerandom"

STATISTICS_INTERVAL_MS = 100
WAIT_TIME_SECONDS = 3
MIN_EXPECTED_CALLBACKS = 15

statistics_callbacks = []
statistics_mutex = Mutex.new

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "statistics.interval.ms": STATISTICS_INTERVAL_MS
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 5_000
end

producer.monitor.subscribe("statistics.emitted") do |_event|
  statistics_mutex.synchronize do
    statistics_callbacks << Time.now
  end
end

topic = "it-fd-stats-wakeup-#{SecureRandom.hex(6)}"

begin
  producer.produce_sync(topic: topic, payload: "initial message")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka not available: #{e.message}"
  exit 1
end

sleep(0.2)
statistics_mutex.synchronize { statistics_callbacks.clear }

sleep(WAIT_TIME_SECONDS)

final_callbacks = statistics_mutex.synchronize { statistics_callbacks.dup }
producer.close

callback_count = final_callbacks.size

if callback_count < MIN_EXPECTED_CALLBACKS
  puts "Expected at least #{MIN_EXPECTED_CALLBACKS} statistics callbacks, got #{callback_count}"
  exit 1
end

exit 0
