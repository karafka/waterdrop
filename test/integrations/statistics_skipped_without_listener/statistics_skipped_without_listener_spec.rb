# frozen_string_literal: true

# Integration test verifying that when no listener is subscribed to `statistics.emitted`
# before the rdkafka client is built, librdkafka statistics are suppressed entirely and
# attempting to subscribe a listener afterwards raises.
#
# This is the end-to-end version of the optimization that avoids parsing statistics JSON
# and materializing decorated hashes when nobody cares about them.

require "waterdrop"
require "securerandom"

STATISTICS_INTERVAL_MS = 100
WAIT_TIME_SECONDS = 2

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "statistics.interval.ms": STATISTICS_INTERVAL_MS
  }
  config.max_wait_timeout = 5_000
end

# Triggers the lazy rdkafka client build. Because no `statistics.emitted` listener was
# subscribed beforehand, WaterDrop should force `statistics.interval.ms` to 0 on the
# underlying client regardless of the user's configuration.
topic = generate_topic("statistics-skipped-without-listener")

begin
  producer.produce_sync(topic: topic, payload: "initial message")
rescue Rdkafka::RdkafkaError => e
  puts "Kafka not available: #{e.message}"
  exit 1
end

# Wait long enough that, if statistics were enabled, dozens of events would have fired.
sleep(WAIT_TIME_SECONDS)

# Even the low-level statistics callback registry must not hold an entry for this producer,
# since we never registered the statistics callback at all.
registered = ::Karafka::Core::Instrumentation
  .statistics_callbacks
  .instance_variable_get(:@callbacks)
  .key?(producer.id)

if registered
  puts "Expected no statistics callback to be registered for producer #{producer.id}"
  producer.close
  exit 1
end

# A late subscription must raise, telling the user loud and clear that they missed the
# window where statistics could have been enabled.
raised = false

begin
  producer.monitor.subscribe("statistics.emitted") { |_event| }
rescue WaterDrop::Errors::StatisticsNotEnabledError
  raised = true
end

unless raised
  puts "Expected late subscription to statistics.emitted to raise StatisticsNotEnabledError"
  producer.close
  exit 1
end

producer.close

exit 0
