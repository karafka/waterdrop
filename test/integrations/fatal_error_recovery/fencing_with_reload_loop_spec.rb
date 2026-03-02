# frozen_string_literal: true

# Integration test demonstrating the infinite loop risk when removing :fenced from
# non_reloadable_errors. This test shows what happens when a user explicitly configures
# non_reloadable_errors to [] and encounters a fencing error - it creates an infinite reload loop
# that we limit to 10 attempts.
#
# This is exactly the scenario the WARNING in config.rb explains:
# "Attempting to reload on fenced errors creates: produce -> fenced -> reload -> produce ->
#  fenced -> reload (infinite loop)"

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
# How many attempts of endless loop before we're done
MAX_RELOAD_ATTEMPTS = 10
# Same ID for both producers
TRANSACTIONAL_ID = "fence-loop-test-#{SecureRandom.uuid}".freeze

# Shared configuration for both producers
def configure_producer
  lambda do |config|
    config.kafka = {
      "bootstrap.servers": BOOTSTRAP_SERVERS,
      "transactional.id": TRANSACTIONAL_ID,
      "transaction.timeout.ms": 30_000,
      "message.timeout.ms": 30_000
    }
    config.max_wait_timeout = 5_000
    config.logger = Logger.new($stdout, level: Logger::INFO)
    config.reload_on_transaction_fatal_error = true
    # DANGEROUS: Removing fenced from non_reloadable_errors - causes infinite loop on fencing!
    config.non_reloadable_errors = []
    # Limit retries to prevent actual infinite loop in test
    config.max_attempts_on_transaction_fatal_error = MAX_RELOAD_ATTEMPTS
    config.wait_backoff_on_transaction_fatal_error = 10 # Fast backoff for testing
  end
end

# Track instrumentation events
reload_events = []
error_events = []

# Create two producers with the same transactional.id
producer1 = WaterDrop::Producer.new(&configure_producer)
producer2 = WaterDrop::Producer.new(&configure_producer)

# Subscribe to events for both producers
[producer1, producer2].each do |producer|
  producer.monitor.subscribe("producer.reloaded") { |event| reload_events << event }
  producer.monitor.subscribe("error.occurred") { |event| error_events << event }
end

topic_name = "it-fence-loop-#{SecureRandom.hex(6)}"

# First transaction with producer1
producer1.transaction do
  producer1.produce_sync(topic: topic_name, payload: "message1")
end

begin
  # This transaction will fence producer1
  producer2.transaction do
    producer2.produce_sync(topic: topic_name, payload: "message2")
  end

  producer1.transaction do
    producer1.produce_sync(topic: topic_name, payload: "message3")
  end

  # This will trigger reload loop: fenced -> reload -> fenced -> reload...
  producer2.transaction do
    producer2.produce_sync(topic: topic_name, payload: "message2")
  end
rescue Rdkafka::RdkafkaError => e
  exit(1) unless e.code == :fenced

  retry unless reload_events.size >= MAX_RELOAD_ATTEMPTS
end

producer1.close
producer2.close

# Verify results
# Count fencing errors (error might be wrapped in ProduceError)
fencing_errors = error_events.count do |event|
  error = event[:error]
  (error.respond_to?(:code) && error.code == :fenced) ||
    (error.respond_to?(:cause) && error.cause.respond_to?(:code) && error.cause.code == :fenced)
end

success = reload_events.size >= MAX_RELOAD_ATTEMPTS && fencing_errors.positive?

exit(success ? 0 : 1)
