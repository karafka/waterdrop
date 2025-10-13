# frozen_string_literal: true

# Integration test demonstrating how to escape producer fencing by using the producer.reload
# event to modify transactional.id. This test shows the recommended pattern for handling fencing
# in production environments.
#
# When a producer gets fenced, simply reloading with the same transactional.id creates an
# infinite loop. But by subscribing to the producer.reload event and modifying the
# transactional.id, the producer can escape fencing and continue operating with a new identity.

require 'waterdrop'
require 'logger'
require 'securerandom'

BOOTSTRAP_SERVERS = ENV.fetch('BOOTSTRAP_SERVERS', '127.0.0.1:9092')
# Same ID for both producers initially
TRANSACTIONAL_ID = "fence-escape-test-#{SecureRandom.uuid}".freeze

# Track instrumentation events
reload_events = []
error_events = []

# Create first producer with reload enabled
producer1 = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'transactional.id': TRANSACTIONAL_ID,
    'transaction.timeout.ms': 30_000,
    'message.timeout.ms': 30_000
  }
  config.max_wait_timeout = 5_000
  config.logger = Logger.new($stdout, level: Logger::INFO)
  config.reload_on_transaction_fatal_error = true
  # IMPORTANT: Remove :fenced from non_reloadable_errors to allow reload attempts
  config.non_reloadable_errors = []
  config.max_attempts_on_transaction_fatal_error = 5
  config.wait_backoff_on_transaction_fatal_error = 100
end

# Subscribe to producer.reload event and modify transactional.id to escape fencing
producer1.monitor.subscribe('producer.reload') do |event|
  # When fenced, rotate to a new transactional.id
  if event[:error].code == :fenced
    new_id = "#{TRANSACTIONAL_ID}-recovered-#{Time.now.to_i}"
    event[:caller].config.kafka[:'transactional.id'] = new_id
    puts "Fencing detected! Rotating transactional.id to: #{new_id}"
  end
end

producer1.monitor.subscribe('producer.reloaded') { |event| reload_events << event }
producer1.monitor.subscribe('error.occurred') { |event| error_events << event }

# Create second producer with same ID to cause fencing
producer2 = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'transactional.id': TRANSACTIONAL_ID,
    'transaction.timeout.ms': 30_000,
    'message.timeout.ms': 30_000
  }
  config.max_wait_timeout = 5_000
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

topic_name = "it-fence-escape-#{SecureRandom.hex(6)}"

# First transaction with producer1
producer1.transaction do
  producer1.produce_sync(topic: topic_name, payload: 'message1')
end

# This transaction will fence producer1
producer2.transaction do
  producer2.produce_sync(topic: topic_name, payload: 'message2')
end

# This should trigger reload with transactional.id change and succeed
begin
  producer1.transaction do
    producer1.produce_sync(topic: topic_name, payload: 'message3-recovered')
  end
rescue Rdkafka::RdkafkaError => e
  puts "Failed after reload: #{e.message}"
  exit(1)
end

producer1.close
producer2.close

# Verify results
# Should have exactly 1 reload (not multiple like in the loop case)
success = reload_events.size == 1 && reload_events.first[:attempt] == 1

puts "Reload events: #{reload_events.size}"
puts "Success: #{success}"

exit(success ? 0 : 1)
