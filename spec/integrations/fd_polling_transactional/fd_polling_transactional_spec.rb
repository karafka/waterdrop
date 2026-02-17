# frozen_string_literal: true

# Integration test for transactional producer with FD polling mode
#
# Tests that:
# 1. Transactional producer works correctly with FD polling
# 2. Transactions commit successfully
# 3. Multiple transactions can be performed sequentially
# 4. Delivery reports are received correctly

require "waterdrop"
require "securerandom"

MESSAGES_PER_TRANSACTION = 5
TRANSACTION_COUNT = 10

delivery_reports = []
errors = []

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "transactional.id": "it-fd-tx-#{SecureRandom.hex(6)}"
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 30_000
end

producer.monitor.subscribe("message.acknowledged") do |event|
  delivery_reports << event[:offset]
end

producer.monitor.subscribe("error.occurred") do |event|
  errors << event[:error].message
end

topic = "it-fd-transactional-#{SecureRandom.hex(6)}"

TRANSACTION_COUNT.times do |tx_index|
  producer.transaction do
    MESSAGES_PER_TRANSACTION.times do |msg_index|
      producer.produce_async(
        topic: topic,
        key: "tx-#{tx_index}",
        payload: "message-#{tx_index}-#{msg_index}"
      )
    end
  end
rescue => e
  errors << e.message
end

producer.close

expected_messages = TRANSACTION_COUNT * MESSAGES_PER_TRANSACTION
failed = false

if errors.any?
  puts "Errors occurred during transactions: #{errors.inspect}"
  failed = true
end

if delivery_reports.size != expected_messages
  puts "Expected #{expected_messages} delivery reports, got #{delivery_reports.size}"
  failed = true
end

exit(failed ? 1 : 0)
