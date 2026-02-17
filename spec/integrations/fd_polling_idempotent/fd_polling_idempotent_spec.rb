# frozen_string_literal: true

# Integration test for idempotent producer with FD polling mode
#
# Tests that:
# 1. Idempotent producer works correctly with FD polling
# 2. Messages are produced with exactly-once semantics
# 3. Delivery reports are received correctly
# 4. No duplicate delivery reports are received

require "waterdrop"
require "securerandom"

MESSAGE_COUNT = 100

delivery_reports = []
errors = []

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "enable.idempotence": true,
    "request.required.acks": "all"
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 30_000
end

# Subscribe to delivery reports
producer.monitor.subscribe("message.acknowledged") do |event|
  delivery_reports << {
    topic: event[:topic],
    partition: event[:partition],
    offset: event[:offset]
  }
end

producer.monitor.subscribe("error.occurred") do |event|
  errors << event[:error].message
end

topic = "it-fd-idempotent-#{SecureRandom.hex(6)}"

# Produce messages
handles = []
MESSAGE_COUNT.times do |i|
  handles << producer.produce_async(
    topic: topic,
    key: "key-#{i}",
    payload: "idempotent-message-#{i}"
  )
end

# Wait for all deliveries
handles.each(&:wait)

producer.close

# Validate results
if errors.any?
  puts "FAIL: Errors occurred during production"
  errors.each { |e| puts "  - #{e}" }
  exit 1
end

if delivery_reports.size != MESSAGE_COUNT
  puts "FAIL: Expected #{MESSAGE_COUNT} delivery reports, got #{delivery_reports.size}"
  exit 1
end

# Check for duplicate offsets (would indicate non-idempotent behavior)
offsets = delivery_reports.map { |r| [r[:partition], r[:offset]] }
if offsets.uniq.size != offsets.size
  puts "FAIL: Duplicate offsets detected - idempotency may be compromised"
  exit 1
end

puts "SUCCESS: Idempotent producer with FD mode delivered #{MESSAGE_COUNT} messages"
exit 0
