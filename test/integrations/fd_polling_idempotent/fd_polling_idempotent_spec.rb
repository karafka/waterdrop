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

handles = []
MESSAGE_COUNT.times do |i|
  handles << producer.produce_async(
    topic: topic,
    key: "key-#{i}",
    payload: "idempotent-message-#{i}"
  )
end

handles.each(&:wait)
producer.close

failed = false

if errors.any?
  puts "Errors occurred during production: #{errors.inspect}"
  failed = true
end

if delivery_reports.size != MESSAGE_COUNT
  puts "Expected #{MESSAGE_COUNT} delivery reports, got #{delivery_reports.size}"
  failed = true
end

offsets = delivery_reports.map { |r| [r[:partition], r[:offset]] }
if offsets.uniq.size != offsets.size
  puts "Duplicate offsets detected"
  failed = true
end

exit(failed ? 1 : 0)
