# frozen_string_literal: true

# Integration test for the sync/async aliases of the producer API. Each `x` alias dispatches the
# way `x_async` does and each `x!` alias the way `x_sync` does: same return type, same events and
# the messages reach Kafka.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

topic = generate_topic("sync-async-aliases")
create_topic(topic)

events = Hash.new(0)

%w[
  message.produced_async
  message.produced_sync
  messages.produced_async
  messages.produced_sync
  buffer.flushed_async
  buffer.flushed_sync
].each do |event_id|
  producer.monitor.subscribe(event_id) { events[event_id] += 1 }
end

message = { topic: topic, payload: "data", key: "key", partition: 0 }
tombstone = { topic: topic, key: "key", partition: 0 }
report = Rdkafka::Producer::DeliveryReport
handle = Rdkafka::Producer::DeliveryHandle

result = producer.produce(message)
unless result.is_a?(handle)
  warn "FAIL: produce returned #{result.class}, not a delivery handle"
  exit(1)
end

unless result.wait.error.nil?
  warn "FAIL: produce did not deliver the message"
  exit(1)
end

result = producer.produce!(message)
unless result.is_a?(report)
  warn "FAIL: produce! returned #{result.class}, not a delivery report"
  exit(1)
end

results = producer.produce_many([message, message])
unless results.all?(handle)
  warn "FAIL: produce_many returned #{results.map(&:class)}, not delivery handles"
  exit(1)
end

unless results.map(&:wait).all? { |r| r.error.nil? }
  warn "FAIL: produce_many did not deliver"
  exit(1)
end

results = producer.produce_many!([message, message])
unless results.map(&:create_result).all?(report)
  warn "FAIL: produce_many! did not deliver"
  exit(1)
end

result = producer.tombstone(tombstone)
unless result.is_a?(handle)
  warn "FAIL: tombstone returned #{result.class}, not a delivery handle"
  exit(1)
end

unless result.wait.error.nil?
  warn "FAIL: tombstone did not deliver the message"
  exit(1)
end

result = producer.tombstone!(tombstone)
unless result.is_a?(report)
  warn "FAIL: tombstone! returned #{result.class}, not a delivery report"
  exit(1)
end

results = producer.tombstone_many([tombstone, tombstone])
unless results.map(&:wait).all? { |r| r.error.nil? }
  warn "FAIL: tombstone_many did not deliver"
  exit(1)
end

results = producer.tombstone_many!([tombstone, tombstone])
unless results.map(&:create_result).all?(report)
  warn "FAIL: tombstone_many! did not deliver"
  exit(1)
end

producer.buffer(message)
results = producer.flush
unless results.map(&:wait).all? { |r| r.error.nil? }
  warn "FAIL: flush did not deliver the buffer"
  exit(1)
end

producer.buffer(message)
results = producer.flush!
unless results.map(&:create_result).all?(report)
  warn "FAIL: flush! did not deliver the buffer"
  exit(1)
end

unless events["message.produced_async"] == 2
  warn "FAIL: expected 2 message.produced_async, got #{events["message.produced_async"]}"
  exit(1)
end

unless events["message.produced_sync"] == 2
  warn "FAIL: expected 2 message.produced_sync, got #{events["message.produced_sync"]}"
  exit(1)
end

unless events["messages.produced_async"] == 3
  warn "FAIL: expected 3 messages.produced_async, got #{events["messages.produced_async"]}"
  exit(1)
end

unless events["messages.produced_sync"] == 3
  warn "FAIL: expected 3 messages.produced_sync, got #{events["messages.produced_sync"]}"
  exit(1)
end

unless events["buffer.flushed_async"] == 1
  warn "FAIL: expected 1 buffer.flushed_async, got #{events["buffer.flushed_async"]}"
  exit(1)
end

unless events["buffer.flushed_sync"] == 1
  warn "FAIL: expected 1 buffer.flushed_sync, got #{events["buffer.flushed_sync"]}"
  exit(1)
end

producer.close

puts "PASS: sync/async aliases dispatch like their _sync/_async methods"
