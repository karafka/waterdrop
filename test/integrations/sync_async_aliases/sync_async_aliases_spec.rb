# frozen_string_literal: true

# Integration test for the sync/async aliases of the producer API. Each `x` alias dispatches the
# way `x_async` does and each `x!` alias the way `x_sync` does: same return type, same events and
# the messages reach Kafka.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

failed = false

# Records and prints a single assertion result instead of raising, so every check runs.
def check(failed, condition, message)
  if condition
    puts "  ok: #{message}"
    failed
  else
    puts "  FAIL: #{message}"
    true
  end
end

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
failed = check(failed, result.is_a?(handle), "produce returns a delivery handle")
failed = check(failed, result.wait.error.nil?, "produce delivers the message")

result = producer.produce!(message)
failed = check(failed, result.is_a?(report), "produce! returns a delivery report")

results = producer.produce_many([message, message])
failed = check(failed, results.all?(handle), "produce_many returns delivery handles")
failed = check(failed, results.map(&:wait).all? { |r| r.error.nil? }, "produce_many delivers")

results = producer.produce_many!([message, message])
failed = check(failed, results.map(&:create_result).all?(report), "produce_many! delivers")

result = producer.tombstone(tombstone)
failed = check(failed, result.is_a?(handle), "tombstone returns a delivery handle")
failed = check(failed, result.wait.error.nil?, "tombstone delivers the message")

result = producer.tombstone!(tombstone)
failed = check(failed, result.is_a?(report), "tombstone! returns a delivery report")

results = producer.tombstone_many([tombstone, tombstone])
failed = check(failed, results.map(&:wait).all? { |r| r.error.nil? }, "tombstone_many delivers")

results = producer.tombstone_many!([tombstone, tombstone])
failed = check(failed, results.map(&:create_result).all?(report), "tombstone_many! delivers")

producer.buffer(message)
results = producer.flush
failed = check(failed, results.map(&:wait).all? { |r| r.error.nil? }, "flush delivers the buffer")

producer.buffer(message)
results = producer.flush!
failed = check(failed, results.map(&:create_result).all?(report), "flush! delivers the buffer")

failed = check(failed, events["message.produced_async"] == 2, "produce and tombstone are async")
failed = check(failed, events["message.produced_sync"] == 2, "produce! and tombstone! are sync")
failed = check(
  failed, events["messages.produced_async"] == 3, "produce_many, tombstone_many and flush are async"
)
failed = check(
  failed, events["messages.produced_sync"] == 3, "produce_many!, tombstone_many! and flush! are sync"
)
failed = check(failed, events["buffer.flushed_async"] == 1, "flush flushes async")
failed = check(failed, events["buffer.flushed_sync"] == 1, "flush! flushes sync")

producer.close

if failed
  puts "\nFAIL: sync/async aliases do not match their _sync/_async methods"
else
  puts "\nPASS: sync/async aliases dispatch like their _sync/_async methods"
end

exit(failed ? 1 : 0)
