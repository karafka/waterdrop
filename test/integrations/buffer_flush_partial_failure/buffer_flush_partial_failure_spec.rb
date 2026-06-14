# frozen_string_literal: true

# Integration test: #flush must not silently drop valid buffered messages when the dispatch fails.
#
# #flush swaps the internal buffer out (`@messages = []`) under @buffer_mutex and then dispatches the
# taken batch via produce_many_*. Any failure there used to discard the whole taken batch - the
# messages were already removed from @messages and were never restored, so valid, already-buffered
# messages were lost permanently with only an exception surfaced. Two real, unmocked triggers:
#
#   1. A single invalid message in the buffer makes produce_many_* raise MessageInvalidError BEFORE
#      anything is dispatched, taking every valid message in the batch down with it.
#   2. A mid-batch inline error (local queue full) leaves the unsent remainder of the batch lost.
#
# After the fix, a failed flush re-buffers the messages that never reached librdkafka, so they can be
# retried and are not lost. No WaterDrop internals are stubbed.

require "waterdrop"
require "logger"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

failures = []

# --- Trigger 1: a validation failure must not take the valid messages down with it ---
validation_producer = WaterDrop::Producer.new do |config|
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

topic = generate_topic("wd02")
validation_producer.buffer(topic: topic, payload: "valid-1")
validation_producer.buffer(topic: topic, payload: "valid-2")
# Invalid: payload must be a String or nil. buffer does not validate, so it lands in the buffer and
# only blows up when flush dispatches the batch.
validation_producer.buffer(topic: topic, payload: 123)

begin
  validation_producer.flush_sync
  failures << "validation: expected flush_sync to raise on the invalid message"
rescue WaterDrop::Errors::MessageInvalidError, WaterDrop::Errors::ProduceManyError
  # expected
end

kept = validation_producer.messages.size
failures << "validation: lost buffered messages (buffer=#{kept}, expected 3)" unless kept == 3

validation_producer.purge
validation_producer.close

# --- Trigger 2: a mid-batch queue-full must not lose the unsent valid messages ---
TOTAL = 20
queue_full_producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    # Unreachable broker so the local queue never drains; combined with a queue size of 1, every
    # produce after the first hits an inline queue-full error mid-batch.
    "bootstrap.servers": "127.0.0.1:9999",
    "queue.buffering.max.messages": 1,
    "message.timeout.ms": 120_000
  }
  # Raise immediately on queue full instead of backing off and retrying, so the batch fails fast.
  config.wait_on_queue_full = false
  config.max_wait_timeout = 2_000
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

qf_topic = generate_topic("wd02-qf")
TOTAL.times { |i| queue_full_producer.buffer(topic: qf_topic, payload: "m#{i}") }

begin
  queue_full_producer.flush_async
  failures << "queue full: expected flush_async to raise once the local queue is full"
rescue WaterDrop::Errors::ProduceManyError
  # expected
end

kept_qf = queue_full_producer.messages.size

if kept_qf.zero?
  failures << "queue full: dropped all #{TOTAL} buffered messages on a mid-batch failure"
elsif kept_qf >= TOTAL
  failures << "queue full: nothing was dispatched (buffer=#{kept_qf}); test did not exercise a partial dispatch"
end

queue_full_producer.purge
queue_full_producer.close

if failures.empty?
  puts "PASS: flush retains unsent buffered messages on dispatch failure (kept #{kept}/3 and #{kept_qf}/#{TOTAL})"
  exit(0)
else
  failures.each { |failure| warn "FAIL: #{failure}" }
  exit(1)
end
