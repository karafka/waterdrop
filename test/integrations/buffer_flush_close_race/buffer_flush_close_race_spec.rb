# frozen_string_literal: true

# Integration test: a #flush stopped by a concurrent #close keeps the undispatched rest of its batch.
#
# The flushing thread dispatches the first message, then waits on a full local queue. A #close
# started from another thread marks the producer as closing, so the next dispatch attempt of the
# flush raises ProducerClosedError. The error carries the handles dispatched before the close, and
# only the messages after them are back in the buffer: none are lost and none are duplicated. No
# WaterDrop internals are stubbed.

require "waterdrop"
require "logger"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOTAL = 5

failed = false

def check(failed, condition, message)
  if condition
    puts "ok: #{message}"
    failed
  else
    puts "FAIL: #{message}"
    true
  end
end

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    # Unreachable broker so the local queue never drains; with a queue size of 1, the second
    # message of the batch waits on queue full until the close starts
    "bootstrap.servers": "127.0.0.1:9999",
    "queue.buffering.max.messages": 1,
    "message.timeout.ms": 1_000
  }
  config.wait_timeout_on_queue_full = 60_000
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

queue_full = Queue.new

producer.monitor.subscribe("error.occurred") do |event|
  queue_full << true if event[:type] == "message.produce_many_async"
end

topic = generate_topic("close-race")
batch = Array.new(TOTAL) { |i| { topic: topic, payload: "m#{i}" } }
producer.buffer_many(batch)

error = nil

flusher = Thread.new do
  producer.flush_async
rescue WaterDrop::Errors::ProducerClosedError => e
  error = e
end

queue_full.pop
producer.close
flusher.join

failed = check(failed, error.is_a?(WaterDrop::Errors::ProducerClosedError), "flush raises ProducerClosedError")
failed = check(failed, error&.dispatched&.size == 1, "error holds the one dispatched handle")
failed = check(failed, producer.messages == batch.drop(1), "buffer holds only the undispatched messages")

exit(failed ? 1 : 0)
