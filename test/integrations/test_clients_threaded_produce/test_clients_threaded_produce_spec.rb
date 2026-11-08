# frozen_string_literal: true

# Integration test for producing from many threads through the test clients.
#
# `Clients::Buffered` (set as `client_class`) and `Clients::Dummy` (`deliver: false`) replace the
# Kafka client when testing producing code. When several threads produce through one producer,
# `Clients::Buffered` must keep every message in `#messages` and `#messages_for`, and both clients
# must give each message of a topic partition its own offset. This must hold on engines without a
# GVL (JRuby) as well.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
THREADS = 8
PER_THREAD = 500
TOTAL = THREADS * PER_THREAD

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

# Produces from many threads at once and returns the offsets of all the delivery reports
def produce_from_threads(producer)
  start = Queue.new

  threads = Array.new(THREADS) do
    Thread.new do
      start.pop
      Array.new(PER_THREAD) { producer.produce_sync(topic: "threaded", payload: "x").offset }
    end
  end

  THREADS.times { start << true }
  threads.flat_map(&:value)
end

buffered = WaterDrop::Producer.new do |config|
  config.client_class = WaterDrop::Clients::Buffered
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

offsets = produce_from_threads(buffered)

failed = check(failed, buffered.client.messages.size == TOTAL, "buffered keeps all #{TOTAL} messages")
failed = check(
  failed,
  buffered.client.messages_for("threaded").size == TOTAL,
  "buffered keeps all #{TOTAL} messages of the topic"
)
failed = check(failed, offsets.sort == (0...TOTAL).to_a, "buffered assigns each offset once")

buffered.close

dummy = WaterDrop::Producer.new do |config|
  config.deliver = false
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

offsets = produce_from_threads(dummy)

failed = check(failed, offsets.sort == (0...TOTAL).to_a, "dummy assigns each offset once")

dummy.close

exit(failed ? 1 : 0)
