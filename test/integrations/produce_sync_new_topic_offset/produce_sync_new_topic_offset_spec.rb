# frozen_string_literal: true

# Integration test locking in the first-produce offset behaviour of `produce_sync`.
#
# On a brand-new topic the delivery report of the very first produce may carry an invalid offset
# (-1001) because librdkafka does not get the offset back from that first report, even though the
# message is written. Every produce after that reports a real, increasing offset. An invalid offset
# therefore means "offset unknown", never a lost message.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
INVALID_OFFSET = -1001
MESSAGES = 5

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

def consume_payloads(topic, expected)
  consumer = Rdkafka::Config.new(
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "#{topic}-verifier",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": false
  ).consumer

  consumer.subscribe(topic)

  payloads = []
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30

  while payloads.size < expected && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    message = consumer.poll(1_000)
    payloads << message.payload if message
  end

  payloads
ensure
  consumer&.close
end

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

# Deliberately not pre-created with `create_topic`: the edge case only exists for the first produce
# to a topic auto-created by that produce, so pre-creating it would stop exercising it.
topic = generate_topic("produce-sync-new-topic")

reports = Array.new(MESSAGES) do |i|
  producer.produce_sync(topic: topic, payload: "payload-#{i}")
end

producer.close

first = reports.first
rest = reports[1..]
payloads = consume_payloads(topic, MESSAGES)

failed = check(failed, reports.size == MESSAGES, "a report per message")
failed = check(failed, reports.none?(&:error), "no report carries an error")
failed = check(
  failed, reports.all? { |report| report.topic_name == topic }, "reports name the produced topic"
)
failed = check(
  failed,
  first.offset == INVALID_OFFSET || first.offset >= 0,
  "first offset is real or the invalid sentinel"
)
failed = check(failed, rest.all? { |report| report.offset >= 0 }, "later offsets are valid")
failed = check(
  failed, rest.each_cons(2).all? { |a, b| b.offset > a.offset }, "later offsets increase"
)
failed = check(
  failed,
  Array.new(MESSAGES) { |i| "payload-#{i}" }.all? { |payload| payloads.include?(payload) },
  "every message landed, including the first"
)

if failed
  puts "\nFAIL: produce_sync first-produce offset handling regressed"
else
  puts "\nPASS: produce_sync lands the first message and reports real increasing offsets afterwards"
end

exit(failed ? 1 : 0)
