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

unless reports.size == MESSAGES
  warn "FAIL: a report per message"
  exit(1)
end

unless reports.none?(&:error)
  warn "FAIL: no report carries an error"
  exit(1)
end

unless reports.all? { |report| report.topic_name == topic }
  warn "FAIL: reports name the produced topic"
  exit(1)
end

unless first.offset == INVALID_OFFSET || first.offset >= 0
  warn "FAIL: first offset is real or the invalid sentinel"
  exit(1)
end

unless rest.all? { |report| report.offset >= 0 }
  warn "FAIL: later offsets are valid"
  exit(1)
end

unless rest.each_cons(2).all? { |a, b| b.offset > a.offset }
  warn "FAIL: later offsets increase"
  exit(1)
end

unless Array.new(MESSAGES) { |i| "payload-#{i}" }.all? { |payload| payloads.include?(payload) }
  warn "FAIL: every message landed, including the first"
  exit(1)
end

puts "PASS: produce_sync lands the first message and reports real increasing offsets afterwards"
