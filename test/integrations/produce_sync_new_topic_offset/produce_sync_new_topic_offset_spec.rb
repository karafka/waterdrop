# frozen_string_literal: true

# Integration test locking in the first-produce offset edge case of `produce_sync`.
#
# On a brand-new topic - one auto-created by the produce itself - librdkafka may not get the
# offset back in that very first delivery report, so `DeliveryReport#offset` comes back as the
# invalid sentinel (-1001) even though the message is written to the log. Every subsequent
# produce to the now-established topic reports a real, increasing offset.
#
# `produce_sync` is correct here and is what we use; the invalid offset is a reporting artifact
# of the first produce, not a lost message. Consumers of the delivery report (e.g. a UI linking
# to the produced message) must therefore treat an invalid offset as "unknown", not as a failure.
#
# What is asserted is only what is actually guaranteed: the first message lands, later offsets
# are real and increasing, and every report names the topic we published to. Whether the first
# offset comes back invalid is broker/librdkafka timing, so it is reported informationally
# rather than required - requiring it would make this spec flaky.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# librdkafka's RD_KAFKA_OFFSET_INVALID - what a delivery report carries when the broker did not
# hand the offset back for that message.
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

# Reads the topic from the beginning and returns the payloads it managed to fetch. Used to prove
# the first message lands even when its delivery report carries an invalid offset.
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

# Deliberately NOT pre-created with the `create_topic` helper: the whole point of this spec is the
# first produce to a topic that does not exist yet and is auto-created by that produce. Creating it
# up front would make the topic established before the first produce and the edge case would never
# be exercised.
topic = generate_topic("produce-sync-new-topic")

reports = Array.new(MESSAGES) do |i|
  producer.produce_sync(topic: topic, payload: "payload-#{i}")
end

producer.close

first = reports.first
rest = reports[1..]

failed = check(failed, reports.size == MESSAGES, "produce_sync returned a report for every message")
failed = check(failed, reports.none?(&:error), "no delivery report carries an error")

# The delivery report must name the topic we actually published to - anything reading the report
# to locate the message (a UI deep link, for instance) depends on this.
failed = check(
  failed,
  reports.all? { |report| report.topic_name == topic },
  "every delivery report names the topic we published to"
)

# The documented edge case: the first offset may be the invalid sentinel. Both outcomes are
# acceptable, so this is reported rather than asserted - the message landing is what matters and
# is checked below.
if first.offset == INVALID_OFFSET
  puts "  note: first produce reported the invalid offset (#{INVALID_OFFSET}) - the documented edge case"
else
  puts "  note: first produce reported a real offset (#{first.offset}) - broker returned it in time"
end

failed = check(
  failed,
  first.offset == INVALID_OFFSET || first.offset >= 0,
  "first offset is either a real offset or the invalid sentinel, never arbitrary"
)

# Once the topic is established every subsequent produce must report a real, increasing offset.
failed = check(
  failed,
  rest.all? { |report| report.offset >= 0 },
  "every produce after the first reports a valid offset (#{rest.map(&:offset).join(", ")})"
)
failed = check(
  failed,
  rest.each_cons(2).all? { |a, b| b.offset > a.offset },
  "offsets after the first are strictly increasing"
)

# The point of the whole edge case: an invalid offset does not mean a lost message.
payloads = consume_payloads(topic, MESSAGES)
expected_payloads = Array.new(MESSAGES) { |i| "payload-#{i}" }

failed = check(
  failed,
  expected_payloads.all? { |payload| payloads.include?(payload) },
  "all #{MESSAGES} messages landed in the topic, including the first"
)

if failed
  puts "\nFAIL: produce_sync first-produce offset handling regressed"
else
  puts "\nPASS: produce_sync lands the first message and reports real increasing offsets afterwards"
end

exit(failed ? 1 : 0)
