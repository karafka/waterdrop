# frozen_string_literal: true

# Integration test locking in the offset a variant with `acks: 0` reports from `produce_sync`.
#
# With `acks: 0` the produce is fire-and-forget: the broker never acknowledges the write, so
# librdkafka has no offset to put in the delivery report and reports the invalid offset (-1001)
# for every message. This is unconditional, unlike the first-produce case covered by
# produce_sync_new_topic_offset_spec, where only the very first message to a new topic is invalid.

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

# Non-idempotent on purpose: an idempotent producer rejects an `acks: 0` variant outright, which
# idempotent_variant_acks_zero_spec already covers.
producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

variant = producer.variant(topic_config: { acks: 0 })
topic = generate_topic("variant-acks-zero")

reports = Array.new(MESSAGES) do |i|
  variant.produce_sync(topic: topic, payload: "payload-#{i}")
end

producer.close

# Control, so the assertions below cannot pass just because everything reports an invalid offset.
# It needs its own producer and topic: `acks: 0` is topic-level config, so it would otherwise stick
# to the topic already used above.
control_producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

control_topic = generate_topic("variant-acks-zero-control")

control_reports = Array.new(MESSAGES) do |i|
  control_producer.produce_sync(topic: control_topic, payload: "payload-#{i}")
end

control_producer.close

failed = check(failed, reports.size == MESSAGES, "a report per message")
failed = check(failed, reports.none?(&:error), "no report carries an error")
failed = check(
  failed, reports.all? { |report| report.topic_name == topic }, "reports name the produced topic"
)
failed = check(
  failed,
  reports.all? { |report| report.offset == INVALID_OFFSET },
  "every acks: 0 report carries the invalid offset"
)
failed = check(
  failed,
  control_reports[1..].all? { |report| report.offset >= 0 },
  "default acks reports real offsets"
)

if failed
  puts "\nFAIL: acks: 0 offset reporting regressed"
else
  puts "\nPASS: a variant with acks: 0 always reports the invalid offset from produce_sync"
end

exit(failed ? 1 : 0)
