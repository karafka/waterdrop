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

unless reports.size == MESSAGES
  warn "FAIL: expected #{MESSAGES} reports, got #{reports.size}"
  exit(1)
end

unless reports.none?(&:error)
  warn "FAIL: a report carries #{reports.find(&:error).error.inspect}"
  exit(1)
end

unless reports.all? { |report| report.topic_name == topic }
  warn "FAIL: a report names another topic than #{topic}"
  exit(1)
end

unless reports.all? { |report| report.offset == INVALID_OFFSET }
  warn "FAIL: acks: 0 offsets #{reports.map(&:offset)} are not all #{INVALID_OFFSET}"
  exit(1)
end

unless control_reports[1..].all? { |report| report.offset >= 0 }
  warn "FAIL: default acks offsets #{control_reports.map(&:offset)} include an invalid one"
  exit(1)
end

puts "PASS: a variant with acks: 0 always reports the invalid offset from produce_sync"
