# frozen_string_literal: true

# Integration test locking in that a buffered message keeps the topic config of the variant it was
# buffered through, whoever flushes the buffer.
#
# A message buffered through an `acks: 0` variant is sent without acks, so its delivery report has
# no offset (negative). A message buffered through the producer itself is sent with the default
# acks and gets a real offset, also when an `acks: 0` variant flushes it. Each variant uses its own
# topic because librdkafka keeps one topic config per topic per client.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
MESSAGES = 3

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

# Buffers into both topics, flushes once through the given flusher and returns the offsets per topic
def round(producer, variant, flusher, sync:)
  variant_topic = generate_topic("variant-buffer-acks-zero")
  default_topic = generate_topic("variant-buffer-default")

  MESSAGES.times do |i|
    variant.buffer(topic: variant_topic, payload: "variant-#{i}")
    producer.buffer(topic: default_topic, payload: "default-#{i}")
  end

  handles = sync ? flusher.flush_sync : flusher.flush_async.each(&:wait)
  results = handles.map(&:create_result)

  [
    results.select { |result| result.topic_name == variant_topic }.map(&:offset),
    results.select { |result| result.topic_name == default_topic }.map(&:offset)
  ]
end

# Non-idempotent on purpose: an idempotent producer rejects an `acks: 0` variant outright.
producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

variant = producer.with(topic_config: { acks: 0 })

[
  ["producer flush_sync", producer, true],
  ["producer flush_async", producer, false],
  ["variant flush_sync", variant, true],
  ["variant flush_async", variant, false]
].each do |name, flusher, sync|
  variant_offsets, default_offsets = round(producer, variant, flusher, sync: sync)

  failed = check(
    failed,
    variant_offsets.size == MESSAGES && variant_offsets.all?(&:negative?),
    "#{name}: variant messages are sent with acks: 0"
  )
  # The first report on a new topic may carry no offset even with acks
  failed = check(
    failed,
    default_offsets.size == MESSAGES && default_offsets.drop(1).all? { |offset| offset >= 0 },
    "#{name}: default messages are sent with the default acks"
  )
end

producer.close

if failed
  puts "\nFAIL: a flush did not send buffered messages with their own variant topic config"
else
  puts "\nPASS: buffered messages keep the topic config of the variant they were buffered through"
end

exit(failed ? 1 : 0)
