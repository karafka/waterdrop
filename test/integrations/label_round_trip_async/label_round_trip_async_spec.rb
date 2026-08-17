# frozen_string_literal: true

# Integration test locking in the "round-trip time tracking via labels" recipe documented in the
# Karafka wiki (https://github.com/karafka/wiki/pull/1078/changes) - async case.
#
# WaterDrop's built-in instrumentation only measures how long the `produce` call itself takes to
# return. For an async dispatch that is just the enqueueing time, NOT the time until the broker
# acknowledges the message. The documented technique to measure that full round trip is to stash a
# monotonic timestamp in the message `label` when producing and diff it against the current
# monotonic clock inside the `message.acknowledged` subscriber.
#
# For this to work, WaterDrop MUST accept an arbitrary object (here a monotonic Float) as a message
# `label` and carry that exact label, untouched, into the `message.acknowledged` event payload.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Monotonic clock, matching what the wiki recipe recommends (unaffected by NTP/clock changes)
def monotonic_now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

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

topic = generate_topic("label-rt-async")

acknowledged = Queue.new

producer.monitor.subscribe("message.acknowledged") do |event|
  acknowledged << event
end

produced_at = monotonic_now
handle = producer.produce_async(topic: topic, payload: "async-payload", label: produced_at)
produce_call_returned_at = monotonic_now

# librdkafka echoes the label back onto the delivery handle immediately
failed = check(failed, handle.label == produced_at, "delivery handle preserves the label")

event = acknowledged.pop
round_trip = monotonic_now - event[:label]

failed = check(
  failed, event[:label].equal?(produced_at), "acknowledged event carries the exact label object"
)
failed = check(failed, event[:label].is_a?(Float), "label is the monotonic Float we stashed")
failed = check(
  failed, round_trip.positive?, "round-trip time is a positive duration (#{round_trip.round(5)}s)"
)
# Being an async dispatch, the round trip must include far more than the near-zero time the
# produce_async call itself took to return - that is the whole point of the recipe.
failed = check(
  failed,
  round_trip >= (produce_call_returned_at - produced_at),
  "round-trip time captures more than the produce call return time"
)
failed = check(failed, !event[:topic].nil?, "acknowledged event still carries topic")

producer.close

if failed
  puts "\nFAIL: async label-based round-trip tracking regressed"
else
  puts "\nPASS: async labels carry a monotonic timestamp into message.acknowledged"
end

exit(failed ? 1 : 0)
