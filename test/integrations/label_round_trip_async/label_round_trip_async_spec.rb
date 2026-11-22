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
unless handle.label == produced_at
  warn "FAIL: delivery handle label is #{handle.label.inspect}, not the produced one"
  exit(1)
end

event = acknowledged.pop
round_trip = monotonic_now - event[:label]

unless event[:label].equal?(produced_at)
  warn "FAIL: acknowledged event label #{event[:label].inspect} is not the produced object"
  exit(1)
end

unless event[:label].is_a?(Float)
  warn "FAIL: expected a Float label, got #{event[:label].class}"
  exit(1)
end

unless round_trip.positive?
  warn "FAIL: round-trip time is not positive (#{round_trip.round(5)}s)"
  exit(1)
end

# Being an async dispatch, the round trip must include far more than the near-zero time the
# produce_async call itself took to return - that is the whole point of the recipe.
unless round_trip >= (produce_call_returned_at - produced_at)
  warn "FAIL: round-trip time #{round_trip.round(5)}s is shorter than the produce call"
  exit(1)
end

if event[:topic].nil?
  warn "FAIL: acknowledged event has no topic"
  exit(1)
end

producer.close

puts "PASS: async labels carry a monotonic timestamp into message.acknowledged"
