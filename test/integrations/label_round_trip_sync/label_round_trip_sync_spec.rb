# frozen_string_literal: true

# Integration test locking in the "round-trip time tracking via labels" recipe documented in the
# Karafka wiki (https://github.com/karafka/wiki/pull/1078/changes) - sync case.
#
# The recipe stashes a monotonic timestamp in the message `label` when producing and diffs it
# against the current monotonic clock inside the `message.acknowledged` subscriber to obtain the
# real round-trip duration. For this to work, WaterDrop MUST accept an arbitrary object (here a
# monotonic Float) as a message `label` and carry that exact label, untouched, into the
# `message.acknowledged` event payload - including for `produce_sync`.

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

topic = generate_topic("label-rt-sync")

acknowledged = Queue.new

producer.monitor.subscribe("message.acknowledged") do |event|
  acknowledged << event
end

produced_at = monotonic_now
producer.produce_sync(topic: topic, payload: "sync-payload", label: produced_at)

event = acknowledged.pop
round_trip = monotonic_now - event[:label]

unless event[:label].equal?(produced_at)
  warn "FAIL: acknowledged event carries the exact label object"
  exit(1)
end

unless event[:label].is_a?(Float)
  warn "FAIL: label is the monotonic Float we stashed"
  exit(1)
end

unless round_trip.positive?
  warn "FAIL: round-trip time is a positive duration (#{round_trip.round(5)}s)"
  exit(1)
end

producer.close

puts "PASS: sync labels carry a monotonic timestamp into message.acknowledged"
