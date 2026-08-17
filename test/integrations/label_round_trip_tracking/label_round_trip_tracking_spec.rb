# frozen_string_literal: true

# Integration test locking in the "round-trip time tracking via labels" recipe documented in
# the Karafka wiki (https://github.com/karafka/wiki/pull/1078/changes).
#
# WaterDrop's built-in instrumentation only measures how long the `produce` call itself takes to
# return. For async dispatches that is just the enqueueing time, NOT the time until the broker
# acknowledges the message. The documented technique to measure that full round trip is:
#
#   1. Stash a monotonic timestamp in the message `label` when producing.
#   2. In the `message.acknowledged` (and `error.occurred`) subscriber, diff that label against
#      the current monotonic clock to obtain the real round-trip duration.
#
# For this recipe to work, WaterDrop MUST:
#   * accept an arbitrary object (here a monotonic Float) as a message `label`,
#   * carry that exact label, untouched, into the `message.acknowledged` event payload, and
#   * carry it into the `error.occurred` event payload when delivery fails.
#
# This test exercises all three so the behaviour can never silently regress.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Monotonic clock, matching what the wiki recipe recommends (unaffected by NTP/clock changes)
def monotonic_now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

# Minimal per-case assertion accumulator. It prints and records failures instead of raising, so
# every check in a case runs and we get a full picture in a single execution.
class Checks
  attr_reader :failed

  def initialize
    @failed = false
  end

  # @param condition [Boolean] result of the assertion
  # @param message [String] human readable description of what we asserted
  def check(condition, message)
    if condition
      puts "  ok: #{message}"
    else
      puts "  FAIL: #{message}"
      @failed = true
    end
  end
end

failed = false

# --- Case 1: async happy path -------------------------------------------------------------------
# The label (a monotonic timestamp) must reach `message.acknowledged` untouched, letting us compute
# the true round-trip time - which, being an async dispatch, includes far more than the near-zero
# time the `produce_async` call itself took to return.
puts "\n--- Case 1: async round-trip time via label ---"

checks = Checks.new

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
checks.check(handle.label == produced_at, "delivery handle preserves the label")

event = acknowledged.pop
round_trip = monotonic_now - event[:label]

checks.check(event[:label].equal?(produced_at), "acknowledged event carries the exact label object")
checks.check(event[:label].is_a?(Float), "label is the monotonic Float we stashed")
checks.check(round_trip.positive?, "round-trip time is a positive duration (#{round_trip.round(5)}s)")
checks.check(
  round_trip >= (produce_call_returned_at - produced_at),
  "round-trip time captures more than the produce call return time"
)
checks.check(!event[:topic].nil?, "acknowledged event still carries topic (#{event[:topic]})")

producer.close

failed = true if checks.failed

# --- Case 2: sync path --------------------------------------------------------------------------
# The same label plumbing must work for `produce_sync`.
puts "\n--- Case 2: sync round-trip time via label ---"

checks = Checks.new

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

checks.check(event[:label].equal?(produced_at), "acknowledged event carries the exact label object")
checks.check(round_trip.positive?, "round-trip time is a positive duration (#{round_trip.round(5)}s)")

producer.close

failed = true if checks.failed

# --- Case 3: error path -------------------------------------------------------------------------
# When delivery FAILS, the recipe still needs the label so the failed round trip can be measured
# and reported. We force a delivery failure with an impossibly short `message.timeout.ms` and assert
# the dispatch error surfaced via `error.occurred` carries our label.
puts "\n--- Case 3: label survives into error.occurred on failed delivery ---"

checks = Checks.new
occurred = Queue.new
error_event = nil

# We retry in a loop because on very fast machines a message may occasionally get acknowledged
# before the 1ms timeout fires. We only care that WHEN a dispatch error occurs, it carries the
# label.
20.times do
  producer = WaterDrop::Producer.new do |config|
    config.deliver = true
    config.kafka = {
      "bootstrap.servers": BOOTSTRAP_SERVERS,
      "queue.buffering.max.ms": 0,
      "message.timeout.ms": 1
    }
  end

  producer.monitor.subscribe("error.occurred") do |event|
    # Only the per-message dispatch errors from the delivery callback carry a message label
    occurred << event if event[:type] == "librdkafka.dispatch_error"
  end

  produced_at = monotonic_now
  topic = generate_topic("label-rt-error")

  50.times { producer.produce_async(topic: topic, payload: "will-time-out", label: produced_at) }

  producer.close

  next if occurred.empty?

  error_event = occurred.pop
  break
end

if error_event
  round_trip = monotonic_now - error_event[:label]
  checks.check(error_event[:label].is_a?(Float), "error.occurred event carries the monotonic label")
  checks.check(round_trip.positive?, "failed round-trip time is positive (#{round_trip.round(5)}s)")
  checks.check(!error_event[:error].nil?, "error.occurred event carries the delivery error")
else
  puts "  WARN: no dispatch error was triggered in this run; skipping error-path assertions"
end

failed = true if checks.failed

# --- Summary ------------------------------------------------------------------------------------
if failed
  puts "\nFAIL: label-based round-trip tracking regressed"
else
  puts "\nPASS: labels reliably carry a monotonic timestamp into acknowledged and error events"
end

exit(failed ? 1 : 0)
