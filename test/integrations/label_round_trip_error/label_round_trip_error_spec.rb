# frozen_string_literal: true

# Integration test locking in the "round-trip time tracking via labels" recipe documented in the
# Karafka wiki (https://github.com/karafka/wiki/pull/1078/changes) - failed delivery case.
#
# The recipe stashes a monotonic timestamp in the message `label` when producing and diffs it
# against the current monotonic clock to obtain the round-trip duration. When delivery FAILS, the
# recipe still needs the label so the failed round trip can be measured and reported. For this to
# work, WaterDrop MUST carry that exact label, untouched, into the `error.occurred` event payload
# raised from the delivery callback.
#
# We force a delivery failure with an impossibly short `message.timeout.ms` and assert the dispatch
# error surfaced via `error.occurred` carries our label.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Monotonic clock, matching what the wiki recipe recommends (unaffected by NTP/clock changes)
def monotonic_now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

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

# We could not provoke a delivery failure at all - the label plumbing was never exercised, so we
# must not report a false pass.
unless error_event
  warn "FAIL: no dispatch error was triggered; could not verify the error-path label"
  exit(1)
end

round_trip = monotonic_now - error_event[:label]

unless error_event[:label].is_a?(Float)
  warn "FAIL: error.occurred event carries the monotonic label"
  exit(1)
end

unless round_trip.positive?
  warn "FAIL: failed round-trip time is positive (#{round_trip.round(5)}s)"
  exit(1)
end

if error_event[:error].nil?
  warn "FAIL: error.occurred event carries the delivery error"
  exit(1)
end

puts "PASS: labels survive into error.occurred on failed delivery"
