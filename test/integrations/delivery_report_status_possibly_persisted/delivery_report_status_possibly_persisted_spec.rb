# frozen_string_literal: true

# Integration test showing the delivery report status of a message that timed out while in flight.
#
# When the message reached a broker but no acknowledgement came back before `message.timeout.ms`,
# librdkafka cannot know if the broker wrote it. The status is then `POSSIBLY_PERSISTED`: a retry
# may write a duplicate, so it is not the same decision as for `NOT_PERSISTED`.
#
# A real broker answers too fast to time out a request in flight, so this spec uses the librdkafka
# mock cluster with a simulated round-trip time (RTT). The producer first connects with the default
# timeouts. A variant then produces to a new topic with a `message.timeout.ms` between one RTT
# (metadata of the new topic, then the produce request is sent) and two RTTs (the acknowledgement),
# so the message times out while in flight. librdkafka scans for timeouts once per second, so the
# RTT is large enough to keep a second of margin on both sides.

require "waterdrop"

RTT_MS = 4_000

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

# The retry decision the delivery report status makes possible.
def retry_decision(report)
  if report.not_persisted?
    :retry
  elsif report.possibly_persisted?
    :retry_may_duplicate
  else
    :done
  end
end

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.max_wait_timeout = 60_000
  config.kafka = {
    "bootstrap.servers": "127.0.0.1:9999",
    "test.mock.num.brokers": 1,
    "test.mock.broker.rtt": RTT_MS
  }
end

failures = Queue.new

producer.monitor.subscribe("error.occurred") do |event|
  failures << event if event[:type] == "librdkafka.dispatch_error"
end

# Establishes the connection, so the slow produce below needs only the metadata and produce RTTs
warmup_report = producer.produce_sync(topic: "warmup", payload: "warmup")

# Topic-level config is fixed when librdkafka first sees a topic, so the variant needs a new topic
slow = producer.variant(topic_config: { "message.timeout.ms": RTT_MS + 1_500 })
slow.produce_async(topic: "in-flight", payload: "maybe")

event = failures.pop
report = event[:delivery_report]

producer.close

failed = check(failed, warmup_report.persisted?, "warm-up message is persisted")
failed = check(
  failed,
  event[:error].code == :msg_timed_out,
  "delivery failed with msg_timed_out (#{event[:error].code})"
)
failed = check(failed, report.possibly_persisted?, "message is possibly persisted")
failed = check(
  failed,
  report.status == Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED,
  "status is RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED"
)
failed = check(
  failed,
  !report.persisted? && !report.not_persisted?,
  "no other status predicate matches"
)
failed = check(
  failed,
  retry_decision(report) == :retry_may_duplicate,
  "a retry may write a duplicate"
)

if failed
  puts "\nFAIL: delivery report status regressed for messages that timed out in flight"
else
  puts "\nPASS: a message that timed out in flight is reported as possibly persisted"
end

exit(failed ? 1 : 0)
