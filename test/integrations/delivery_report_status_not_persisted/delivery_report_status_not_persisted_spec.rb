# frozen_string_literal: true

# Integration test showing how to use the delivery report status to decide whether a failed
# message can be retried without a risk of duplicates.
#
# A failed delivery is reported through the `error.occurred` event (type
# `librdkafka.dispatch_error`), whose payload carries the `delivery_report`. When librdkafka never
# sent the message to a broker, the status is `NOT_PERSISTED`: a retry cannot write a duplicate.
# An unreachable broker with a short `message.timeout.ms` makes every message time out locally.

require "waterdrop"

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
  config.max_wait_timeout = 10_000
  config.kafka = {
    # Nothing listens here, so the message never leaves the local queue
    "bootstrap.servers": "127.0.0.1:9999",
    "message.timeout.ms": 1_000
  }
end

failures = Queue.new

producer.monitor.subscribe("error.occurred") do |event|
  failures << event if event[:type] == "librdkafka.dispatch_error"
end

producer.produce_async(topic: generate_topic("dr-not-persisted"), payload: "lost")

event = failures.pop
report = event[:delivery_report]

producer.close

failed = check(
  failed,
  event[:error].code == :msg_timed_out,
  "delivery failed with msg_timed_out (#{event[:error].code})"
)
failed = check(failed, report.not_persisted?, "message is not persisted")
failed = check(
  failed,
  report.status == Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_NOT_PERSISTED,
  "status is RD_KAFKA_MSG_STATUS_NOT_PERSISTED"
)
failed = check(
  failed,
  !report.persisted? && !report.possibly_persisted?,
  "no other status predicate matches"
)
failed = check(failed, report.broker_id.nil?, "no broker took the write, so broker id is nil")
failed = check(
  failed,
  report.latency.is_a?(Integer) && report.latency >= 1_000_000,
  "latency covers the whole message.timeout.ms (#{report.latency} us)"
)
failed = check(failed, retry_decision(report) == :retry, "the message is safe to retry")

if failed
  puts "\nFAIL: delivery report status regressed for messages that never reached a broker"
else
  puts "\nPASS: a message that never reached a broker is reported as not persisted"
end

exit(failed ? 1 : 0)
