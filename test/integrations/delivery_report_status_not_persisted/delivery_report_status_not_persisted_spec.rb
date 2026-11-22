# frozen_string_literal: true

# Integration test showing how to use the delivery report status to decide whether a failed
# message can be retried without a risk of duplicates.
#
# A failed delivery is reported through the `error.occurred` event (type
# `librdkafka.dispatch_error`), whose payload carries the `delivery_report`. When librdkafka never
# sent the message to a broker, the status is `NOT_PERSISTED`: a retry cannot write a duplicate.
# An unreachable broker with a short `message.timeout.ms` makes every message time out locally.

require "waterdrop"

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

unless event[:error].code == :msg_timed_out
  warn "FAIL: delivery failed with msg_timed_out (#{event[:error].code})"
  exit(1)
end

unless report.not_persisted?
  warn "FAIL: message is not persisted"
  exit(1)
end

unless report.status == Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_NOT_PERSISTED
  warn "FAIL: status is RD_KAFKA_MSG_STATUS_NOT_PERSISTED"
  exit(1)
end

unless !report.persisted? && !report.possibly_persisted?
  warn "FAIL: no other status predicate matches"
  exit(1)
end

unless report.broker_id.nil?
  warn "FAIL: no broker took the write, so broker id is nil"
  exit(1)
end

unless report.latency.is_a?(Integer) && report.latency >= 1_000_000
  warn "FAIL: latency covers the whole message.timeout.ms (#{report.latency} us)"
  exit(1)
end

unless retry_decision(report) == :retry
  warn "FAIL: the message is safe to retry"
  exit(1)
end

puts "PASS: a message that never reached a broker is reported as not persisted"
