# frozen_string_literal: true

# Integration test showing the delivery report status of a message that timed out while in flight.
#
# When the message reached a broker but no acknowledgement came back before `message.timeout.ms`,
# librdkafka cannot know if the broker wrote it. The status is then `POSSIBLY_PERSISTED`: a retry
# may write a duplicate, so it is not the same decision as for `NOT_PERSISTED`.
#
# A real broker answers too fast to time out a request in flight, so this spec uses the librdkafka
# mock cluster. The first message goes through with no delay, so the connection and the topic
# metadata are ready. Then the mock broker round-trip time (RTT) is raised far above
# `message.timeout.ms`: the next produce request is sent at once, and its acknowledgement cannot
# arrive before the message times out, however slow the machine is.

require "waterdrop"

# The mock cluster API is not part of the Ruby bindings, so we attach the two functions we need
# from the bundled librdkafka.
module MockCluster
  extend FFI::Library

  ffi_lib Rdkafka::Bindings.ffi_libraries.map(&:name)

  attach_function :rd_kafka_handle_mock_cluster, [:pointer], :pointer
  attach_function :rd_kafka_mock_broker_set_rtt, [:pointer, :int32, :int], :int
end

MESSAGE_TIMEOUT_MS = 5_000
SLOW_RTT_MS = MESSAGE_TIMEOUT_MS * 10

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
  config.kafka = {
    "bootstrap.servers": "127.0.0.1:9999",
    "test.mock.num.brokers": 1,
    "message.timeout.ms": MESSAGE_TIMEOUT_MS
  }
end

failures = Queue.new

producer.monitor.subscribe("error.occurred") do |event|
  failures << event if event[:type] == "librdkafka.dispatch_error"
end

topic = "in-flight"

warmup_report = producer.produce_sync(topic: topic, payload: "warmup")

producer.client.instance_variable_get(:@native_kafka).with_inner do |inner|
  mock_cluster = MockCluster.rd_kafka_handle_mock_cluster(inner)
  MockCluster.rd_kafka_mock_broker_set_rtt(mock_cluster, warmup_report.broker_id, SLOW_RTT_MS)
end

producer.produce_async(topic: topic, payload: "maybe")

event = failures.pop
report = event[:delivery_report]

producer.close

unless warmup_report.persisted?
  warn "FAIL: warm-up message is not persisted (status #{warmup_report.status})"
  exit(1)
end

unless event[:error].code == :msg_timed_out
  warn "FAIL: expected msg_timed_out, got #{event[:error].code}"
  exit(1)
end

unless report.possibly_persisted?
  warn "FAIL: message is not reported as possibly persisted (status #{report.status})"
  exit(1)
end

unless report.status == Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED
  warn "FAIL: expected RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED, got status #{report.status}"
  exit(1)
end

if report.persisted? || report.not_persisted?
  warn "FAIL: another status predicate also matches"
  exit(1)
end

unless retry_decision(report) == :retry_may_duplicate
  warn "FAIL: expected :retry_may_duplicate, got #{retry_decision(report)}"
  exit(1)
end

puts "PASS: a message that timed out in flight is reported as possibly persisted"
