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
