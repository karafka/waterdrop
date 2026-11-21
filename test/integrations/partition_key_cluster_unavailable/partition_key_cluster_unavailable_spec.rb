# frozen_string_literal: true

# Integration test for `produce_async` with `partition_key` while the cluster is unreachable.
#
# Routing by `partition_key` needs the topic partition count. It comes from the process-wide
# karafka-rdkafka partition count cache, or from a blocking metadata request when the cache has
# no entry. With the broker down:
#
# - a topic with a fresh cached count is buffered at once,
# - a topic with no cached count fails with `ProduceError` and an `error.occurred` event,
# - a message with only `key` needs no partition count, so it is buffered and delivered once the
#   broker is back,
# - a topic whose cached count expired is buffered with the last known count.
#
# The broker is stopped and started with the librdkafka mock cluster, so no Docker restart is
# needed. No `statistics.emitted` listener is subscribed, so statistics never refresh the cache.

require "waterdrop"

# The mock cluster API is not part of the Ruby bindings, so we attach the functions we need from
# the bundled librdkafka.
module MockCluster
  extend FFI::Library

  ffi_lib Rdkafka::Bindings.ffi_libraries.map(&:name)

  attach_function :rd_kafka_mock_cluster_new, [:pointer, :int], :pointer
  attach_function :rd_kafka_mock_cluster_bootstraps, [:pointer], :string
  attach_function :rd_kafka_mock_cluster_destroy, [:pointer], :void
  attach_function :rd_kafka_mock_broker_set_down, [:pointer, :int32], :int
  attach_function :rd_kafka_mock_broker_set_up, [:pointer, :int32], :int
end

TTL_MS = 2_000
BROKER_ID = 1

# What each produce does while the broker is down: `:buffered` or `:raises`
EXPECTED = {
  warm: :buffered,
  cold: :raises,
  key_only: :buffered,
  expired: :buffered
}.freeze

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

# @return [Array(Symbol, Exception, nil)] outcome of the produce and the error it raised
def attempt
  yield
  [:buffered, nil]
rescue WaterDrop::Errors::ProduceError => e
  [:raises, e]
end

# The mock cluster lives in its own librdkafka handle, which has to outlive the producer
errstr = FFI::MemoryPointer.new(:char, 256)
mock_handle = Rdkafka::Bindings.rd_kafka_new(
  :rd_kafka_producer,
  Rdkafka::Bindings.rd_kafka_conf_new,
  errstr,
  errstr.size
)
mock_cluster = MockCluster.rd_kafka_mock_cluster_new(mock_handle, 1)

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.partition_count_cache_ttl_ms = TTL_MS
  config.kafka = {
    "bootstrap.servers": MockCluster.rd_kafka_mock_cluster_bootstraps(mock_cluster),
    "message.timeout.ms": 60_000
  }
end

errors = Queue.new

producer.monitor.subscribe("error.occurred") do |event|
  errors << event if event[:type] == "message.produce_async"
end

warm_topic = "warm"
cold_topic = "cold"

producer.produce_sync(topic: warm_topic, payload: "warmup", partition_key: "k")
warmed_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

MockCluster.rd_kafka_mock_broker_set_down(mock_cluster, BROKER_ID)

results = {}

results[:warm] = attempt do
  producer.produce_async(topic: warm_topic, payload: "warm", partition_key: "k")
end

warm_elapsed_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - warmed_at) * 1_000

results[:cold] = attempt do
  producer.produce_async(topic: cold_topic, payload: "cold", partition_key: "k")
end

key_only_handle = nil

results[:key_only] = attempt do
  key_only_handle = producer.produce_async(topic: cold_topic, payload: "key", key: "k")
end

sleep((TTL_MS / 1_000.0) + 0.5)

results[:expired] = attempt do
  producer.produce_async(topic: warm_topic, payload: "expired", partition_key: "k")
end

MockCluster.rd_kafka_mock_broker_set_up(mock_cluster, BROKER_ID)

key_only_report = key_only_handle&.wait(max_wait_timeout_ms: 30_000)

error_events = []
error_events << errors.pop until errors.empty?

producer.close
MockCluster.rd_kafka_mock_cluster_destroy(mock_cluster)
Rdkafka::Bindings.rd_kafka_destroy(mock_handle)

failed = check(failed, warm_elapsed_ms < TTL_MS, "warm topic produce ran within the TTL")

EXPECTED.each do |name, expected|
  outcome, error = results.fetch(name)

  failed = check(
    failed,
    outcome == expected,
    "#{name}: #{expected} (got #{outcome}#{": #{error.cause.inspect}" if error})"
  )
end

raised = results.values.filter_map { |_, error| error }

failed = check(
  failed,
  error_events.size == raised.size,
  "one message.produce_async error.occurred per raised produce (#{error_events.size})"
)

failed = check(
  failed,
  error_events.all? do |event|
    event[:error].is_a?(WaterDrop::Errors::ProduceError) &&
      event[:error].cause.is_a?(Rdkafka::RdkafkaError)
  end,
  "error.occurred carries ProduceError caused by Rdkafka::RdkafkaError"
)

failed = check(
  failed,
  key_only_report&.error.nil? && key_only_report&.topic_name == cold_topic,
  "key-only message is delivered once the broker is up"
)

if failed
  puts "\nFAIL: partition_key produce while the cluster is unavailable changed"
else
  puts "\nPASS: partition_key produce while the cluster is unavailable behaves as expected"
end

exit(failed ? 1 : 0)
