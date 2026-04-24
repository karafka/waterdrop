# frozen_string_literal: true

# Benchmark comparing StatisticsDecorator performance:
#   1. Full decoration (no options)
#   2. only_keys alone
#   3. only_keys + excluded_keys combined
#
# Simulates a realistic producer environment:
#   - 10 brokers
#   - 20 topics
#   - 100 partitions per topic (2000 total)
#
# Usage: ruby benchmarks/statistics_decorator_only_keys.rb

# Use local karafka-core with only_keys support
$LOAD_PATH.unshift(File.expand_path("../../karafka-core/lib", __dir__))
require "karafka-core"

def realtime
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
end

# Build a realistic librdkafka producer statistics hash
def build_producer_statistics(num_brokers: 10, num_topics: 20, partitions_per_topic: 100)
  stats = {
    "name" => "waterdrop-abc123",
    "client_id" => "waterdrop",
    "type" => "producer",
    "ts" => 1_700_000_000_000,
    "time" => 1_700_000_000,
    "age" => 120_000_000,
    "replyq" => 0,
    "msg_cnt" => 42,
    "msg_size" => 8400,
    "msg_max" => 1_000_000,
    "msg_size_max" => 1_000_000_000,
    "tx" => 15_230,
    "tx_bytes" => 4_500_000,
    "rx" => 14_800,
    "rx_bytes" => 3_200_000,
    "txmsgs" => 10_000,
    "txmsg_bytes" => 2_500_000,
    "rxmsgs" => 200,
    "rxmsg_bytes" => 50_000,
    "simple_cnt" => 0,
    "metadata_cache_cnt" => 20
  }

  # Brokers
  brokers = {}
  num_brokers.times do |i|
    broker_name = "localhost:#{9092 + i}/#{i}"
    brokers[broker_name] = {
      "name" => broker_name,
      "nodeid" => i,
      "nodename" => "localhost:#{9092 + i}",
      "source" => "learned",
      "state" => "UP",
      "stateage" => 60_000_000 + i * 1000,
      "outbuf_cnt" => rand(10),
      "outbuf_msg_cnt" => rand(100),
      "waitresp_cnt" => rand(5),
      "waitresp_msg_cnt" => rand(50),
      "tx" => 1500 + rand(500),
      "tx_bytes" => 450_000 + rand(100_000),
      "txerrs" => rand(10),
      "txretries" => rand(20),
      "txidle" => rand(5000),
      "req_timeouts" => rand(3),
      "rx" => 1400 + rand(500),
      "rx_bytes" => 320_000 + rand(80_000),
      "rxerrs" => rand(5),
      "rxcorriderrs" => 0,
      "rxpartial" => 0,
      "rxidle" => rand(5000),
      "zbuf_grow" => 0,
      "buf_grow" => 0,
      "wakeups" => 500 + rand(200),
      "connects" => 3 + rand(5),
      "disconnects" => rand(3),
      "int_latency" => {
        "min" => 5, "max" => 500, "avg" => 50, "sum" => 25_000,
        "stddev" => 30, "p50" => 40, "p75" => 60, "p90" => 80,
        "p95" => 100, "p99" => 200, "p99_99" => 450, "cnt" => 500,
        "hdrsize" => 11376
      },
      "outbuf_latency" => {
        "min" => 1, "max" => 100, "avg" => 10, "sum" => 5000,
        "stddev" => 8, "p50" => 8, "p75" => 12, "p90" => 18,
        "p95" => 25, "p99" => 50, "p99_99" => 95, "cnt" => 500,
        "hdrsize" => 11376
      },
      "rtt" => {
        "min" => 100, "max" => 5000, "avg" => 500, "sum" => 250_000,
        "stddev" => 200, "p50" => 400, "p75" => 600, "p90" => 800,
        "p95" => 1000, "p99" => 2000, "p99_99" => 4500, "cnt" => 500,
        "hdrsize" => 11376
      },
      "throttle" => {
        "min" => 0, "max" => 0, "avg" => 0, "sum" => 0,
        "stddev" => 0, "p50" => 0, "p75" => 0, "p90" => 0,
        "p95" => 0, "p99" => 0, "p99_99" => 0, "cnt" => 0,
        "hdrsize" => 11376
      },
      "req" => {
        "Produce" => 1000, "Fetch" => 0, "ListOffsets" => 10,
        "Metadata" => 5, "OffsetCommit" => 0, "OffsetFetch" => 0,
        "FindCoordinator" => 2, "JoinGroup" => 0, "Heartbeat" => 0,
        "LeaveGroup" => 0, "SyncGroup" => 0, "DescribeGroups" => 0,
        "ListGroups" => 0, "SaslHandshake" => 0, "ApiVersion" => 1,
        "CreateTopics" => 0, "DeleteTopics" => 0, "DeleteRecords" => 0,
        "InitProducerId" => 1, "OffsetForLeaderEpoch" => 0,
        "AddPartitionsToTxn" => 0, "AddOffsetsToTxn" => 0,
        "EndTxn" => 0, "TxnOffsetCommit" => 0, "SaslAuthenticate" => 0
      },
      "toppars" => {}
    }
  end
  stats["brokers"] = brokers

  # Topics (producer-side stats include per-partition delivery info)
  topics = {}
  num_topics.times do |t|
    topic_name = "topic_#{t}"
    partitions = {}
    partitions_per_topic.times do |p|
      partitions[p.to_s] = {
        "partition" => p,
        "broker" => p % num_brokers,
        "leader" => p % num_brokers,
        "desired" => false,
        "unknown" => false,
        "msgq_cnt" => rand(10),
        "msgq_bytes" => rand(10_000),
        "xmit_msgq_cnt" => rand(5),
        "xmit_msgq_bytes" => rand(5000),
        "fetchq_cnt" => 0,
        "fetchq_size" => 0,
        "fetch_state" => "none",
        "query_offset" => -1001,
        "next_offset" => 0,
        "app_offset" => -1001,
        "stored_offset" => -1001,
        "committed_offset" => -1001,
        "eof_offset" => -1001,
        "lo_offset" => -1001,
        "hi_offset" => -1001,
        "ls_offset" => -1001,
        "consumer_lag" => -1,
        "consumer_lag_stored" => -1,
        "txmsgs" => 100 + rand(500),
        "txbytes" => 25_000 + rand(100_000),
        "rxmsgs" => 0,
        "rxbytes" => 0,
        "msgs" => 100 + rand(500),
        "rx_ver_drops" => 0,
        "msgs_inflight" => rand(5),
        "next_ack_seq" => 0,
        "next_err_seq" => 0,
        "acked_msgid" => 0
      }
    end
    # Also add the aggregate -1 partition
    partitions["-1"] = {
      "partition" => -1,
      "broker" => -1,
      "leader" => -1,
      "desired" => false,
      "unknown" => false,
      "msgq_cnt" => 0,
      "msgq_bytes" => 0,
      "xmit_msgq_cnt" => 0,
      "xmit_msgq_bytes" => 0,
      "fetchq_cnt" => 0,
      "fetchq_size" => 0,
      "fetch_state" => "none",
      "query_offset" => -1001,
      "next_offset" => 0,
      "app_offset" => -1001,
      "stored_offset" => -1001,
      "committed_offset" => -1001,
      "eof_offset" => -1001,
      "lo_offset" => -1001,
      "hi_offset" => -1001,
      "ls_offset" => -1001,
      "consumer_lag" => -1,
      "consumer_lag_stored" => -1,
      "txmsgs" => 0,
      "txbytes" => 0,
      "rxmsgs" => 0,
      "rxbytes" => 0,
      "msgs" => 0,
      "rx_ver_drops" => 0,
      "msgs_inflight" => 0,
      "next_ack_seq" => 0,
      "next_err_seq" => 0,
      "acked_msgid" => 0
    }

    topics[topic_name] = {
      "topic" => topic_name,
      "age" => 120_000_000,
      "metadata_age" => 5000,
      "batchsize" => {
        "min" => 100, "max" => 50_000, "avg" => 10_000, "sum" => 5_000_000,
        "stddev" => 5000, "p50" => 8000, "p75" => 12_000, "p90" => 20_000,
        "p95" => 30_000, "p99" => 45_000, "p99_99" => 49_000, "cnt" => 500,
        "hdrsize" => 14512
      },
      "batchcnt" => {
        "min" => 1, "max" => 100, "avg" => 10, "sum" => 5000,
        "stddev" => 8, "p50" => 8, "p75" => 12, "p90" => 20,
        "p95" => 30, "p99" => 50, "p99_99" => 95, "cnt" => 500,
        "hdrsize" => 8304
      },
      "partitions" => partitions
    }
  end
  stats["topics"] = topics

  stats
end

def deep_copy(hash)
  Marshal.load(Marshal.dump(hash))
end

# --- Measure allocations ---
def count_allocations
  before = GC.stat(:total_allocated_objects)
  yield
  GC.stat(:total_allocated_objects) - before
end

def benchmark_decorator(decorator, template, iterations, warmup)
  warmup.times { decorator.call(deep_copy(template)) }

  times = []
  allocs = []

  iterations.times do
    stats = deep_copy(template)
    a = count_allocations do
      times << realtime { decorator.call(stats) }
    end
    allocs << a
  end

  [times, allocs]
end

def avg(arr) = arr.sum / arr.size
def median(arr) = arr.sort[arr.size / 2]
def p95(arr) = arr.sort[(arr.size * 0.95).to_i]

# --- Configuration ---
NUM_BROKERS = 10
NUM_TOPICS = 20
PARTITIONS_PER_TOPIC = 100
ITERATIONS = 50
WARMUP = 5

only_keys = %w[tx txretries txerrs rxerrs]

# excluded_keys: skip broker sub-hashes (int_latency, outbuf_latency, rtt, throttle, req,
# toppars) and topics entirely since WaterDrop only needs root + broker numeric keys
excluded_keys = %w[
  int_latency outbuf_latency rtt throttle req toppars
  topics
]

puts "=" * 78
puts "StatisticsDecorator Benchmark: WaterDrop decoration strategies"
puts "=" * 78
puts
puts "Cluster: #{NUM_BROKERS} brokers, #{NUM_TOPICS} topics, #{PARTITIONS_PER_TOPIC} partitions/topic"
puts "Total partitions: #{NUM_TOPICS * PARTITIONS_PER_TOPIC}"
puts "only_keys: #{only_keys.inspect}"
puts "excluded_keys: #{excluded_keys.inspect}"
puts

template = build_producer_statistics(
  num_brokers: NUM_BROKERS,
  num_topics: NUM_TOPICS,
  partitions_per_topic: PARTITIONS_PER_TOPIC
)

flat_keys = template.keys.size
broker_keys = template["brokers"].values.first&.keys&.size || 0
partition_keys = template.dig("topics", "topic_0", "partitions", "0")&.keys&.size || 0
puts "Stats hash: #{flat_keys} root keys, #{broker_keys} keys/broker, #{partition_keys} keys/partition"
puts

# 1. Full decoration (baseline)
full_t, full_a = benchmark_decorator(
  Karafka::Core::Monitoring::StatisticsDecorator.new,
  template, ITERATIONS, WARMUP
)

# 2. only_keys alone
only_t, only_a = benchmark_decorator(
  Karafka::Core::Monitoring::StatisticsDecorator.new(only_keys: only_keys),
  template, ITERATIONS, WARMUP
)

# 3. excluded_keys alone
excl_t, excl_a = benchmark_decorator(
  Karafka::Core::Monitoring::StatisticsDecorator.new(excluded_keys: excluded_keys),
  template, ITERATIONS, WARMUP
)

# 4. only_keys + excluded_keys combined
both_t, both_a = benchmark_decorator(
  Karafka::Core::Monitoring::StatisticsDecorator.new(
    only_keys: only_keys,
    excluded_keys: excluded_keys
  ),
  template, ITERATIONS, WARMUP
)

puts "-" * 78
puts "Results (#{ITERATIONS} iterations, #{WARMUP} warmup):"
puts "-" * 78
puts
header = format(
  "%-25s %12s %12s %12s %12s",
  "", "Full", "only_keys", "excluded", "both"
)
puts header
puts format("%-25s %12s %12s %12s %12s", "-" * 25, "-" * 12, "-" * 12, "-" * 12, "-" * 12)
puts format(
  "%-25s %9.2f ms %9.2f ms %9.2f ms %9.2f ms",
  "Avg time",
  avg(full_t) * 1000, avg(only_t) * 1000, avg(excl_t) * 1000, avg(both_t) * 1000
)
puts format(
  "%-25s %9.2f ms %9.2f ms %9.2f ms %9.2f ms",
  "Median time",
  median(full_t) * 1000, median(only_t) * 1000, median(excl_t) * 1000, median(both_t) * 1000
)
puts format(
  "%-25s %9.2f ms %9.2f ms %9.2f ms %9.2f ms",
  "P95 time",
  p95(full_t) * 1000, p95(only_t) * 1000, p95(excl_t) * 1000, p95(both_t) * 1000
)
puts format(
  "%-25s %12d %12d %12d %12d",
  "Avg allocations",
  avg(full_a), avg(only_a), avg(excl_a), avg(both_a)
)
puts

puts "Speedup vs full:"
puts format("  only_keys:       %5.1fx", avg(full_t) / avg(only_t))
puts format("  excluded_keys:   %5.1fx", avg(full_t) / avg(excl_t))
puts format("  both combined:   %5.1fx", avg(full_t) / avg(both_t))
puts
puts "only_keys vs both (marginal benefit of adding excluded_keys):"
puts format("  %5.1f%% faster", ((1.0 - avg(both_t) / avg(only_t)) * 100))
puts
