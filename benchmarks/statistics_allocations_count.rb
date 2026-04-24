# frozen_string_literal: true

# Counts exact object allocations from StatisticsDecorator
#
# Usage: ruby benchmarks/statistics_allocations_count.rb

$LOAD_PATH.unshift(File.expand_path("../../karafka-core/lib", __dir__))
require "karafka-core"

def build_producer_stats(num_brokers: 5, num_topics: 1, partitions_per_topic: 100)
  stats = {
    "name" => "waterdrop-abc123", "client_id" => "waterdrop", "type" => "producer",
    "ts" => 1_700_000_000_000, "time" => 1_700_000_000, "age" => 120_000_000,
    "replyq" => 0, "msg_cnt" => 42, "msg_size" => 8400,
    "msg_max" => 1_000_000, "msg_size_max" => 1_000_000_000,
    "tx" => 15_230, "tx_bytes" => 4_500_000, "rx" => 14_800, "rx_bytes" => 3_200_000,
    "txmsgs" => 10_000, "txmsg_bytes" => 2_500_000, "rxmsgs" => 200,
    "rxmsg_bytes" => 50_000, "simple_cnt" => 0, "metadata_cache_cnt" => 20
  }

  brokers = {}
  num_brokers.times do |i|
    broker_name = "localhost:#{9092 + i}/#{i}"
    brokers[broker_name] = {
      "name" => broker_name, "nodeid" => i, "nodename" => "localhost:#{9092 + i}",
      "source" => "learned", "state" => "UP", "stateage" => 60_000_000,
      "outbuf_cnt" => 0, "outbuf_msg_cnt" => 0, "waitresp_cnt" => 0,
      "waitresp_msg_cnt" => 0, "tx" => 1500, "tx_bytes" => 450_000,
      "txerrs" => 0, "txretries" => 0, "txidle" => 0, "req_timeouts" => 0,
      "rx" => 1400, "rx_bytes" => 320_000, "rxerrs" => 0, "rxcorriderrs" => 0,
      "rxpartial" => 0, "rxidle" => 0, "zbuf_grow" => 0, "buf_grow" => 0,
      "wakeups" => 500, "connects" => 3, "disconnects" => 0,
      "int_latency" => { "min" => 5, "max" => 500, "avg" => 50, "sum" => 25_000,
        "stddev" => 30, "p50" => 40, "p75" => 60, "p90" => 80, "p95" => 100,
        "p99" => 200, "p99_99" => 450, "cnt" => 500, "hdrsize" => 11376 },
      "outbuf_latency" => { "min" => 1, "max" => 100, "avg" => 10, "sum" => 5000,
        "stddev" => 8, "p50" => 8, "p75" => 12, "p90" => 18, "p95" => 25,
        "p99" => 50, "p99_99" => 95, "cnt" => 500, "hdrsize" => 11376 },
      "rtt" => { "min" => 100, "max" => 5000, "avg" => 500, "sum" => 250_000,
        "stddev" => 200, "p50" => 400, "p75" => 600, "p90" => 800, "p95" => 1000,
        "p99" => 2000, "p99_99" => 4500, "cnt" => 500, "hdrsize" => 11376 },
      "throttle" => { "min" => 0, "max" => 0, "avg" => 0, "sum" => 0,
        "stddev" => 0, "p50" => 0, "p75" => 0, "p90" => 0, "p95" => 0,
        "p99" => 0, "p99_99" => 0, "cnt" => 0, "hdrsize" => 11376 },
      "req" => { "Produce" => 1000, "Fetch" => 0, "ListOffsets" => 10,
        "Metadata" => 5, "OffsetCommit" => 0, "OffsetFetch" => 0,
        "FindCoordinator" => 2, "JoinGroup" => 0, "Heartbeat" => 0,
        "LeaveGroup" => 0, "SyncGroup" => 0, "DescribeGroups" => 0,
        "ListGroups" => 0, "SaslHandshake" => 0, "ApiVersion" => 1,
        "CreateTopics" => 0, "DeleteTopics" => 0, "DeleteRecords" => 0,
        "InitProducerId" => 1, "OffsetForLeaderEpoch" => 0,
        "AddPartitionsToTxn" => 0, "AddOffsetsToTxn" => 0,
        "EndTxn" => 0, "TxnOffsetCommit" => 0, "SaslAuthenticate" => 0 },
      "toppars" => {}
    }
  end
  stats["brokers"] = brokers

  topics = {}
  num_topics.times do |t|
    topic_name = "topic_#{t}"
    partitions = {}
    (partitions_per_topic + 1).times do |p|
      pid = p < partitions_per_topic ? p : -1
      partitions[pid.to_s] = {
        "partition" => pid, "broker" => pid % [num_brokers, 1].max,
        "leader" => pid % [num_brokers, 1].max, "desired" => false, "unknown" => false,
        "msgq_cnt" => 0, "msgq_bytes" => 0, "xmit_msgq_cnt" => 0,
        "xmit_msgq_bytes" => 0, "fetchq_cnt" => 0, "fetchq_size" => 0,
        "fetch_state" => "none", "query_offset" => -1001, "next_offset" => 0,
        "app_offset" => -1001, "stored_offset" => -1001, "committed_offset" => -1001,
        "eof_offset" => -1001, "lo_offset" => -1001, "hi_offset" => -1001,
        "ls_offset" => -1001, "consumer_lag" => -1, "consumer_lag_stored" => -1,
        "txmsgs" => 100, "txbytes" => 25_000, "rxmsgs" => 0, "rxbytes" => 0,
        "msgs" => 100, "rx_ver_drops" => 0, "msgs_inflight" => 0,
        "next_ack_seq" => 0, "next_err_seq" => 0, "acked_msgid" => 0
      }
    end
    topics[topic_name] = {
      "topic" => topic_name, "age" => 120_000_000, "metadata_age" => 5000,
      "batchsize" => { "min" => 100, "max" => 50_000, "avg" => 10_000, "sum" => 5_000_000,
        "stddev" => 5000, "p50" => 8000, "p75" => 12_000, "p90" => 20_000,
        "p95" => 30_000, "p99" => 45_000, "p99_99" => 49_000, "cnt" => 500,
        "hdrsize" => 14512 },
      "batchcnt" => { "min" => 1, "max" => 100, "avg" => 10, "sum" => 5000,
        "stddev" => 8, "p50" => 8, "p75" => 12, "p90" => 20,
        "p95" => 30, "p99" => 50, "p99_99" => 95, "cnt" => 500,
        "hdrsize" => 8304 },
      "partitions" => partitions
    }
  end
  stats["topics"] = topics
  stats
end

def deep_copy(h) = Marshal.load(Marshal.dump(h))

def count_allocations
  GC.disable
  before = GC.stat(:total_allocated_objects)
  yield
  after = GC.stat(:total_allocated_objects)
  GC.enable
  after - before
end

configs = [
  ["1 topic, 100 parts, 5 brokers", { num_brokers: 5, num_topics: 1, partitions_per_topic: 100 }],
  ["10 topics, 100 parts, 5 brokers", { num_brokers: 5, num_topics: 10, partitions_per_topic: 100 }],
  ["20 topics, 100 parts, 10 brokers", { num_brokers: 10, num_topics: 20, partitions_per_topic: 100 }],
]

emissions_per_min = 12  # every 5 seconds

puts "=" * 80
puts "Object allocations from StatisticsDecorator (decorator only, no deep_copy)"
puts "=" * 80
puts

configs.each do |label, params|
  template = build_producer_stats(**params)
  total_partitions = params[:num_topics] * params[:partitions_per_topic]

  # Measure deep_copy baseline
  # warm
  deep_copy(template)
  copy_allocs = count_allocations { deep_copy(template) }

  # Full decoration
  dec = Karafka::Core::Monitoring::StatisticsDecorator.new
  3.times { dec.call(deep_copy(template)) }
  stats = deep_copy(template)
  full_allocs = count_allocations { dec.call(stats) }

  # only_keys
  dec2 = Karafka::Core::Monitoring::StatisticsDecorator.new(only_keys: %w[tx txretries txerrs rxerrs])
  3.times { dec2.call(deep_copy(template)) }
  stats = deep_copy(template)
  only_allocs = count_allocations { dec2.call(stats) }

  # both
  dec3 = Karafka::Core::Monitoring::StatisticsDecorator.new(
    only_keys: %w[tx txretries txerrs rxerrs],
    excluded_keys: %w[int_latency outbuf_latency rtt throttle req toppars topics]
  )
  3.times { dec3.call(deep_copy(template)) }
  stats = deep_copy(template)
  both_allocs = count_allocations { dec3.call(stats) }

  puts "#{label} (#{total_partitions} total partitions):"
  puts format("  %-30s %8s %12s", "", "Per call", "Per minute")
  puts format("  %-30s %8s %12s", "-" * 30, "-" * 8, "-" * 12)
  puts format("  %-30s %8d %12d", "Full decoration", full_allocs, full_allocs * emissions_per_min)
  puts format("  %-30s %8d %12d", "only_keys", only_allocs, only_allocs * emissions_per_min)
  puts format("  %-30s %8d %12d", "only_keys + excluded_keys", both_allocs, both_allocs * emissions_per_min)
  puts
end
