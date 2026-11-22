# frozen_string_literal: true

# Integration test for the `partition_count_cache_ttl_ms` setting.
#
# The setting configures the process-wide karafka-rdkafka partition count cache used to route
# messages with `partition_key`. A producer that sets it changes the TTL for all the producers.
# A producer that leaves it as `nil` does not reset it. Once the TTL expires, a producer sees
# partitions added to a topic it already produced to.

require "waterdrop"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

TTL_MS = 1_000

unless Rdkafka::Config.partition_count_cache_ttl_ms > TTL_MS * 10
  warn "FAIL: karafka-rdkafka default TTL #{Rdkafka::Config.partition_count_cache_ttl_ms} ms is not far above #{TTL_MS} ms"
  exit(1)
end

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
  config.partition_count_cache_ttl_ms = TTL_MS
end

unless Rdkafka::Config.partition_count_cache_ttl_ms == TTL_MS
  warn "FAIL: producer setup did not set the TTL: expected #{TTL_MS}, got #{Rdkafka::Config.partition_count_cache_ttl_ms}"
  exit(1)
end

other = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

unless Rdkafka::Config.partition_count_cache_ttl_ms == TTL_MS
  warn "FAIL: producer setup without the setting changed the TTL to #{Rdkafka::Config.partition_count_cache_ttl_ms}"
  exit(1)
end

topic = generate_topic("partition-count-ttl")
create_topic(topic, partitions: 1)

producer.produce_sync(topic: topic, payload: "before", partition_key: "key")

unless producer.partition_count(topic) == 1
  warn "FAIL: expected a cached partition count of 1, got #{producer.partition_count(topic)}"
  exit(1)
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP_SERVERS).admin
admin.create_partitions(topic, 3).wait
admin.close

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
count = nil

loop do
  count = producer.partition_count(topic)
  break if count == 3
  break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

  sleep(0.1)
end

unless count == 3
  warn "FAIL: new partitions not seen after the TTL expired: expected 3, got #{count}"
  exit(1)
end

producer.close
other.close

puts "PASS: partition_count_cache_ttl_ms drives the process-wide partition count cache"
