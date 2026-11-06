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

failed = check(
  failed,
  Rdkafka::Config.partition_count_cache_ttl_ms > TTL_MS * 10,
  "karafka-rdkafka default TTL is far above the configured one"
)

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
  config.partition_count_cache_ttl_ms = TTL_MS
end

failed = check(
  failed,
  Rdkafka::Config.partition_count_cache_ttl_ms == TTL_MS,
  "producer setup sets the process-wide TTL"
)

other = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
end

failed = check(
  failed,
  Rdkafka::Config.partition_count_cache_ttl_ms == TTL_MS,
  "producer setup without the setting leaves the process-wide TTL untouched"
)

topic = generate_topic("partition-count-ttl")
create_topic(topic, partitions: 1)

producer.produce_sync(topic: topic, payload: "before", partition_key: "key")

failed = check(failed, producer.partition_count(topic) == 1, "partition count is cached as 1")

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

failed = check(failed, count == 3, "new partitions are seen once the TTL expires (got #{count})")

producer.close
other.close

if failed
  puts "\nFAIL: partition_count_cache_ttl_ms does not drive the partition count cache"
else
  puts "\nPASS: partition_count_cache_ttl_ms drives the process-wide partition count cache"
end

exit(failed ? 1 : 0)
