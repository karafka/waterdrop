# frozen_string_literal: true

# Integration test for WaterDrop connection pool under concurrent load
# Tests that connection pooling works correctly under heavy concurrent usage
# and validates proper resource management and thread safety.
#
# This validates:
# - Thread-safe producer pooling
# - Proper producer checkout/check-in
# - No resource leaks under concurrent load
# - Successful message delivery from multiple threads

require 'waterdrop'
require 'securerandom'

# Pool size for this spec
POOL_SIZE = 3
# Thread count for this spec
THREAD_COUNT = 10
# How many messages to produce in this spec
MESSAGES_PER_THREAD = 5

# Create connection pool
pool = WaterDrop::ConnectionPool.new(size: POOL_SIZE) do |config|
  config.kafka = { 'bootstrap.servers': ENV.fetch('BOOTSTRAP_SERVERS', '127.0.0.1:9092') }
end

# Track results
successful_sends = []
errors = []
mutex = Mutex.new

# Create threads that will compete for producers from the pool
threads = Array.new(THREAD_COUNT) do |thread_index|
  Thread.new do
    MESSAGES_PER_THREAD.times do |msg_index|
      # Use a producer from the pool
      pool.with do |producer|
        topic = "it-pool-#{SecureRandom.hex(6)}"

        result = producer.produce_sync(
          topic: topic,
          key: "thread-#{thread_index}",
          payload: "msg-#{msg_index}"
        )

        mutex.synchronize do
          successful_sends << {
            thread: thread_index,
            msg: msg_index,
            partition: result.partition
          }
        end
      end

      # Small random delay to increase contention
      sleep(rand * 0.01)
    end
  rescue StandardError => e
    mutex.synchronize do
      errors << { thread: thread_index, error: e.message, backtrace: e.backtrace[0..2] }
    end
  end
end

# Wait for all threads to complete
threads.each(&:join)

# Verify pool stats are consistent after load
final_stats = pool.stats

pool.shutdown

# Validate results
expected_message_count = THREAD_COUNT * MESSAGES_PER_THREAD
success = errors.empty? &&
          successful_sends.size == expected_message_count &&
          final_stats[:size] == POOL_SIZE

unless success
  puts "Errors encountered: #{errors.inspect}" unless errors.empty?
  puts "Expected #{expected_message_count} messages, got #{successful_sends.size}"
  puts "Final pool stats: #{final_stats.inspect}"
  puts 'Pool stats size mismatch' if final_stats[:size] != POOL_SIZE
end

exit(success ? 0 : 1)
