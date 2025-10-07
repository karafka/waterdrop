# frozen_string_literal: true

# Integration test for WaterDrop transactional producer concurrent operations
# Tests that multiple threads can safely use transactional producers simultaneously
# and that all transactions complete successfully without race conditions.
#
# This validates:
# - Thread-safe transaction operations
# - Proper isolation between concurrent transactions
# - No deadlocks or race conditions in transaction lifecycle

require 'waterdrop'
require 'securerandom'

THREAD_COUNT = 5
MESSAGES_PER_THREAD = 10

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': ENV.fetch('BOOTSTRAP_SERVERS', '127.0.0.1:9092'),
    'transactional.id': "it-tx-concurrent-#{SecureRandom.hex(6)}"
  }
end

# Track successful operations
successful_transactions = []
errors = []
mutex = Mutex.new

# Create multiple threads that perform transactions concurrently
threads = Array.new(THREAD_COUNT) do |thread_index|
  Thread.new do
    # Each thread performs multiple transactions
    MESSAGES_PER_THREAD.times do |msg_index|
      topic = "it-tx-concurrent-#{SecureRandom.hex(6)}"

      producer.transaction do
        # Send multiple messages per transaction
        3.times do |batch_index|
          producer.produce_async(
            topic: topic,
            key: "thread-#{thread_index}",
            payload: "msg-#{msg_index}-#{batch_index}"
          )
        end
      end

      mutex.synchronize do
        successful_transactions << "thread-#{thread_index}-tx-#{msg_index}"
      end
    end
  rescue StandardError => e
    mutex.synchronize do
      errors << { thread: thread_index, error: e.message }
    end
  end
end

# Wait for all threads to complete
threads.each(&:join)

producer.close

# Validate results
expected_transaction_count = THREAD_COUNT * MESSAGES_PER_THREAD
success = errors.empty? && successful_transactions.size == expected_transaction_count

unless success
  puts "Errors encountered: #{errors.inspect}" unless errors.empty?
  puts "Expected #{expected_transaction_count} transactions, got #{successful_transactions.size}"
end

exit(success ? 0 : 1)
