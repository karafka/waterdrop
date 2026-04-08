# frozen_string_literal: true

# Integration test for two independent transactional producers in FD polling mode
#
# Tests that:
# 1. Two separate transactional producers can run transactions concurrently in :fd mode
# 2. Neither producer deadlocks or blocks the other
# 3. All transactions from both producers commit successfully
# 4. Delivery reports are received correctly for both producers
# 5. Producers truly run in parallel (total time well under sequential sum)

require "waterdrop"
require "securerandom"
require "timeout"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TRANSACTIONS_PER_PRODUCER = 2
MESSAGES_PER_TRANSACTION = 5
SLEEP_PER_TRANSACTION = 5 # seconds - sleep between transactions to prove parallelism
DEADLOCK_TIMEOUT = 30 # seconds
# If producers ran sequentially, total would be 2 * 2 * 5 = 20s. In parallel it should be ~10s.
MAX_PARALLEL_TIME = 15 # seconds - generous upper bound for parallel execution

topic = generate_topic("fd-two-tx")
create_topic(topic)

errors = []
mutex = Mutex.new

producer1_deliveries = []
producer2_deliveries = []

producer1 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "transactional.id": generate_topic("fd-two-tx-p1")
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 30_000
end

producer2 = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "transactional.id": generate_topic("fd-two-tx-p2")
  }
  config.polling.mode = :fd
  config.max_wait_timeout = 30_000
end

producer1.monitor.subscribe("message.acknowledged") do |event|
  mutex.synchronize { producer1_deliveries << event[:offset] }
end

producer2.monitor.subscribe("message.acknowledged") do |event|
  mutex.synchronize { producer2_deliveries << event[:offset] }
end

producer1.monitor.subscribe("error.occurred") do |event|
  mutex.synchronize { errors << "producer1: #{event[:error].message}" }
end

producer2.monitor.subscribe("error.occurred") do |event|
  mutex.synchronize { errors << "producer2: #{event[:error].message}" }
end

failed = false
thread1 = nil
thread2 = nil
start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

begin
  Timeout.timeout(DEADLOCK_TIMEOUT) do
    # Run both producers' transactions concurrently in separate threads
    thread1 = Thread.new do
      TRANSACTIONS_PER_PRODUCER.times do |tx_index|
        producer1.transaction do
          MESSAGES_PER_TRANSACTION.times do |msg_index|
            producer1.produce_async(
              topic: topic,
              key: "p1-tx-#{tx_index}",
              payload: "p1-#{tx_index}-#{msg_index}"
            )
          end
        end

        sleep(SLEEP_PER_TRANSACTION)
      end
    rescue => e
      mutex.synchronize { errors << "producer1 thread: #{e.message}" }
    end

    thread2 = Thread.new do
      TRANSACTIONS_PER_PRODUCER.times do |tx_index|
        producer2.transaction do
          MESSAGES_PER_TRANSACTION.times do |msg_index|
            producer2.produce_async(
              topic: topic,
              key: "p2-tx-#{tx_index}",
              payload: "p2-#{tx_index}-#{msg_index}"
            )
          end
        end

        sleep(SLEEP_PER_TRANSACTION)
      end
    rescue => e
      mutex.synchronize { errors << "producer2 thread: #{e.message}" }
    end

    thread1.join
    thread2.join
  end
rescue Timeout::Error
  puts "Deadlock detected: two transactional producers blocked each other in :fd mode"
  failed = true

  # Kill threads to avoid hanging on close
  thread1&.kill
  thread2&.kill
ensure
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time

  begin
    producer1.close
  rescue
    nil
  end

  begin
    producer2.close
  rescue
    nil
  end
end

expected_per_producer = TRANSACTIONS_PER_PRODUCER * MESSAGES_PER_TRANSACTION

if errors.any?
  puts "Errors occurred: #{errors.inspect}"
  failed = true
end

if producer1_deliveries.size != expected_per_producer
  puts "Producer 1: expected #{expected_per_producer} deliveries, got #{producer1_deliveries.size}"
  failed = true
end

if producer2_deliveries.size != expected_per_producer
  puts "Producer 2: expected #{expected_per_producer} deliveries, got #{producer2_deliveries.size}"
  failed = true
end

if elapsed > MAX_PARALLEL_TIME
  puts "Producers did not run in parallel: took #{elapsed.round(1)}s (max #{MAX_PARALLEL_TIME}s)"
  failed = true
end

exit(failed ? 1 : 0)
