# frozen_string_literal: true

# Integration test for FD polling deadlock prevention
#
# Tests that closing a producer from within callbacks doesn't deadlock.
# This can happen when user code in callbacks decides to close the producer
# based on certain conditions (e.g., too many errors, specific delivery failures).
#
# The fix ensures that when unregister is called from the poller thread itself,
# it handles the close directly instead of signaling and waiting.

require "waterdrop"
require "securerandom"
require "timeout"

DEADLOCK_TIMEOUT = 10 # seconds

topic = "it-fd-deadlock-#{SecureRandom.hex(6)}"
failed = false

# Test 1: Close producer from delivery callback
begin
  Timeout.timeout(DEADLOCK_TIMEOUT) do
    producer = WaterDrop::Producer.new do |config|
      config.kafka = {
        "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
      }
      config.polling.mode = :fd
      config.max_wait_timeout = 5_000
    end

    close_triggered = false

    producer.monitor.subscribe("message.acknowledged") do |_event|
      unless close_triggered
        close_triggered = true
        producer.close
      end
    end

    producer.produce_async(topic: topic, payload: "test")
    sleep(0.5) while producer.status.active?

    unless producer.status.closed?
      puts "Test 1: Producer should be closed"
      failed = true
    end
  end
rescue Timeout::Error
  puts "Test 1: Deadlock detected"
  failed = true
end

# Test 2: Normal close works correctly
begin
  Timeout.timeout(DEADLOCK_TIMEOUT) do
    producer = WaterDrop::Producer.new do |config|
      config.kafka = {
        "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
      }
      config.polling.mode = :fd
      config.max_wait_timeout = 5_000
    end

    producer.produce_sync(topic: topic, payload: "test")
    producer.close

    unless producer.status.closed?
      puts "Test 2: Producer should be closed"
      failed = true
    end
  end
rescue Timeout::Error
  puts "Test 2: Deadlock detected"
  failed = true
end

# Test 3: Rapid open/close cycles
begin
  Timeout.timeout(DEADLOCK_TIMEOUT) do
    5.times do |i|
      producer = WaterDrop::Producer.new do |config|
        config.kafka = {
          "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
        }
        config.polling.mode = :fd
        config.max_wait_timeout = 5_000
      end

      producer.produce_sync(topic: topic, payload: "cycle-#{i}")
      producer.close
    end
  end
rescue Timeout::Error
  puts "Test 3: Deadlock detected"
  failed = true
end

# Test 4: Multiple producers closing from callbacks
begin
  Timeout.timeout(DEADLOCK_TIMEOUT) do
    producers = []
    mutex = Mutex.new

    3.times do
      producer = WaterDrop::Producer.new do |config|
        config.kafka = {
          "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
        }
        config.polling.mode = :fd
        config.max_wait_timeout = 5_000
      end

      producer.monitor.subscribe("message.acknowledged") do |_event|
        mutex.synchronize do
          producer.close if producer.status.active?
        end
      end

      producers << producer
    end

    producers.each { |p| p.produce_async(topic: topic, payload: "multi-test") }

    deadline = Time.now + 5
    until producers.all? { |p| p.status.closed? }
      if Time.now > deadline
        puts "Test 4: Timed out waiting for producers to close"
        failed = true
        break
      end
      sleep(0.1)
    end
  end
rescue Timeout::Error
  puts "Test 4: Deadlock detected"
  failed = true
end

# Test 5: Close during high-volume production
begin
  Timeout.timeout(DEADLOCK_TIMEOUT) do
    producer = WaterDrop::Producer.new do |config|
      config.kafka = {
        "bootstrap.servers": ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
      }
      config.polling.mode = :fd
      config.max_wait_timeout = 5_000
    end

    message_count = 0
    close_at = 50

    producer.monitor.subscribe("message.acknowledged") do |_event|
      message_count += 1
      producer.close if message_count >= close_at && producer.status.active?
    end

    100.times do |i|
      break unless producer.status.active?

      producer.produce_async(topic: topic, payload: "volume-#{i}")
    rescue WaterDrop::Errors::ProducerClosedError
      break
    end

    # Wait for producer to fully close with explicit deadline
    deadline = Time.now + 5
    until producer.status.closed?
      if Time.now > deadline
        puts "Test 5: Timed out waiting for producer to close"
        failed = true
        break
      end
      sleep(0.1)
    end

    unless failed || producer.status.closed?
      puts "Test 5: Producer should be closed"
      failed = true
    end
  end
rescue Timeout::Error
  puts "Test 5: Deadlock detected"
  failed = true
end

exit(failed ? 1 : 0)
