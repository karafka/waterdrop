# frozen_string_literal: true

# Integration test for WaterDrop produce_sync fiber yielding behavior
# Tests that produce_sync properly yields control when encountering blocking operations,
# allowing other fibers in the async gems ecosystem to run concurrently.
#
# The test sends each message to a unique randomly generated topic to create realistic
# delays from topic creation on every produce_sync call. This tests that WaterDrop operations
# properly yield control in the async ecosystem during actual Kafka operations.

require 'async'
require 'waterdrop'
require 'logger'
require 'securerandom'

producer = WaterDrop::Producer.new do |config|
  config.kafka = { 'bootstrap.servers': 'localhost:9092' }
end

message_template = {
  payload: 'integration test payload',
  key: 'test_key'
}

# Timing data for analysis
timing_data = {
  producer_calls: [],
  worker_ticks: [],
  counter_ticks: []
}

Async do |task|
  # Producer fiber - will have blocking behavior to test yielding
  producer_fiber = task.async do
    # Send each message to a unique new topic to ensure topic creation overhead per message
    5.times do |i|
      before_call = Time.now

      # Each message gets its own topic, guaranteeing topic creation delay
      unique_topic = "it-#{SecureRandom.hex(6)}-#{i}"
      message = message_template.merge(topic: unique_topic, key: "test_#{i}")

      producer.produce_sync(message)

      after_call = Time.now

      timing_data[:producer_calls] << {
        message_num: i + 1,
        start_time: before_call,
        end_time: after_call
      }
    end
  end

  # Worker fiber - should run concurrently
  worker_fiber = task.async do
    12.times do |i|
      tick_time = Time.now
      sleep(0.2) # Regular ticks to catch concurrent execution during topic creation

      timing_data[:worker_ticks] << {
        tick: i + 1,
        time: tick_time
      }
    end
  end

  # Counter fiber - also should run concurrently
  counter_fiber = task.async do
    8.times do |i|
      tick_time = Time.now
      sleep(0.3) # Different timing to increase chance of overlap

      timing_data[:counter_ticks] << {
        tick: i + 1,
        time: tick_time
      }
    end
  end

  # Wait for all fibers to complete
  [producer_fiber, worker_fiber, counter_fiber].each(&:wait)
end

producer.close

# Check for concurrent activity during producer calls
concurrent_during_calls = 0
timing_data[:producer_calls].each do |call|
  concurrent_worker = timing_data[:worker_ticks].select do |tick|
    tick[:time] >= call[:start_time] && tick[:time] <= call[:end_time]
  end

  concurrent_counter = timing_data[:counter_ticks].select do |tick|
    tick[:time] >= call[:start_time] && tick[:time] <= call[:end_time]
  end

  concurrent_during_calls += (concurrent_worker.size + concurrent_counter.size)
end

# Determine test result - success ONLY if we have concurrent execution during producer calls
# This ensures that Fiber.blocking{} or similar blocking behavior is detected
success = concurrent_during_calls > 0

# Exit with appropriate code
exit(success ? 0 : 1)
