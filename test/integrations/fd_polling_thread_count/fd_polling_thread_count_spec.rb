# frozen_string_literal: true

# Integration test comparing thread counts between FD polling and thread polling modes
#
# Tests that FD mode uses significantly fewer threads than thread mode when running
# multiple producers. With N producers:
# - Thread mode: N additional polling threads (one per producer)
# - FD mode: 1 shared polling thread (regardless of producer count)

require "waterdrop"
require "securerandom"

PRODUCER_COUNT = 10
BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

def create_producer(polling_mode:)
  WaterDrop::Producer.new do |config|
    config.deliver = true
    config.kafka = {
      "bootstrap.servers": BOOTSTRAP_SERVERS
    }
    config.polling.mode = polling_mode
    config.max_wait_timeout = 5_000
    config.logger = Logger.new(File::NULL)
  end
end

def thread_count
  Thread.list.count
end

topic = "it-fd-thread-count-#{SecureRandom.hex(6)}"
failed = false

# Get baseline thread count
baseline_threads = thread_count

# Test 1: Thread mode - should add N threads for N producers
thread_mode_producers = []

PRODUCER_COUNT.times do
  thread_mode_producers << create_producer(polling_mode: :thread)
end

# Warm up to ensure all threads are started
thread_mode_producers.each do |producer|
  producer.produce_sync(topic: topic, payload: "warmup")
end

sleep(0.5)

thread_mode_threads = thread_count
thread_mode_added = thread_mode_threads - baseline_threads

# Close all thread mode producers
thread_mode_producers.each(&:close)
sleep(0.3)

after_thread_cleanup = thread_count

# Test 2: FD mode - should add only 1 thread regardless of producer count
fd_mode_producers = []

PRODUCER_COUNT.times do
  fd_mode_producers << create_producer(polling_mode: :fd)
end

# Warm up to ensure poller thread is started
fd_mode_producers.each do |producer|
  producer.produce_sync(topic: topic, payload: "warmup")
end

sleep(0.5)

fd_mode_threads = thread_count
fd_mode_added = fd_mode_threads - after_thread_cleanup

# Close all FD mode producers
fd_mode_producers.each(&:close)
sleep(0.3)

# Output results
puts "Baseline threads: #{baseline_threads}"
puts "Thread mode with #{PRODUCER_COUNT} producers: +#{thread_mode_added} threads"
puts "FD mode with #{PRODUCER_COUNT} producers: +#{fd_mode_added} threads"

# Verify thread mode added at least N threads (one per producer)
if thread_mode_added < PRODUCER_COUNT
  puts "FAIL: Thread mode should add at least #{PRODUCER_COUNT} threads, got #{thread_mode_added}"
  failed = true
end

# Verify FD mode added only 1 thread (the shared poller)
if fd_mode_added != 1
  puts "FAIL: FD mode should add exactly 1 thread, got #{fd_mode_added}"
  failed = true
end

# Verify FD mode uses significantly fewer threads
if fd_mode_added >= thread_mode_added
  puts "FAIL: FD mode should use fewer threads than thread mode"
  failed = true
end

if failed
  puts "FAILED"
  exit 1
else
  puts "PASSED: FD mode uses #{thread_mode_added - fd_mode_added} fewer threads than thread mode"
  exit 0
end
