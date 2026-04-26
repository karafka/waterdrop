# frozen_string_literal: true

# Integration test to replicate GitHub issue #853:
# produce_many_sync succeeds on the first call but hangs until max_wait_timeout
# on subsequent calls from a long-lived producer, then raises WaitTimeoutError.
#
# The delivery report never arrives from librdkafka on calls after the first one.
#
# The reporter's scenario involves a long-lived producer (e.g., Rails app) where calls
# are separated by idle periods. This test simulates that with sleeps between calls.
# It tests both :fd and :thread polling modes since the regression was introduced
# when the default switched from :thread to :fd in PR #827.
#
# See: https://github.com/karafka/waterdrop/issues/853

require "waterdrop"
require "securerandom"

MAX_WAIT_TIMEOUT = 20_000
# Each call should complete well under this threshold (in seconds).
# If produce_many_sync is hanging, it will hit the max_wait_timeout (20s).
# We use 10s as a generous upper bound — healthy calls finish in <1s.
PER_CALL_DEADLINE = 10

ITERATIONS = 5
# Idle time between produce calls to simulate a long-lived producer
# that doesn't produce continuously (like a web app handling requests)
IDLE_BETWEEN_CALLS = 2

BOOTSTRAP = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

failed = false

def run_test(label:, kafka_config:, polling_mode:, iterations:, idle:, deadline:, max_wait:)
  puts "\n--- #{label} (polling: #{polling_mode}, idle: #{idle}s) ---"

  producer = WaterDrop::Producer.new do |config|
    config.deliver = true
    config.kafka = kafka_config
    config.polling.mode = polling_mode
    config.max_wait_timeout = max_wait
  end

  topic = generate_topic("issue-853")

  errors = []
  timings = []

  producer.monitor.subscribe("error.occurred") do |event|
    errors << event[:error]
  end

  test_failed = false

  iterations.times do |i|
    sleep(idle) if i > 0

    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    begin
      producer.produce_many_sync(
        [{ topic: topic, payload: "probe-#{i}" }]
      )
    rescue => e
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
      timings << elapsed
      puts "  Call ##{i} FAILED after #{elapsed.round(3)}s: #{e.class}: #{e.message}"
      test_failed = true
      next
    end

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    timings << elapsed
    puts "  Call ##{i} completed in #{elapsed.round(3)}s"
  end

  producer.close

  timings.each_with_index do |t, i|
    if t >= deadline
      puts "  Call ##{i} exceeded deadline: #{t.round(3)}s >= #{deadline}s"
      test_failed = true
    end
  end

  if errors.any?
    puts "  Errors: #{errors.map { |e| "#{e.class}: #{e.message}" }.join(", ")}"
    test_failed = true
  end

  test_failed
end

base_config = {
  "bootstrap.servers": BOOTSTRAP,
  "request.required.acks": "all"
}

# Test 1: Both polling modes with short idle
[:fd, :thread].each do |polling_mode|
  result = run_test(
    label: "PLAINTEXT",
    kafka_config: base_config,
    polling_mode: polling_mode,
    iterations: ITERATIONS,
    idle: IDLE_BETWEEN_CALLS,
    deadline: PER_CALL_DEADLINE,
    max_wait: MAX_WAIT_TIMEOUT
  )
  failed = true if result
end

# Test 2: FD polling with statistics.interval.ms (common in production configs)
result = run_test(
  label: "PLAINTEXT + statistics",
  kafka_config: base_config.merge("statistics.interval.ms": 1_000),
  polling_mode: :fd,
  iterations: ITERATIONS,
  idle: IDLE_BETWEEN_CALLS,
  deadline: PER_CALL_DEADLINE,
  max_wait: MAX_WAIT_TIMEOUT
)
failed = true if result

# Test 3: FD polling with longer idle (10s) to simulate truly sporadic production
result = run_test(
  label: "PLAINTEXT long idle",
  kafka_config: base_config,
  polling_mode: :fd,
  iterations: 3,
  idle: 10,
  deadline: PER_CALL_DEADLINE,
  max_wait: MAX_WAIT_TIMEOUT
)
failed = true if result

# Test 4: produce_sync (single message) for comparison
puts "\n--- PLAINTEXT produce_sync (fd mode, idle: #{IDLE_BETWEEN_CALLS}s) ---"

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = base_config
  config.polling.mode = :fd
  config.max_wait_timeout = MAX_WAIT_TIMEOUT
end

topic = generate_topic("issue-853-single")

ITERATIONS.times do |i|
  sleep(IDLE_BETWEEN_CALLS) if i > 0

  start = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  begin
    producer.produce_sync(topic: topic, payload: "single-probe-#{i}")
  rescue => e
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    puts "  produce_sync ##{i} FAILED after #{elapsed.round(3)}s: #{e.class}: #{e.message}"
    failed = true
    next
  end

  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
  puts "  produce_sync ##{i} completed in #{elapsed.round(3)}s"

  if elapsed >= PER_CALL_DEADLINE
    puts "  produce_sync ##{i} exceeded deadline: #{elapsed.round(3)}s"
    failed = true
  end
end

producer.close

if failed
  puts "\nFAIL: Issue #853 reproduced - produce calls hang on subsequent invocations"
else
  puts "\nPASS: All produce calls completed within deadline across all modes"
end

exit(failed ? 1 : 0)
