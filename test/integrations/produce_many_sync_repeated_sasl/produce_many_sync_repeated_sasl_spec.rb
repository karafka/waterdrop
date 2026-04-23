# frozen_string_literal: true

# Integration test to replicate GitHub issue #853 with SASL/PLAIN authentication:
# produce_many_sync succeeds on the first call but hangs until max_wait_timeout
# on subsequent calls from a long-lived producer, then raises WaitTimeoutError.
#
# SASL adds extra authentication handshakes that may interact with the FD poller
# differently than plain connections. The reporter's environment uses SASL, so this
# test specifically targets that configuration.
#
# Requires: docker compose -f docker-compose.sasl.yml up -d
# Excluded from CI (no SASL broker in standard CI pipeline).
#
# See: https://github.com/karafka/waterdrop/issues/853

require "waterdrop"
require "securerandom"

SASL_BOOTSTRAP = ENV.fetch("SASL_BOOTSTRAP_SERVERS", "127.0.0.1:9095")

# Verify SASL broker is reachable before running
require "socket"

begin
  host, port = SASL_BOOTSTRAP.split(":")
  sock = TCPSocket.new(host, port.to_i)
  sock.close
rescue => e
  puts "SASL broker at #{SASL_BOOTSTRAP} not reachable: #{e.message}"
  puts "Start it with: docker compose -f docker-compose.sasl.yml up -d"
  exit 1
end

MAX_WAIT_TIMEOUT = 20_000
PER_CALL_DEADLINE = 10
ITERATIONS = 5
IDLE_BETWEEN_CALLS = 2

failed = false

def run_test(label:, kafka_config:, polling_mode:, iterations:, idle:, deadline:, max_wait:)
  puts "\n--- #{label} (polling: #{polling_mode}, idle: #{idle}s) ---"

  producer = WaterDrop::Producer.new do |config|
    config.deliver = true
    config.kafka = kafka_config
    config.polling.mode = polling_mode
    config.max_wait_timeout = max_wait
  end

  topic = generate_topic("issue-853-sasl")

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

  if timings.size < iterations
    puts "  Only #{timings.size}/#{iterations} calls completed"
    test_failed = true
  end

  test_failed
end

sasl_config = {
  "bootstrap.servers": SASL_BOOTSTRAP,
  "request.required.acks": "all",
  "security.protocol": "SASL_PLAINTEXT",
  "sasl.mechanism": "PLAIN",
  "sasl.username": "testuser",
  "sasl.password": "testuser-secret"
}

# Test 1: SASL/PLAIN, both polling modes, short idle
[:fd, :thread].each do |polling_mode|
  result = run_test(
    label: "SASL/PLAIN",
    kafka_config: sasl_config,
    polling_mode: polling_mode,
    iterations: ITERATIONS,
    idle: IDLE_BETWEEN_CALLS,
    deadline: PER_CALL_DEADLINE,
    max_wait: MAX_WAIT_TIMEOUT
  )
  failed = true if result
end

# Test 2: SASL/PLAIN + statistics.interval.ms (common in production)
result = run_test(
  label: "SASL/PLAIN + statistics",
  kafka_config: sasl_config.merge("statistics.interval.ms": 1_000),
  polling_mode: :fd,
  iterations: ITERATIONS,
  idle: IDLE_BETWEEN_CALLS,
  deadline: PER_CALL_DEADLINE,
  max_wait: MAX_WAIT_TIMEOUT
)
failed = true if result

# Test 3: SASL/PLAIN with long idle (10s) — most likely to trigger the issue
result = run_test(
  label: "SASL/PLAIN long idle",
  kafka_config: sasl_config,
  polling_mode: :fd,
  iterations: 3,
  idle: 10,
  deadline: PER_CALL_DEADLINE,
  max_wait: MAX_WAIT_TIMEOUT
)
failed = true if result

# Test 4: SASL/PLAIN + statistics + long idle — maximum stress combo
result = run_test(
  label: "SASL/PLAIN + statistics + long idle",
  kafka_config: sasl_config.merge("statistics.interval.ms": 1_000),
  polling_mode: :fd,
  iterations: 3,
  idle: 10,
  deadline: PER_CALL_DEADLINE,
  max_wait: MAX_WAIT_TIMEOUT
)
failed = true if result

# Test 5: SASL/PLAIN produce_sync (single message) for comparison
puts "\n--- SASL/PLAIN produce_sync (fd mode, idle: #{IDLE_BETWEEN_CALLS}s) ---"

producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = sasl_config
  config.polling.mode = :fd
  config.max_wait_timeout = MAX_WAIT_TIMEOUT
end

topic = generate_topic("issue-853-single-sasl")

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
  puts "\nFAIL: Issue #853 reproduced with SASL - produce calls hang on subsequent invocations"
else
  puts "\nPASS: All SASL produce calls completed within deadline across all modes"
end

exit(failed ? 1 : 0)
