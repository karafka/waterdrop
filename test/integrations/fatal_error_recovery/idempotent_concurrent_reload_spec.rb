# frozen_string_literal: true

# Integration test reproducing the concurrent idempotent fatal-error reload race.
#
# Multiple threads share a single idempotent producer with `reload_on_idempotent_fatal_error`
# enabled. `#produce` increments the operations counter under `@operating_mutex` but RELEASES it
# before the actual `client.produce`, so several threads can sit inside `client.produce` at the same
# time. When the underlying client enters a fatal state, those in-flight produces all fail fatally
# and each thread independently enters the idempotent reload path. `@operating_mutex` serializes the
# reload bodies, but the second reload was unguarded: the first reload sets `@client = nil` (status
# `:configured`), then the second runs `reload!`, which calls `@client.flush` on `nil` and raises
# `NoMethodError`. The transactional reload path already has a `return if @status.configured?` guard;
# the idempotent path lacked an equivalent one.
#
# The fault is a REAL librdkafka fatal error injected through the shipped
# `WaterDrop::Producer::Testing` module (`rd_kafka_test_fatal_error`), and the contention is driven
# by real threads. No WaterDrop internals are stubbed.

require "waterdrop"
require "waterdrop/producer/testing"
require "logger"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

THREADS = 8
ROUNDS = 5

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "enable.idempotence": true,
    "request.required.acks": "all"
  }
  config.max_wait_timeout = 2_000
  config.reload_on_idempotent_fatal_error = true
  config.wait_backoff_on_idempotent_fatal_error = 50
  # High enough that every contending thread can reach the reload path within a round without
  # exhausting the retry budget before the race has a chance to manifest.
  config.max_attempts_on_idempotent_fatal_error = (THREADS * ROUNDS) + 10
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

producer.singleton_class.include(WaterDrop::Producer::Testing)

topic = generate_topic("idem-concurrent-reload")

# Connect and confirm the producer is healthy before we start injecting faults.
producer.produce_sync(topic: topic, payload: "warmup")

reloads = 0
producer.monitor.subscribe("producer.reloaded") { reloads += 1 }

crashes = []

ROUNDS.times do |round|
  # Put the real librdkafka producer into a fatal state. Every produce after this fails fatally
  # until the client is reloaded.
  producer.trigger_test_fatal_error(47, "Injected fatal for concurrent reload round #{round}")

  go = Queue.new
  threads = Array.new(THREADS) do |i|
    Thread.new do
      go.pop
      producer.produce_sync(topic: topic, payload: "r#{round}-m#{i}")
    rescue NoMethodError => e
      # The bug: reload! ran while @client was already nil (a concurrent reload had reset it).
      crashes << e
    rescue WaterDrop::Errors::ProduceError
      # Acceptable: a fatal error that exhausted the retry budget surfaces as a clean ProduceError.
      nil
    end
  end

  # Release all threads as simultaneously as possible so several are inside client.produce when the
  # fatal error hits and more than one enters the reload path concurrently.
  THREADS.times { go << true }
  threads.each(&:join)

  # Stop early once the bug is reproduced; no point hammering a producer in a broken state.
  break unless crashes.empty?
end

producer.close

failures = []

unless crashes.empty?
  failures << "#{crashes.size} thread(s) crashed in the reload path: " \
              "#{crashes.map(&:message).uniq.join(" | ")}"
end

# Sanity: make sure we actually exercised the reload path, otherwise the test proves nothing.
failures << "no reloads happened - the fatal-error path was never exercised" if reloads.zero?

if failures.empty?
  puts "PASS: concurrent idempotent fatal-error reload stayed safe (#{reloads} reloads, no crashes)"
  exit(0)
else
  failures.each { |failure| warn "FAIL: #{failure}" }
  exit(1)
end
