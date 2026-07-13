# frozen_string_literal: true

# Integration test reproducing the librdkafka#4849 abort-after-commit race at the WaterDrop level.
#
# librdkafka defect: https://github.com/confluentinc/librdkafka/issues/4849
#
# Aborting a transaction immediately after an async produce, on a producer that JUST committed a
# previous transaction, can make the abort's EndTxn fail with a fatal INVALID_TXN_STATE under load:
# the very first produce is still in flight and the transaction has not yet been registered at the
# transaction coordinator when the abort is issued.
#
# Karafka mitigates this in its transactional consumers by (a) sleeping briefly before the abort so
# the produce registers at the coordinator, and (b) treating the resulting fatal as a recoverable
# blip - WaterDrop reloads the client on the fatal and the work is retried. See karafka's
# spec/integrations/pro/consumption/parallel_segments/with_transactions/with_aborted_transaction_spec.rb
#
# This spec exercises the SAME sequence directly against WaterDrop, deliberately WITHOUT the
# mitigating sleep, to lock in WaterDrop's own contract: even if librdkafka#4849 fires, WaterDrop
# must never crash or deadlock. The fatal INVALID_TXN_STATE is reloaded away (reload is on by
# default) and the producer stays fully usable afterwards. If the race does not fire on a given run
# (it is timing dependent) the aborts simply succeed. Either way the process must finish cleanly and
# every producer must be able to commit a fresh transaction at the end.

require "waterdrop"
require "logger"
require "securerandom"
require "timeout"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Producers running the commit->abort cycle concurrently. The librdkafka#4849 window only opens
# under load, so these counts are deliberately tuned high enough that the fatal INVALID_TXN_STATE
# actually shows up (a handful of times per run locally) instead of the spec silently degrading into
# a smoke test. Firing is still timing dependent and is NOT required for the spec to pass - see the
# reload_consistent check at the bottom.
PRODUCER_COUNT = 8
# How many commit-then-immediately-abort cycles each producer performs.
CYCLES_PER_PRODUCER = 40
# Fail-safe: if a cycle deadlocked (the failure mode this whole family of specs guards against) we
# want a clear FAIL, not a hung runner.
DEADLOCK_TIMEOUT = 300

topic = generate_topic("tx-abort-race")
create_topic(topic)

mutex = Mutex.new
# Recoverable librdkafka#4849 blips (fatal INVALID_TXN_STATE on the abort's EndTxn).
race_errors = []
# producer.reloaded events - the recovery path we expect when the race fires.
reload_events = []
# transaction.aborted instrumentation - confirms the aborts actually ran.
abort_events = []

build_producer = lambda do
  WaterDrop::Producer.new do |config|
    config.kafka = {
      "bootstrap.servers": BOOTSTRAP_SERVERS,
      "transactional.id": generate_topic("tx-abort-race-id"),
      "transaction.timeout.ms": 30_000,
      "message.timeout.ms": 30_000
    }
    config.max_wait_timeout = 30_000
    # Defaults already enable reload; we make the backoff short so the recovery path is quick.
    config.reload_on_transaction_fatal_error = true
    config.wait_backoff_on_transaction_fatal_error = 100
    config.logger = Logger.new($stdout, level: Logger::ERROR)
  end
end

producers = Array.new(PRODUCER_COUNT) { build_producer.call }

producers.each do |producer|
  producer.monitor.subscribe("producer.reloaded") do |event|
    mutex.synchronize { reload_events << event }
  end

  producer.monitor.subscribe("transaction.aborted") do |event|
    mutex.synchronize { abort_events << event }
  end
end

# Warm each producer up so it is connected and has already run init_transactions before we start
# racing - we want to exercise only the commit-then-abort window, not the first connect.
producers.each_with_index do |producer, idx|
  producer.transaction do
    producer.produce_async(topic: topic, payload: "warmup-#{idx}")
  end
end

failed_cycle = nil

begin
  Timeout.timeout(DEADLOCK_TIMEOUT) do
    threads = producers.each_with_index.map do |producer, producer_idx|
      Thread.new do
        CYCLES_PER_PRODUCER.times do |cycle|
          # 1. Commit a transaction so the producer is in the exact "just committed a previous
          #    transaction" state that librdkafka#4849 requires.
          producer.transaction do
            producer.produce_async(topic: topic, payload: "commit-#{producer_idx}-#{cycle}")
          end

          # 2. Immediately open a new transaction, produce, and abort with that first produce still
          #    in flight. NO sleep here on purpose - this is the precise librdkafka#4849 window.
          begin
            producer.transaction do
              producer.produce_async(topic: topic, payload: "abort-#{producer_idx}-#{cycle}")
              raise WaterDrop::AbortTransaction
            end
          rescue Rdkafka::RdkafkaError => e
            # librdkafka#4849: the abort's EndTxn failed fatally (INVALID_TXN_STATE). WaterDrop has
            # already reloaded the client under the hood, so this is a recoverable blip, not a
            # rollback-correctness failure. Record it and keep going - the definitive check is that
            # the producer still commits a fresh transaction after the loop.
            mutex.synchronize { race_errors << e }
          end
        end
      rescue => e
        mutex.synchronize { failed_cycle ||= "producer #{producer_idx}: #{e.class}: #{e.message}" }
      end
    end

    threads.each(&:join)
  end
rescue Timeout::Error
  failed_cycle = "deadlock - commit/abort cycles did not finish within #{DEADLOCK_TIMEOUT}s"
end

# Definitive usability check: every producer must still be able to commit a fresh transaction after
# all the aborts (and any reloads). This is what proves the librdkafka#4849 blip was fully absorbed.
recovered = producers.each_with_index.map do |producer, idx|
  producer.transaction do
    producer.produce_sync(topic: topic, payload: "final-#{idx}")
  end
  true
rescue => e
  mutex.synchronize { failed_cycle ||= "final commit for producer #{idx}: #{e.class}: #{e.message}" }
  false
end

producers.each(&:close)

race_count = race_errors.size
reload_count = reload_events.size
abort_count = abort_events.size

puts "librdkafka#4849 fatal aborts observed: #{race_count} (reloads: #{reload_count})"
puts "aborts run: #{abort_count}, producers recovered: #{recovered.count(true)}/#{PRODUCER_COUNT}"

# Whenever the race actually fired, WaterDrop must have reloaded the client at least once - that is
# the recovery mechanism we are documenting. If the race never fired this run, there is simply
# nothing to reload and that is fine (the window is timing dependent).
reload_consistent = race_count.zero? || reload_count.positive?

if failed_cycle.nil? && recovered.all? && reload_consistent
  puts "PASS: abort-after-commit race handled without crash/deadlock; producers stayed usable"
  exit(0)
else
  warn "FAIL: #{failed_cycle || "post-race state inconsistent"} " \
       "(recovered=#{recovered}, race=#{race_count}, reloads=#{reload_count})"
  # If anything wedged we force-exit so finalizers do not hang re-closing a broken producer.
  exit!(1)
end
